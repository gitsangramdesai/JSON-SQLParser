const readline = require('readline');

// --- 1. REGISTRIES & HELPERS ---
const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    "sum": (values) => {
        const valid = values.filter(v => v !== null && v !== "NULL" && v !== undefined);
        return valid.reduce((a, b) => a + (Number(b) || 0), 0);
    },
    "count": (values) => values.filter(v => v !== null && v !== "NULL" && v !== undefined).length,
    "avg": (values) => {
        const valid = values.filter(v => v !== null && v !== "NULL" && v !== undefined);
        return valid.length ? (valid.reduce((a, b) => a + (Number(b) || 0), 0) / valid.length) : 0;
    },
    "min": (values) => {
        const valid = values.filter(v => v !== null && v !== "NULL" && v !== undefined).map(Number);
        return valid.length ? Math.min(...valid) : "NULL";
    },
    "max": (values) => {
        const valid = values.filter(v => v !== null && v !== "NULL" && v !== undefined).map(Number);
        return valid.length ? Math.max(...valid) : "NULL";
    },
    "coalesce": (args) => args.find(v => v !== null && v !== "NULL" && v !== undefined && v !== "") ?? "NULL"
};

function resolvePath(path, scope) {
    if (!path) return null;
    const parts = path.split('.');
    let current = scope;
    for (let part of parts) {
        const key = part.startsWith('$') ? part.slice(1) : part;
        if (current === scope && current[key] === undefined && (key === 'data' || key === 'root')) continue;
        current = current?.[key];
    }
    return current;
}

// --- 2. PAGER ---
async function displayWithPager(allRows, pageSize = 5) {
    if (!allRows || allRows.length === 0) {
        console.log("Empty set.");
        return;
    }
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    let currentIndex = 0;
    while (currentIndex < allRows.length) {
        console.clear();
        const page = allRows.slice(currentIndex, currentIndex + pageSize);
        const startRow = currentIndex + 1;
        const endRow = Math.min(currentIndex + pageSize, allRows.length);

        console.log(`\n--- Showing rows ${startRow} to ${endRow} of ${allRows.length} ---`);
        console.log(formatAsMySQLTable(page));

        if (currentIndex + pageSize >= allRows.length) {
            console.log("\n[END OF DATA] Press [Enter] to exit.");
            await new Promise(res => rl.question('', res));
            break;
        }
        const answer = await new Promise(res => rl.question('\nPress [Enter] for next page, or type ":q" to quit: ', res));
        if (answer.toLowerCase().trim() === ':q') break;
        currentIndex += pageSize;
    }
    rl.close();
    console.clear();
}

// --- 3. PARSER ENGINE ---
function parseToken(raw) {
    const aliasMatch = raw.match(/(.+) as (.+)/i);
    const expr = aliasMatch ? aliasMatch[1].trim() : raw;
    const alias = aliasMatch ? aliasMatch[2].trim() : expr.split('.').pop();
    const isAggregate = /^(sum|avg|count|min|max)\(/i.test(expr);
    
    let aggFunc = null, aggColumn = null;
    if (isAggregate) {
        const match = expr.match(/^(\w+)\((.*)\)/i);
        aggFunc = match[1].toLowerCase();
        aggColumn = match[2].trim() === "*" ? null : match[2].trim();
    }
    return { expr, alias, isAggregate, aggFunc, aggColumn };
}

function findTopLevelClause(sql, clause) {
    let depth = 0;
    const lowerSql = sql.toLowerCase();
    const search = clause.toLowerCase();
    for (let i = 0; i < sql.length; i++) {
        if (sql[i] === '(') depth++;
        if (sql[i] === ')') depth--;
        if (depth === 0 && lowerSql.startsWith(search, i)) {
            const before = i === 0 || /\s/.test(sql[i - 1]);
            const after = /\s/.test(sql[i + search.length]) || i + search.length === sql.length;
            if (before && after) return i;
        }
    }
    return -1;
}

function parseSQL(sql) {
    sql = sql.trim().replace(/\s+/g, ' ');
    const lower = sql.toLowerCase();

    const sIdx = findTopLevelClause(lower, "select");
    const fIdx = findTopLevelClause(lower, "from");
    const wIdx = findTopLevelClause(lower, "where");
    const withIdx = findTopLevelClause(lower, "with");

    const getEnd = (start) => {
        const boundaries = [wIdx, withIdx].filter(i => i > start);
        return boundaries.length > 0 ? Math.min(...boundaries) : sql.length;
    };

    let selectPartRaw = sql.slice(sIdx + 6, fIdx).trim();
    const selectTokens = [];
    let depth = 0, current = "";
    for (let i = 0; i < selectPartRaw.length; i++) {
        if (selectPartRaw[i] === '(') depth++;
        if (selectPartRaw[i] === ')') depth--;
        if (selectPartRaw[i] === ',' && depth === 0) {
            selectTokens.push(parseToken(current.trim()));
            current = "";
        } else current += selectPartRaw[i];
    }
    selectTokens.push(parseToken(current.trim()));

    const fromSection = sql.slice(fIdx + 4, getEnd(fIdx)).trim();
    const joinParts = fromSection.split(/\sjoin\s/i);
    const baseTable = joinParts[0].trim();
    const joins = [];

    for (let i = 1; i < joinParts.length; i++) {
        const joinSegment = joinParts[i];
        const onIdx = joinSegment.toLowerCase().indexOf(" on ");
        const tablePath = joinSegment.slice(0, onIdx).trim();
        const condition = joinSegment.slice(onIdx + 4).trim();
        joins.push({ tablePath, condition });
    }

    return { 
        selectTokens, 
        baseTable, 
        joins, 
        whereClause: wIdx !== -1 ? sql.slice(wIdx + 6, getEnd(wIdx)).trim() : null 
    };
}

// --- 4. EXECUTION ENGINE ---
async function executeQuery(sql, rootData) {
    // 1. EXTRACT HINTS
    const hintMatch = sql.match(/with\s*\(([^)]+)\)\s*;?\s*$/i);
    let activeHints = hintMatch ? hintMatch[1].split(',').map(h => h.trim().toLowerCase()) : [];
    let cleanSql = hintMatch ? sql.replace(hintMatch[0], '').trim() : sql;

    const parsed = parseSQL(cleanSql);

    // 2. DATA LOADING & JOINS
    let rows = [];
    const baseData = resolvePath(parsed.baseTable, rootData);
    if (!baseData) throw new Error(`Table not found: ${parsed.baseTable}`);

    const baseName = parsed.baseTable.split('.').pop();
    rows = baseData.map(r => {
        const namespaced = {};
        Object.keys(r).forEach(k => namespaced[`${baseName}.${k}`] = r[k]);
        return { ...r, ...namespaced };
    });

    for (const joinDef of parsed.joins) {
        const nextTableData = resolvePath(joinDef.tablePath, rootData);
        const nextTableName = joinDef.tablePath.split('.').pop();
        const [leftCol, rightCol] = joinDef.condition.split('=').map(c => c.trim());
        const newRows = [];

        rows.forEach(existingRow => {
            const lookupValue = existingRow[leftCol] ?? existingRow[leftCol.split('.').pop()];
            nextTableData.forEach(m => {
                if (String(lookupValue) === String(m[rightCol])) {
                    const namespacedMatch = {};
                    Object.keys(m).forEach(k => namespacedMatch[`${nextTableName}.${k}`] = m[k]);
                    newRows.push({ ...existingRow, ...m, ...namespacedMatch });
                }
            });
        });
        rows = newRows;
    }

    // 2.5 APPLY WHERE CLAUSE (Fix: Case-insensitive & Multi-condition)
    // --- 2.5 APPLY WHERE CLAUSE (Handles AND & OR) ---
    if (parsed.whereClause) {
        // Split by OR first (lowest precedence)
        const orGroups = parsed.whereClause.split(/\s+or\s+/i);
        
        rows = rows.filter(row => {
            // If ANY group in the OR list is true, the row stays
            return orGroups.some(group => {
                // Within each group, check AND conditions
                const andConditions = group.split(/\s+and\s+/i);
                
                return andConditions.every(cond => {
                    const match = cond.match(/(.+?)\s*(=|!=|>|<)\s*['"]?(.+?)['"]?$/i);
                    if (!match) return true;
                    
                    let [_, col, op, val] = match;
                    col = col.trim();
                    val = val.trim().replace(/['"]/g, "");
                    const actualValue = String(row[col] ?? row[col.split('.').pop()] ?? "").toLowerCase();
                    const targetValue = val.toLowerCase();

                    // Basic operator logic
                    if (op === '=') return actualValue === targetValue;
                    if (op === '!=') return actualValue !== targetValue;
                    return false;
                });
            });
        });
    }

    // 3. FINAL PROJECTION
    let result = rows.map(row => {
        const clean = {};
        parsed.selectTokens.forEach(t => {
            const val = row[t.alias] !== undefined ? row[t.alias] : row[t.expr];
            clean[t.alias] = (val !== undefined && val !== null) ? val : "NULL";
        });
        return clean;
    });

    // 4. APPLY HINTS
    if (activeHints.includes('paginate')) {
        await displayWithPager(result, 5);
    }

    if (activeHints.includes('headercolumnuppercase')) {
        result = result.map(row =>
            Object.fromEntries(Object.entries(row).map(([k, v]) => [k.toUpperCase(), v]))
        );
    }

    if (activeHints.includes('outputjson')) {
        return JSON.stringify(result, null, 2);
    }

    return result;
}

// --- 5. FORMATTER ---
function formatAsMySQLTable(rows) {
    if (!rows || !rows.length || typeof rows === 'string') return rows;
    const columns = Object.keys(rows[0]);
    const widths = {};
    columns.forEach(col => widths[col] = Math.max(col.length, ...rows.map(r => String(r[col] ?? "").length)));
    const line = () => "+" + columns.map(c => "-".repeat(widths[c] + 2)).join("+") + "+";
    const rowStr = (vals) => "| " + vals.map((v, i) => String(v).padEnd(widths[columns[i]])).join(" | ") + " |";

    let output = line() + "\n" + rowStr(columns) + "\n" + line() + "\n";
    rows.forEach(r => output += rowStr(columns.map(c => r[c])) + "\n");
    return output + line();
}

// --- 6. RUN TEST ---
const db = {
    friends: [
        { name: "Chris", city: "New York", countryCode: "USA" },
        { name: "Yuki", city: "Tokyo", countryCode: "JPN" },
        { name: "Emily", city: "Atlanta", countryCode: "USA" },
        { name: "Sato", city: "Tokyo", countryCode: "JPN" },
        { name: "James", city: "New York", countryCode: "USA" },
        { name: "Aiko", city: "Tokyo", countryCode: "JPN" },
        { name: "Sarah", city: "Atlanta", countryCode: "USA" },
        { name: "Kenji", city: "Tokyo", countryCode: "JPN" },
        { name: "John", city: "New York", countryCode: "USA" },
        { name: "Miku", city: "Tokyo", countryCode: "JPN" },
        { name: "Robert", city: "Atlanta", countryCode: "USA" },
        { name: "Hiro", city: "Tokyo", countryCode: "JPN" },
        { name: "Alice", city: "New York", countryCode: "USA" },
        { name: "Takumi", city: "Tokyo", countryCode: "JPN" },
        { name: "Laura", city: "Atlanta", countryCode: "USA" }
    ],
    cities: [{ cityName: "New York" }, { cityName: "Atlanta" }, { cityName: "Tokyo" }],
    countries: [{ code: "USA", countryName: "United States" }, { code: "JPN", countryName: "Japan" }]
};

const sql = `SELECT friends.name, cities.cityName, countries.countryName 
             FROM friends 
             JOIN cities ON city = cityName 
             JOIN countries ON countryCode = code 
             WHERE cities.cityName='Tokyo' OR friends.name='Sarah'
             WITH(HeaderColumnUpperCase, OutputJSON);`;

executeQuery(sql, db)
    .then(res => console.log(typeof res === 'string' ? res : formatAsMySQLTable(res)))
    .catch(err => console.error(err));