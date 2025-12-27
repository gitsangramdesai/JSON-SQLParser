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
function parseAlias(token) {
    const parts = token.split(/\s+as\s+/i);
    return parts.length === 2 ? { expr: parts[0].trim(), alias: parts[1].trim() } : { expr: token, alias: token };
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

function parseToken(raw) {
    const aliasMatch = raw.match(/(.+) as (.+)/i);
    const expr = aliasMatch ? aliasMatch[1].trim() : raw;
    const alias = aliasMatch ? aliasMatch[2].trim() : expr.split('.').pop();
    const isAggregate = /^(sum|avg|count|min|max)\(/i.test(expr);
    const isWindow = /over\s*\(/i.test(expr);

    let aggFunc = null, aggColumn = null;
    if (isAggregate) {
        const match = expr.match(/^(\w+)\((.*)\)/i);
        aggFunc = match[1].toLowerCase();
        aggColumn = match[2].trim() === "*" ? null : match[2].trim();
    }
    return { expr, alias, isAggregate, aggFunc, aggColumn, isWindow };
}

function parseSQL(sql) {
    sql = sql.trim().replace(/\s+/g, ' ');
    const lower = sql.toLowerCase();

    const sIdx = findTopLevelClause(lower, "select");
    const fIdx = findTopLevelClause(lower, "from");
    const wIdx = findTopLevelClause(lower, "where");
    const lIdx = findTopLevelClause(lower, "limit");

    const getEnd = (start) => {
        const boundaries = [wIdx, lIdx].filter(i => i > start);
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

    return { selectTokens, baseTable, joins, whereClause: wIdx !== -1 ? sql.slice(wIdx + 6, getEnd(wIdx)).trim() : null };
}

// --- 4. EXECUTION ENGINE ---
async function executeQuery(sql, rootData) {
    // 1. EXTRACT HINTS & CLEAN SQL
    const hintMatch = sql.match(/with\s*\(([^)]+)\)\s*;?\s*$/i);
    let activeHints = [];
    let cleanSql = sql;

    if (hintMatch) {
        activeHints = hintMatch[1].split(',').map(h => h.trim().toLowerCase());
        cleanSql = sql.replace(hintMatch[0], '').trim();
    }

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

    // 3. FINAL PROJECTION (Mapping rows to selected columns)
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
        result = JSON.stringify(result, null, 2);
    }

    return result;
}

// --- 5. FORMATTER ---
function formatAsMySQLTable(rows) {
    if (!rows || !rows.length) return "Empty set";
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
             JOIN countries ON countryCode = code where  cities.cityName='Tokyo'
             WITH(HeaderColumnUpperCase,PAGINATE);`;

executeQuery(sql, db)
    .then(console.log)
    .catch(err => console.error(err));

    //OutputJSON,HeaderColumnUpperCase,Paginate