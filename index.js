const readline = require('readline');

// --- 1. REGISTRIES ---
const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    "initcap": (args) => String(args[0] ?? "").replace(/\b\w/g, c => c.toUpperCase()),
    "coalesce": (args) => args.find(v => v !== null && v !== "NULL" && v !== undefined && v !== "") ?? "NULL",
    "contains": (args) => String(args[0] ?? "").toLowerCase().includes(String(args[1] ?? "").toLowerCase()),
    "sum": (values) => values.filter(v => !isNaN(v)).reduce((a, b) => a + Number(b), 0),
    "count": (values) => values.filter(v => v !== null && v !== "NULL").length,
    "avg": (values) => {
        const v = values.filter(v => !isNaN(v));
        return v.length ? v.reduce((a, b) => a + Number(b), 0) / v.length : 0;
    }
};

// --- 2. HELPERS ---
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

function evaluateArithmetic(expr, row) {
    const tokens = expr.split(/([+\-*/])/).map(t => t.trim());
    let result = 0;
    let currentOp = '+';
    for (let token of tokens) {
        if (['+', '-', '*', '/'].includes(token)) { currentOp = token; } 
        else {
            let val = isNaN(token) ? (row[token] ?? row[token.split('.').pop()] ?? 0) : Number(token);
            switch (currentOp) {
                case '+': result += val; break;
                case '-': result -= val; break;
                case '*': result *= val; break;
                case '/': result /= (val !== 0 ? val : 1); break;
            }
        }
    }
    return result;
}

// --- 3. RELATIONAL JOIN ENGINE ---
function performJoin(leftRows, rightRows, leftCol, rightCol, type) {
    const result = [];
    const matchedRightIndices = new Set();
    const matchedLeftIndices = new Set();

    leftRows.forEach((lRow, lIdx) => {
        let foundMatch = false;
        rightRows.forEach((rRow, rIdx) => {
            const lVal = lRow[leftCol] ?? lRow[leftCol.split('.').pop()];
            const rVal = rRow[rightCol] ?? rRow[rightCol.split('.').pop()];

            if (lVal !== undefined && rVal !== undefined && String(lVal) === String(rVal)) {
                result.push({ ...lRow, ...rRow });
                matchedRightIndices.add(rIdx);
                matchedLeftIndices.add(lIdx);
                foundMatch = true;
            }
        });

        // Handle OUTER logic for Left/Full
        if (!foundMatch && (type.includes('left') || type.includes('full'))) {
            const nullRight = Object.fromEntries(Object.keys(rightRows[0] || {}).map(k => [k, "NULL"]));
            result.push({ ...lRow, ...nullRight });
        }
    });

    // Handle OUTER logic for Right/Full
    if (type.includes('right') || type.includes('full')) {
        rightRows.forEach((rRow, rIdx) => {
            if (!matchedRightIndices.has(rIdx)) {
                const nullLeft = Object.fromEntries(Object.keys(leftRows[0] || {}).map(k => [k, "NULL"]));
                result.push({ ...nullLeft, ...rRow });
            }
        });
    }
    return result;
}

// --- 4. PARSER ---
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
    sql = sql.trim().replace(/;$/, '').replace(/\s+/g, ' ');
    const lower = sql.toLowerCase();

    const sIdx = findTopLevelClause(lower, "select");
    const fIdx = findTopLevelClause(lower, "from");
    const wIdx = findTopLevelClause(lower, "where");
    const lIdx = findTopLevelClause(lower, "limit");
    const offIdx = findTopLevelClause(lower, "offset");
    const withIdx = findTopLevelClause(lower, "with");

    const getEnd = (start) => {
        const boundaries = [wIdx, lIdx, offIdx, withIdx].filter(i => i > start);
        return boundaries.length > 0 ? Math.min(...boundaries) : sql.length;
    };

    const selectPart = sql.slice(sIdx + 6, fIdx).trim();
    const selectTokens = selectPart.split(',').map(t => {
        const parts = t.split(/\sas\s/i);
        const expr = parts[0].trim();
        return { expr, alias: (parts[1] || expr).trim().split('.').pop() };
    });

    const fromSection = sql.slice(fIdx + 4, getEnd(fIdx)).trim();
    // Regex matches: [Join Type] JOIN [Table] ON [Col] = [Col]
    const joinRegex = /(?:(left(?:\s+outer)?|right(?:\s+outer)?|full(?:\s+outer)?|inner)?\s+join)\s+([\w.]+)\s+on\s+([\w.]+)\s*=\s*([\w.]+)/gi;
    
    let baseTable = fromSection.split(/\s+(?:left|right|full|inner|join)/i)[0].trim();
    const joins = [];
    let match;
    while ((match = joinRegex.exec(fromSection)) !== null) {
        joins.push({
            type: (match[1] || 'inner').toLowerCase(),
            table: match[2],
            leftCol: match[3],
            rightCol: match[4]
        });
    }

    return {
        selectTokens, baseTable, joins,
        whereClause: wIdx !== -1 ? sql.slice(wIdx + 6, getEnd(wIdx)).trim() : null,
        limit: lIdx !== -1 ? parseInt(sql.slice(lIdx + 5, (offIdx !== -1 ? offIdx : (withIdx !== -1 ? withIdx : sql.length))).trim()) : null,
        offset: offIdx !== -1 ? parseInt(sql.slice(offIdx + 6, (withIdx !== -1 ? withIdx : sql.length)).trim()) : 0
    };
}

// --- 5. ENGINE CORE ---
async function executeQuery(sql, rootData) {
    const hintMatch = sql.match(/with\s*\(([^)]+)\)\s*;?\s*$/i);
    const activeHints = hintMatch ? hintMatch[1].split(',').map(h => h.trim().toLowerCase()) : [];
    let cleanSql = hintMatch ? sql.replace(hintMatch[0], '').trim() : sql;

    const parsed = parseSQL(cleanSql);
    let rows = resolvePath(parsed.baseTable, rootData) || [];
    const baseName = parsed.baseTable.split('.').pop();
    rows = rows.map(r => Object.fromEntries(Object.entries(r).map(([k, v]) => [`${baseName}.${k}`, v])));

    // Execute Joins
    parsed.joins.forEach(join => {
        let joinData = resolvePath(join.table, rootData) || [];
        const joinName = join.table.split('.').pop();
        joinData = joinData.map(r => Object.fromEntries(Object.entries(r).map(([k, v]) => [`${joinName}.${k}`, v])));
        rows = performJoin(rows, joinData, join.leftCol, join.rightCol, join.type);
    });

    // Filter
    if (parsed.whereClause) {
        rows = rows.filter(row => {
            const m = parsed.whereClause.match(/(.+?)\s*(=|!=)\s*(.+)/);
            if (!m) return true;
            const actual = String(row[m[1].trim()] ?? row[m[1].trim().split('.').pop()] ?? "").toLowerCase();
            const target = m[3].trim().replace(/['"]/g, "").toLowerCase();
            return m[2] === '=' ? actual === target : actual !== target;
        });
    }

    // Project
    let result = rows.map(row => {
        const clean = {};
        parsed.selectTokens.forEach(t => {
            const funcMatch = t.expr.match(/^(\w+)\((.*)\)$/);
            const hasMath = /[+\-*/]/.test(t.expr) && !t.expr.includes('(');
            if (funcMatch) {
                const funcName = funcMatch[1].toLowerCase();
                const args = funcMatch[2].split(',').map(a => a.trim().replace(/['"]/g, ''));
                const resolved = args.map(arg => row[arg] ?? row[arg.split('.').pop()] ?? arg);
                clean[t.alias] = sqlFunctions[funcName] ? sqlFunctions[funcName](resolved) : "NULL";
            } else if (hasMath) {
                clean[t.alias] = evaluateArithmetic(t.expr, row);
            } else {
                clean[t.alias] = row[t.expr] ?? row[t.expr.split('.').pop()] ?? "NULL";
            }
        });
        return clean;
    });

    // Limit/Offset
    if (parsed.limit !== null || parsed.offset > 0) {
        result = result.slice(parsed.offset, parsed.limit !== null ? parsed.offset + parsed.limit : undefined);
    }

    // Apply Hints
    if (activeHints.includes('headercolumnuppercase')) {
        result = result.map(row => Object.fromEntries(Object.entries(row).map(([k, v]) => [k.toUpperCase(), v])));
    }
    if (activeHints.includes('outputjson')) return JSON.stringify(result, null, 2);
    
    return result;
}

function formatAsMySQLTable(rows) {
    if (typeof rows === 'string') return rows;
    if (!rows || !rows.length) return "Empty set";
    const columns = Object.keys(rows[0]);
    const widths = Object.fromEntries(columns.map(c => [c, Math.max(c.length, ...rows.map(r => String(r[c]).length))]));
    const line = () => "+" + columns.map(c => "-".repeat(widths[c] + 2)).join("+") + "+";
    const rowStr = (vals) => "| " + vals.map((v, i) => String(v).padEnd(widths[columns[i]])).join(" | ") + " |";
    let output = line() + "\n" + rowStr(columns) + "\n" + line() + "\n";
    rows.forEach(r => output += rowStr(columns.map(c => r[c])) + "\n");
    return output + line();
}

// --- YOUR ORIGINAL TEST DATA ---
const db = {
    friends: [
        { name: "Chris", city: "New York", countryCode: "USA","age":50 },
        { name: "Yuki", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "Emily", city: "Atlanta", countryCode: "USA" ,"age":50},
        { name: "Sato", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "James", city: "New York", countryCode: "USA" ,"age":50},
        { name: "Aiko", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "Sarah", city: "Atlanta", countryCode: "USA" ,"age":50},
        { name: "Kenji", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "John", city: "New York", countryCode: "USA" ,"age":50},
        { name: "Miku", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "Robert", city: "Atlanta", countryCode: "USA" ,"age":50},
        { name: "Hiro", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "Alice", city: "New York", countryCode: "USA" ,"age":50},
        { name: "Takumi", city: "Tokyo", countryCode: "JPN" ,"age":50},
        { name: "Laura", city: "Atlanta", countryCode: "USA" ,"age":50}
    ],
    cities: [{ cityName: "New York" }, { cityName: "Atlanta" }, { cityName: "Tokyo" }],
    countries: [{ code: "USA", countryName: "United States" }, { code: "JPN", countryName: "Japan" }]
};

// TEST QUERY
const sql = `SELECT name, city, age
FROM data.friends LEFT OUTER JOIN data.cities ON city = cityName
WHERE city='Tokyo' 
LIMIT 5 WITH(OutputJSON)`;

executeQuery(sql, db).then(res => console.log(formatAsMySQLTable(res)));