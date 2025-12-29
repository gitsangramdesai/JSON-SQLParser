const readline = require('readline');

// --- 1. REGISTRIES ---
const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    "count": (values) => values.length,
    "contains": (args) => String(args[0] ?? "").toLowerCase().includes(String(args[1] ?? "").toLowerCase()),
};

// --- 2. ENGINE CORE ---
async function executeQuery(sql, rootData) {
    // A. Parse Hints
    const hintMatch = sql.match(/with\s*\(([^)]+)\)\s*;?\s*$/i);
    const activeHints = hintMatch ? hintMatch[1].split(',').map(h => h.trim().toLowerCase()) : [];
    let cleanSql = hintMatch ? sql.replace(hintMatch[0], '').trim() : sql;

    const lower = cleanSql.toLowerCase();
    
    // B. Parse Main Clauses (Simple Splitting)
    const sIdx = lower.indexOf("select");
    const fIdx = lower.indexOf("from");
    const wIdx = lower.indexOf("where");
    
    const selectPart = cleanSql.slice(sIdx + 6, fIdx).trim();
    const fromEnd = wIdx !== -1 ? wIdx : cleanSql.length;
    const fromPart = cleanSql.slice(fIdx + 4, fromEnd).trim();

    // C. 1: FROM & JOIN Logic
    const tableSegments = fromPart.split(/\sjoin\s/i);
    let baseTableName = tableSegments[0].trim().replace('data.', '').replace('$', '');
    let rows = JSON.parse(JSON.stringify(rootData[baseTableName] || []));

    // Create namespaces (e.g., friends.name)
    rows = rows.map(r => {
        let entry = { ...r };
        Object.keys(r).forEach(k => entry[`${baseTableName}.${k}`] = r[k]);
        return entry;
    });

    // Handle Joins
    for (let i = 1; i < tableSegments.length; i++) {
        const joinParts = tableSegments[i].split(/\son\s/i);
        const joinTableRaw = joinParts[0].trim().replace('data.', '').replace('$', '');
        const condition = joinParts[1].split('=');
        const leftCol = condition[0].trim();
        const rightCol = condition[1].trim();
        
        const joinData = rootData[joinTableRaw] || [];
        let joinedRows = [];

        rows.forEach(row => {
            const leftVal = row[leftCol] ?? row[leftCol.split('.').pop()];
            joinData.forEach(jRow => {
                if (String(leftVal) === String(jRow[rightCol])) {
                    let combined = { ...row, ...jRow };
                    Object.keys(jRow).forEach(k => combined[`${joinTableRaw}.${k}`] = jRow[k]);
                    joinedRows.push(combined);
                }
            });
        });
        rows = joinedRows;
    }

    // D. 2: WHERE (OR/AND Logic)
    if (wIdx !== -1) {
        const whereClause = cleanSql.slice(wIdx + 5).trim();
        const orGroups = whereClause.split(/\s+or\s+/i);
        
        rows = rows.filter(row => {
            return orGroups.some(group => {
                const andConditions = group.split(/\s+and\s+/i);
                return andConditions.every(cond => {
                    const match = cond.match(/(.+?)\s*(=|!=|contains|>|<)\s*['"]?(.+?)['"]?$/i);
                    if (!match) return true;
                    let [_, col, op, val] = match;
                    val = val.trim().replace(/['"]/g, "");
                    const actual = String(row[col.trim()] ?? row[col.trim().split('.').pop()] ?? "").toLowerCase();
                    const target = val.toLowerCase();

                    if (op === '=') return actual === target;
                    if (op === '!=') return actual !== target;
                    if (op === 'contains') return actual.includes(target);
                    if (op === '>') return Number(actual) > Number(target);
                    if (op === '<') return Number(actual) < Number(target);
                    return false;
                });
            });
        });
    }

    // E. 3: SELECT & WINDOW FUNCTIONS
    const selectTokens = selectPart.split(',').map(t => {
        const parts = t.split(/\sas\s/i);
        const expr = parts[0].trim();
        const alias = (parts[1] || parts[0]).trim().split('.').pop();
        return { expr, alias };
    });

    // Process ROW_NUMBER
    selectTokens.forEach(token => {
        if (token.expr.toLowerCase().includes("row_number")) {
            rows.forEach((row, idx) => row[token.alias] = idx + 1);
        }
    });

    let finalData = rows.map(row => {
        let clean = {};
        selectTokens.forEach(t => {
            clean[t.alias] = row[t.alias] ?? row[t.expr] ?? row[t.expr.split('.').pop()] ?? "NULL";
        });
        return clean;
    });

    // F. 4: HINTS
    if (activeHints.includes('headercolumnuppercase')) {
        finalData = finalData.map(row => 
            Object.fromEntries(Object.entries(row).map(([k, v]) => [k.toUpperCase(), v]))
        );
    }

    if (activeHints.includes('outputjson')) {
        return JSON.stringify(finalData, null, 2);
    }

    return finalData;
}

// --- 3. FORMATTER ---
function formatAsMySQLTable(rows) {
    if (typeof rows === 'string') return rows;
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

// --- 4. DATA & EXECUTION ---
const db = {
    friends: [
        { name: "Chris", age: 23, city: "New York" },
        { name: "Emily", age: 19, city: "Atlanta" },
        { name: "Sarah", age: 31, city: "New York" }
    ],
    cities: [
        { cityName: "New York", state: "NY" },
        { cityName: "Atlanta", state: "GA" }
    ]
};

// --- DEFINE SQL QUERY BEFORE CALLING EXECUTE ---
const mySqlQuery = `
    SELECT friends.name,friends.age, cities.state, ROW_NUMBER() OVER() as rank
    FROM data.friends 
    JOIN data.cities ON city = cityName 
    WHERE age > 20 OR state = 'GA'
    WITH(HeaderColumnUpperCase, OutputJSON)
`;

// Now it works because mySqlQuery is defined
executeQuery(mySqlQuery, db)
    .then(res => console.log(formatAsMySQLTable(res)))
    .catch(err => console.error(err));