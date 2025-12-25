// --- 1. REGISTRIES ---
const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    "replace": (args) => {
        const str = String(args[0] ?? ""), s = String(args[1] ?? ""), r = String(args[2] ?? "");
        return str.split(s).join(r);
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

// --- 3. PARSER ---
function parseAlias(token) {
    const parts = token.split(/\s+as\s+/i);
    return parts.length === 2 ? { expr: parts[0].trim(), alias: parts[1].trim() } : { expr: token, alias: token };
}

function getSelectTokensFromPart(expr) {
    let result = [], depth = 0, buf = "";
    for (let ch of expr) {
        if (ch === "(") depth++; else if (ch === ")") depth--;
        if (ch === "," && depth === 0) { result.push(parseAlias(buf.trim())); buf = ""; } else buf += ch;
    }
    if (buf) result.push(parseAlias(buf.trim()));
    return result;
}

function parseSQL(sql) {
    sql = sql.trim().replace(/;$/, '');
    const lower = sql.toLowerCase();
    const findClause = (kw) => lower.indexOf(kw);

    const sIdx = findClause("select"), fIdx = findClause("from"), wIdx = findClause("where"), gIdx = findClause("group by"), oIdx = findClause("order by");

    const getEnd = (start) => {
        const b = [fIdx, wIdx, gIdx, oIdx].filter(i => i > start);
        return b.length > 0 ? Math.min(...b) : sql.length;
    };

    const selectPart = sql.slice(sIdx + 6, fIdx).trim();
    const selectTokens = getSelectTokensFromPart(selectPart).map(token => {
        const windowMatch = token.expr.match(/row_number\s*\(\s*\)\s+over\s*\((.*)\)/i);
        let internalOrder = null, partitionBy = null;
        if (windowMatch) {
            const content = windowMatch[1].trim();
            const pMatch = content.match(/partition\s+by\s+(.*?)(?=order\s+by|$)/i);
            if (pMatch) partitionBy = pMatch[1].trim();
            const oMatch = content.match(/order\s+by\s+(.*)/i);
            if (oMatch) internalOrder = oMatch[1].trim();
        }
        return { ...token, isWindow: !!windowMatch, windowOrder: internalOrder, partitionBy: partitionBy };
    });

    return {
        selectTokens,
        fromClause: sql.slice(fIdx + 4, getEnd(fIdx)).trim(),
        groupBy: gIdx !== -1 ? sql.slice(gIdx + 8, getEnd(gIdx)).trim() : null,
        orderClause: oIdx !== -1 ? sql.slice(oIdx + 9).trim() : null
    };
}

// --- 4. ENGINE CORE ---
function applyGroupBy(rows, groupingColumnsStr, selectTokens) {
    const groups = {};
    const columns = groupingColumnsStr.split(',').map(c => c.trim());
    for (const row of rows) {
        const key = columns.map(col => row[col]).join('|');
        if (!groups[key]) groups[key] = [];
        groups[key].push(row);
    }
    return Object.entries(groups).map(([key, group]) => {
        const result = {}, firstRow = group[0];
        selectTokens.forEach(t => {
            if (t.isWindow) return;
            const expr = t.expr.toLowerCase().trim();
            const matchedCol = columns.find(c => c.toLowerCase() === expr);
            if (matchedCol) result[t.alias] = firstRow[matchedCol];
            else if (firstRow[t.expr] !== undefined) result[t.alias] = firstRow[t.expr];
        });
        columns.forEach(c => { 
            if (result[c] === undefined) Object.defineProperty(result, c, { value: firstRow[c], enumerable: false, writable: true }); 
        });
        return result;
    });
}

function applyOrderBy(rows, orderClause) {
    if (!orderClause) return rows;
    const orders = orderClause.split(",").map(p => {
        const t = p.trim().split(/\s+/);
        return { expr: t[0].trim(), dir: (t[1] || "ASC").toUpperCase() };
    });
    return [...rows].sort((a, b) => {
        for (const { expr, dir } of orders) {
            let av = a[expr], bv = b[expr];
            if (!isNaN(av) && !isNaN(bv)) { av = Number(av); bv = Number(bv); }
            if (av < bv) return dir === "ASC" ? -1 : 1;
            if (av > bv) return dir === "ASC" ? 1 : -1;
        }
        return 0;
    });
}

function executeQuery(sql, rootData) {
    const parsed = parseSQL(sql);
    const sourceData = resolvePath(parsed.fromClause, rootData);
    if (!Array.isArray(sourceData)) return [];

    let rows = sourceData; // (Omitting filter for brevity)

    if (parsed.groupBy) rows = applyGroupBy(rows, parsed.groupBy, parsed.selectTokens);
    
    // Window Function Processing
    parsed.selectTokens.filter(t => t.isWindow).forEach(t => {
        if (t.partitionBy) {
            const partitions = {}, partCols = t.partitionBy.split(',').map(c => c.trim());
            rows.forEach(r => {
                const compositeKey = partCols.map(col => String(r[col] ?? "default")).join('|');
                if (!partitions[compositeKey]) partitions[compositeKey] = [];
                partitions[compositeKey].push(r);
            });
            Object.values(partitions).forEach(pRows => {
                const sorted = applyOrderBy(pRows, t.windowOrder);
                sorted.forEach((r, idx) => r[t.alias] = idx + 1);
            });
        }
    });

    // FINAL GLOBAL ORDER BY
    if (parsed.orderClause) rows = applyOrderBy(rows, parsed.orderClause);

    const finalAliases = parsed.selectTokens.map(t => t.alias);
    return rows.map(row => {
        const clean = {};
        finalAliases.forEach(a => clean[a] = row[a]);
        return clean;
    });
}

// --- FORMATTER ---
function formatAsMySQLTable(rows) {
    const columns = Object.keys(rows[0]);
    const widths = {};
    columns.forEach(col => widths[col] = Math.max(col.length, ...rows.map(r => String(r[col] ?? "").length)));
    const line = () => "+" + columns.map(c => "-".repeat(widths[c] + 2)).join("+") + "+";
    const rowStr = (vals) => "| " + vals.map((v, i) => String(v ?? "").padEnd(widths[columns[i]])).join(" | ") + " |";
    let output = line() + "\n" + rowStr(columns) + "\n" + line() + "\n";
    rows.forEach(r => output += rowStr(columns.map(c => r[c])) + "\n");
    return output + line();
}

// --- DATA ---
const data = {
    friends: [
        { name: "Chris", age: 23, city: "New York", gender: "Male" },
        { name: "Emily", age: 19, city: "Atlanta", gender: "Female" },
        { name: "Joe", age: 32, city: "New York", gender: "Male" },
        { name: "Kevin", age: 19, city: "Atlanta", gender: "Male" },
        { name: "Michelle", age: 27, city: "Los Angeles", gender: "Female" },
        { name: "Robert", age: 45, city: "Manhattan", gender: "Male" },
        { name: "Sarah", age: 31, city: "New York", gender: "Female" }
    ]
};

// Sorting by City (A-Z) and Gender (Female then Male)
const sqlQuery = `SELECT city, gender, name, age, 
       ROW_NUMBER() OVER(PARTITION BY city, gender ORDER BY city, gender ) as rank 
FROM data.friends 
GROUP BY city, gender, name, age 
ORDER BY city ASC, gender ASC`;



const resultSet = executeQuery(sqlQuery, data);
console.log(formatAsMySQLTable(resultSet));