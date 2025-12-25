// --- 1. REGISTRIES & HELPERS ---
const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    ...Object.getOwnPropertyNames(Math).reduce((acc, name) => {
        if (typeof Math[name] === 'function') {
            acc[name.toLowerCase()] = (args) => Math[name](...args.map(a => parseFloat(a || 0)));
        }
        return acc;
    }, {}),
    "count": (args) => args.length,
};

const sqlConstants = { "e": Math.E, "pi": Math.PI };

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

function evaluateExpression(expr, row) {
    let evalStr = expr.replace(/COUNT\(\*\)/gi, "COUNT_ALL");
    const safeRow = { ...row };
    if (row["COUNT(*)"] !== undefined) safeRow["COUNT_ALL"] = row["COUNT(*)"];

    evalStr = evalStr.replace(/[a-zA-Z_][a-zA-Z0-9_]*/g, (match) => {
        const m = match.toLowerCase();
        if (m === "true" || m === "false") return m;
        if (sqlFunctions[m]) return `sqlFunctions.${m}`;
        if (safeRow[match] !== undefined) return JSON.stringify(safeRow[match]);
        return match;
    });

    try {
        return new Function('sqlFunctions', `return ${evalStr}`)(sqlFunctions);
    } catch (e) { return false; }
}

// --- 2. PARSER ---

function parseAlias(token) {
    const parts = token.split(/\s+as\s+/i);
    return parts.length === 2 ? { expr: parts[0].trim(), alias: parts[1].trim() } : { expr: token, alias: token };
}

function getSelectTokensFromPart(expr) {
    let result = [], depth = 0, buf = "";
    for (let ch of expr) {
        if (ch === "(") depth++;
        if (ch === ")") depth--;
        if (ch === "," && depth === 0) {
            result.push(parseAlias(buf.trim()));
            buf = "";
        } else { buf += ch; }
    }
    if (buf) result.push(parseAlias(buf.trim()));
    return result;
}

function parseSQL(sql) {
    const lower = sql.toLowerCase();
    const selectIdx = lower.indexOf("select");
    const fromIdx = lower.indexOf("from");
    const whereIdx = lower.indexOf("where");
    const groupIdx = lower.indexOf("group by");
    const havingIdx = lower.indexOf("having");
    const orderIdx = lower.indexOf("order by");
    const limitIdx = lower.indexOf("limit");

    const getNextIdx = (curr) => {
        const next = [whereIdx, groupIdx, havingIdx, orderIdx, limitIdx].filter(i => i > curr);
        return next.length > 0 ? Math.min(...next) : sql.length;
    };

    const selectPart = sql.slice(selectIdx + 6, fromIdx).trim();
    const selectTokens = getSelectTokensFromPart(selectPart).map(token => ({
        ...token,
        isWindow: /row_number\s*\(\s*\)\s+over/i.test(token.expr)
    }));

    return {
        selectTokens,
        fromClause: sql.slice(fromIdx + 4, getNextIdx(fromIdx)).trim(),
        whereClause: whereIdx !== -1 ? sql.slice(whereIdx + 5, getNextIdx(whereIdx)).trim() : null,
        groupBy: groupIdx !== -1 ? sql.slice(groupIdx + 8, getNextIdx(groupIdx)).trim() : null,
        havingClause: havingIdx !== -1 ? sql.slice(havingIdx + 6, getNextIdx(havingIdx)).trim() : null,
        orderClause: orderIdx !== -1 ? sql.slice(orderIdx + 9, getNextIdx(orderIdx)).trim() : null
    };
}

// --- 3. ENGINE LOGIC ---

function applyGroupBy(rows, groupingColumnsStr, selectTokens) {
    const groups = {};
    const columns = groupingColumnsStr.split(',').map(c => c.trim());

    for (const row of rows) {
        const groupKey = columns.map(col => row[col]).join('|');
        if (!groups[groupKey]) groups[groupKey] = [];
        groups[groupKey].push(row);
    }

    return Object.entries(groups).map(([key, group]) => {
        const result = {};
        const firstRow = group[0];
        for (const token of selectTokens) {
            if (token.isWindow) continue;
            const expr = token.expr.toLowerCase().trim();
            const matchedCol = columns.find(c => c.toLowerCase() === expr);
            if (matchedCol) {
                result[token.alias] = firstRow[matchedCol];
                continue;
            }
            const aggMatch = expr.match(/^(\w+)\((.*?)\)$/);
            if (aggMatch) {
                const [_, func, arg] = aggMatch;
                if (func === "count") result[token.alias] = group.length;
                else if (func === "sum") result[token.alias] = group.reduce((s, r) => s + Number(r[arg] || 0), 0);
            }
        }
        return result;
    });
}

function applyOrderBy(rows, orderClause) {
    if (!orderClause) return rows;
    const orders = orderClause.split(",").map(p => {
        const t = p.trim().split(/\s+/);
        return { expr: t[0], dir: (t[1] || "ASC").toUpperCase() };
    });
    return [...rows].sort((a, b) => {
        for (const { expr, dir } of orders) {
            let av = a[expr], bv = b[expr];
            const na = Number(av), nb = Number(bv);
            if (!isNaN(na) && !isNaN(nb)) { av = na; bv = nb; }
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

    let rows = sourceData.filter(row => {
        if (!parsed.whereClause) return true;
        const js = parsed.whereClause.replace(/=/g, '==').replace(/\bAND\b/gi, '&&').replace(/\bOR\b/gi, '||');
        return Boolean(evaluateExpression(js, row));
    });

    if (parsed.groupBy) {
        rows = applyGroupBy(rows, parsed.groupBy, parsed.selectTokens);
        if (parsed.havingClause) {
            const jsH = parsed.havingClause.replace(/=/g, '==').replace(/\bAND\b/gi, '&&').replace(/\bOR\b/gi, '||');
            rows = rows.filter(row => Boolean(evaluateExpression(jsH, row)));
        }
    } else {
        rows = rows.map(row => {
            const obj = {};
            parsed.selectTokens.forEach(t => { if (!t.isWindow) obj[t.alias] = row[t.expr] ?? t.expr; });
            return obj;
        });
    }

    rows = applyOrderBy(rows, parsed.orderClause);

    const windowTokens = parsed.selectTokens.filter(t => t.isWindow);
    if (windowTokens.length > 0) {
        rows = rows.map((row, index) => {
            windowTokens.forEach(t => { row[t.alias] = index + 1; });
            return row;
        });
    }
    return rows;
}

function formatAsMySQLTable(rows) {
    if (!rows || rows.length === 0) return "Empty Set";
    const columns = Object.keys(rows[0]);
    const widths = {};
    columns.forEach(col => widths[col] = Math.max(col.length, ...rows.map(r => String(r[col] ?? "").length)));
    const line = () => "+" + columns.map(c => "-".repeat(widths[c] + 2)).join("+") + "+";
    const rowStr = (vals) => "| " + vals.map((v, i) => String(v ?? "").padEnd(widths[columns[i]])).join(" | ") + " |";
    let output = line() + "\n" + rowStr(columns) + "\n" + line() + "\n";
    rows.forEach(r => output += rowStr(columns.map(c => r[c])) + "\n");
    return output + line();
}

// --- RUN ---
const data = {
    friends: [
        { name: "Chris", age: 23, city: "New York" },
        { name: "Emily", age: 19, city: "Atlanta" },
        { name: "Joe", age: 32, city: "New York" },
        { name: "Kevin", age: 19, city: "Atlanta" },
        { name: "Michelle", age: 27, city: "Los Angeles" },
        { name: "Robert", age: 45, city: "Manhattan" },
        { name: "Sarah", age: 31, city: "New York" }
    ]
};

const sqlQuery = `SELECT name, age, COUNT(*), ROW_NUMBER() OVER() as rank 
FROM data.$friends 
GROUP BY name, age 
ORDER BY name DESC;`;

console.log(formatAsMySQLTable(executeQuery(sqlQuery, data)));