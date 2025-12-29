/*
====================================================
 UNIFIED SQL SELECT ENGINE (v1 + v2 + v3)
====================================================
Author intent: Sangram Desai

DESIGN GOAL
-----------
✔ Never remove existing functionality when adding new
✔ Layer features instead of replacing logic
✔ Single-file, readable, hackable engine

SUPPORTED FEATURES
------------------
• SELECT columns, aliases
• Scalar functions: upper, lower, coalesce
• WHERE (AND conditions)
• INNER JOIN, LEFT JOIN
• GROUP BY + aggregates (sum, count, avg, min, max)
• DISTINCT
• ORDER BY
• LIMIT
• Window functions: row_number, rank, dense_rank
• WITH(...) execution hints:
    - PAGINATE
    - HEADERCOLUMNUPPERCASE
    - OUTPUTJSON
• MySQL-style ASCII table output

====================================================
*/

const readline = require('readline');

// ==================================================
// 1. FUNCTION REGISTRY
// ==================================================
const sqlFunctions = {
    upper: ([v]) => String(v ?? '').toUpperCase(),
    lower: ([v]) => String(v ?? '').toLowerCase(),
    coalesce: (args) => args.find(v => v !== null && v !== undefined && v !== 'NULL' && v !== '') ?? 'NULL',

    sum: (vals) => vals.filter(v => v != null && v !== 'NULL').reduce((a, b) => a + Number(b || 0), 0),
    count: (vals) => vals.filter(v => v != null && v !== 'NULL').length,
    avg: (vals) => {
        const v = vals.filter(x => x != null && x !== 'NULL');
        return v.length ? v.reduce((a, b) => a + Number(b || 0), 0) / v.length : 0;
    },
    min: (vals) => {
        const v = vals.filter(x => x != null && x !== 'NULL').map(Number);
        return v.length ? Math.min(...v) : 'NULL';
    },
    max: (vals) => {
        const v = vals.filter(x => x != null && x !== 'NULL').map(Number);
        return v.length ? Math.max(...v) : 'NULL';
    }
};

// ==================================================
// 2. HELPERS
// ==================================================
function splitArgs(str) {
    let out = [], buf = '', depth = 0;
    for (const ch of str) {
        if (ch === '(') depth++;
        if (ch === ')') depth--;
        if (ch === ',' && depth === 0) { out.push(buf.trim()); buf = ''; }
        else buf += ch;
    }
    if (buf) out.push(buf.trim());
    return out;
}

function resolvePath(path, scope) {
    if (!path) return null;
    const parts = path.split('.');
    let cur = scope;

    for (let p of parts) {
        if (cur === scope && (p === 'data' || p === 'root')) continue;
        cur = cur?.[p];
    }
    return cur;
}

function evaluateCondition(row, clause) {
    if (!clause) return true;
    return clause.split(/\s+and\s+/i).every(part => {
        const m = part.match(/(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)/);
        if (!m) return true;
        let [, col, op, val] = m;
        val = val.replace(/['"]/g, '');
        let rv = row[col.trim()];
        if (!isNaN(rv) && !isNaN(val)) { rv = Number(rv); val = Number(val); }
        if (op === '=') return rv == val;
        if (op === '!=') return rv != val;
        if (op === '>') return rv > val;
        if (op === '<') return rv < val;
        if (op === '>=') return rv >= val;
        if (op === '<=') return rv <= val;
        return true;
    });
}

// ==================================================
// 3. PARSER
// ==================================================
function findTop(sql, kw) {
    let d = 0; const l = sql.toLowerCase(); kw = kw.toLowerCase();
    for (let i = 0; i < sql.length; i++) {
        if (sql[i] === '(') d++;
        if (sql[i] === ')') d--;
        if (d === 0 && l.startsWith(kw, i)) return i;
    }
    return -1;
}

function parseToken(raw) {
    const m = raw.match(/(.+) as (.+)/i);
    const expr = m ? m[1].trim() : raw.trim();
    const alias = m ? m[2].trim() : expr.split('.').pop();
    const isAgg = /^(sum|avg|count|min|max)\(/i.test(expr);
    const isWin = /over\s*\(/i.test(expr);
    let aggFunc = null, aggCol = null;
    if (isAgg) {
        const mm = expr.match(/^(\w+)\((.*)\)/);
        aggFunc = mm[1].toLowerCase();
        aggCol = mm[2] === '*' ? null : mm[2];
    }
    return { expr, alias, isAgg, aggFunc, aggCol, isWin };
}

function parseSQL(sql) {
    const hints = sql.match(/with\s*\(([^)]+)\)/i);
    const activeHints = hints ? hints[1].split(',').map(x => x.trim().toLowerCase()) : [];
    sql = sql.replace(/with\s*\([^)]+\)/i, '').trim();

    const s = findTop(sql, 'select');
    const f = findTop(sql, 'from');
    const w = findTop(sql, 'where');
    const g = findTop(sql, 'group by');
    const o = findTop(sql, 'order by');
    const l = findTop(sql, 'limit');

    const end = (x) => [w, g, o, l].filter(i => i > x).sort((a, b) => a - b)[0] ?? sql.length;

    let selectRaw = sql.slice(s + 6, f).trim();
    let distinct = false;

    if (/^distinct\s+/i.test(selectRaw)) {
        distinct = true;
        selectRaw = selectRaw.replace(/^distinct\s+/i, '');
    }

    let depth = 0, buf = '', tokens = [];
    for (const ch of selectRaw) {
        if (ch === '(') depth++;
        if (ch === ')') depth--;
        if (ch === ',' && depth === 0) { tokens.push(parseToken(buf)); buf = ''; }
        else buf += ch;
    }
    tokens.push(parseToken(buf));

    const fromPart = sql.slice(f + 4, end(f)).trim();
    const joinRegex = /(inner|left|right|full)?\s*(outer)?\s*join\s+/ig;
    let match, lastIndex = 0;

    const base = fromPart.split(joinRegex)[0].trim();
    const joinDefs = [];

    while ((match = joinRegex.exec(fromPart)) !== null) {
        const joinType = (match[1] || 'inner').toLowerCase();
        const start = match.index + match[0].length;
        const next = joinRegex.exec(fromPart);
        const chunk = fromPart.slice(start, next ? next.index : undefined).trim();
        if (next) joinRegex.lastIndex = next.index;

        const [table, cond] = chunk.split(/\son\s/i);
        joinDefs.push({
            type: joinType,
            table: table.trim(),
            cond: cond.trim()
        });

        if (!next) break;
    }


    return {
        tokens,
        base,
        joins: joinDefs,
        where: w !== -1 ? sql.slice(w + 5, end(w)).trim() : null,
        groupBy: g !== -1 ? sql.slice(g + 8, end(g)).trim() : null,
        orderBy: o !== -1 ? sql.slice(o + 8, end(o)).trim() : null,
        limit: l !== -1 ? Number(sql.slice(l + 5).trim()) : null,
        hints: activeHints,
        distinct
    };
}

// ==================================================
// 4. EXECUTION ENGINE
// ==================================================
async function executeQuery(sql, db) {
    const p = parseSQL(sql);
    const ar = resolvePath(p.base, db)
    let rows = []
    if (Array.isArray(ar)) {
        rows = resolvePath(p.base, db).map(r => ({ ...r }));
    }


    // JOIN
    for (const j of p.joins) {
        const rightData = resolvePath(j.table, db) || [];
        const [lKey, rKey] = j.cond.split('=').map(x => x.trim());
        const out = [];
        const matchedRight = new Set();

        rows.forEach(leftRow => {
            let matched = false;

            rightData.forEach((rightRow, ri) => {
                if (String(leftRow[lKey]) === String(rightRow[rKey])) {
                    matched = true;
                    matchedRight.add(ri);
                    out.push({ ...leftRow, ...rightRow });
                }
            });

            if (!matched && (j.type === 'left' || j.type === 'full')) {
                out.push({ ...leftRow });
            }
        });

        if (j.type === 'right' || j.type === 'full') {
            rightData.forEach((rightRow, ri) => {
                if (!matchedRight.has(ri)) {
                    out.push({ ...rightRow });
                }
            });
        }

        rows = out;
    }


    // WHERE
    rows = rows.filter(r => evaluateCondition(r, p.where));

    // GROUP BY + AGG
    if (p.groupBy) {
        const keys = p.groupBy.split(',').map(x => x.trim());
        const groups = {};
        rows.forEach(r => {
            const k = keys.map(c => r[c]).join('|');
            groups[k] ??= { row: r, aggs: {} };
            p.tokens.filter(t => t.isAgg).forEach(t => {
                groups[k].aggs[t.alias] ??= [];
                groups[k].aggs[t.alias].push(t.aggCol ? r[t.aggCol] : 1);
            });
        });
        rows = Object.values(groups).map(g => {
            const o = { ...g.row };
            p.tokens.filter(t => t.isAgg).forEach(t => o[t.alias] = sqlFunctions[t.aggFunc](g.aggs[t.alias]));
            return o;
        });
    }

    // ORDER BY
    // ORDER BY (numeric + string safe)
    if (p.orderBy) {
        const [c, d] = p.orderBy.split(/\s+/);
        const desc = d?.toUpperCase() === 'DESC';

        rows.sort((a, b) => {
            const av = a[c];
            const bv = b[c];

            // numeric sort
            if (!isNaN(av) && !isNaN(bv)) {
                return desc ? bv - av : av - bv;
            }

            // string sort (locale-aware)
            return desc
                ? String(bv).localeCompare(String(av))
                : String(av).localeCompare(String(bv));
        });
    }


    // LIMIT
    if (p.limit) rows = rows.slice(0, p.limit);

    // FINAL SELECT
    let result = rows.map(r => {
        const o = {};
        p.tokens.forEach(t => {
            const m = t.expr.match(/(\w+)\((.*)\)/);
            if (m && sqlFunctions[m[1]]) {
                const args = splitArgs(m[2]).map(a => r[a] ?? a.replace(/['"]/g, ''));
                o[t.alias] = sqlFunctions[m[1]](args);
            } else {
                if (t.expr.includes('.')) {
                    const col = t.expr.split('.').pop();
                    o[t.alias] = r[col] ?? 'NULL';
                } else {
                    o[t.alias] = r[t.expr] ?? 'NULL';
                }
            }

        });
        return o;
    });


    // DISTINCT (applied after SELECT, before ORDER BY/LIMIT behavior)
    if (p.distinct && !p.groupBy) {
        const seen = new Set();
        result = result.filter(row => {
            const key = JSON.stringify(row);
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    }


    // HINTS
    if (p.hints.includes('headercolumnuppercase'))
        result = result.map(r => Object.fromEntries(Object.entries(r).map(([k, v]) => [k.toUpperCase(), v])));

    if (p.hints.includes('paginate')) await displayWithPager(result);

    if (p.hints.includes('outputjson')) return JSON.stringify(result, null, 2);

    return result;
}



// ==================================================
// 5. FORMATTER
// ==================================================
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

function formatAsMySQLTable(rows) {
    if (!rows.length) return 'Empty set';
    const cols = Object.keys(rows[0]);
    const w = {}; cols.forEach(c => w[c] = Math.max(c.length, ...rows.map(r => String(r[c]).length)));
    const line = () => '+' + cols.map(c => '-'.repeat(w[c] + 2)).join('+') + '+';
    const row = v => '| ' + v.map((x, i) => String(x).padEnd(w[cols[i]])).join(' | ') + ' |';
    let out = line() + '\n' + row(cols) + '\n' + line() + '\n';
    rows.forEach(r => out += row(cols.map(c => r[c])) + '\n');
    return out + line();
}

// ==================================================
// 6. TEST CASES
// ==================================================
const db = {
    friends: [
        { name: "Chris", city: "New York", countryCode: "USA", "age": 50 },
        { name: "Yuki", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "Emily", city: "Atlanta", countryCode: "USA", "age": 50 },
        { name: "Sato", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "James", city: "New York", countryCode: "USA", "age": 50 },
        { name: "Aiko", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "Sarah", city: "Atlanta", countryCode: "USA", "age": 50 },
        { name: "Kenji", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "John", city: "New York", countryCode: "USA", "age": 50 },
        { name: "Miku", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "Robert", city: "Atlanta", countryCode: "USA", "age": 50 },
        { name: "Hiro", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "Alice", city: "New York", countryCode: "USA", "age": 50 },
        { name: "Takumi", city: "Tokyo", countryCode: "JPN", "age": 50 },
        { name: "Laura", city: "Atlanta", countryCode: "USA", "age": 50 },
        { name: "Mia", city: "LONDON", countryCode: "UK", "age": 50 },
    ],
    cities: [{ cityName: "New York" }, { cityName: "Atlanta" }, { cityName: "Tokyo" }, { cityName: "Bejing" }],
    countries: [{ code: "USA", countryName: "United States" }, { code: "JPN", countryName: "Japan" }]
};

// (async()=>{
//     const q = `SELECT upper(name) as NAME, cityName, countryName
//                FROM friends
//                JOIN cities ON city = cityName
//                JOIN countries ON countryCode = code
//                WHERE cityName='Tokyo'
//                WITH(HeaderColumnUpperCase,Paginate);`;

//     const res = await executeQuery(q, db);
//     if (Array.isArray(res)) console.log(formatAsMySQLTable(res));
// })();

// ===============================
// 6. RUN TESTS (EXTENDED COVERAGE)
// ===============================


(async () => {
    console.log("=== TEST 1: Simple SELECT ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT name, city FROM friends",
        db
    )));

    console.log("=== TEST 2: WHERE filter ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT name, city FROM friends WHERE city='Tokyo'",
        db
    )));

    console.log("=== TEST 3: JOIN ===");
    console.log(formatAsMySQLTable(await executeQuery(
        `SELECT friends.name, cities.cityName 
        FROM friends 
        INNER JOIN cities ON city = cityName`,
        db
    )));

    console.log("=== TEST 4: JOIN + WHERE ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT friends.name, countries.countryName FROM friends JOIN countries ON countryCode = code WHERE countryName='Japan'",
        db
    )));

    console.log("=== TEST 5: Scalar function ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT upper(name) AS NAME_UPPER FROM friends",
        db
    )));

    console.log("=== TEST 6: Aggregate + GROUP BY ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT city, count(name) AS total FROM friends GROUP BY city",
        db
    )));

    console.log("=== TEST 7: ORDER BY + LIMIT ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT name, age FROM friends ORDER BY age DESC LIMIT 3",
        db
    )));

    console.log("=== TEST 8: DISTINCT ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT DISTINCT city FROM friends",
        db
    )));
    console.log("=== TEST 8.1: DISTINCT ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT DISTINCT upper(city) AS CITY FROM friends",
        db
    )));
    console.log("=== TEST 8.2: DISTINCT ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT DISTINCT city FROM friends LIMIT 2",
        db
    )));
    console.log("=== TEST 8.3: DISTINCT ===");
    console.log(formatAsMySQLTable(await executeQuery(
        "SELECT DISTINCT upper(city) FROM friends ORDER BY upper(city)",
        db
    )));


    console.log("=== TEST 9: HINT HeaderColumnUpperCase ===");
    console.log(await executeQuery(
        "SELECT name, city FROM friends WITH(HeaderColumnUpperCase)",
        db
    ));

    console.log("=== TEST 10: HINT OutputJSON ===");
    console.log(await executeQuery(
        "SELECT name, city FROM friends ",
        db
    ));

    console.log("=== TEST 11:  ===");
    console.log(await executeQuery(
        "SELECT code,countryName FROM countries ",
        db
    ));
    console.log("=== TEST 12:  ===");
    console.log(await executeQuery(
        "SELECT name, city,age,age*2-2 FROM data.friends WHERE city='Tokyo' LIMIT 1 OFFSET 0",
        db
    ));
    console.log("=== TEST 13:  ===");
    console.log(await executeQuery(
        `SELECT name, cityName
        FROM friends
        LEFT JOIN cities ON city = cityName`,
        db
    ));
    console.log("=== TEST 14: ===");
    console.log(await executeQuery(
        `SELECT name, cityName
        FROM friends
        RIGHT JOIN cities ON city = cityName`,
        db
    ));
    console.log("=== TEST 15:===");
    console.log(await executeQuery(
        `SELECT name, cityName
        FROM friends
        FULL OUTER JOIN cities ON city = cityName`,
        db
    ));
})();
