let data = {
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

// --- 1. REGISTRIES ---
const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    "replace": (args) => {
        const str = String(args[0] ?? "");
        const search = String(args[1] ?? "");
        const replacement = String(args[2] ?? "");
        return str.split(search).join(replacement); // Global replacement
    },
    "instr": (args) => {
        const str = String(args[0] ?? "").toLocaleLowerCase();
        const sub = String(args[1] ?? "").toLocaleLowerCase();
        // SQL INSTR returns 1-based index, 0 if not found
        const idx = str.indexOf(sub);
        return idx === -1 ? 0 : idx + 1;
    },
    "contains": (args) => {
        const str = String(args[0] ?? "").toLocaleLowerCase();
        const sub = String(args[1] ?? "").toLocaleLowerCase();
        return str.includes(sub);
    },
    "substr": (args) => {
        const str = String(args[0] ?? "");
        const start = (parseInt(args[1]) || 1) - 1;
        const len = args[2] ? parseInt(args[2]) : undefined;
        return str.substr(start, len);
    },
    ...Object.getOwnPropertyNames(Math).reduce((acc, name) => {
        if (typeof Math[name] === 'function') {
            acc[name.toLowerCase()] = (args) => Math[name](...args.map(a => parseFloat(a || 0)));
        }
        return acc;
    }, {}),
    "abs": (args) => Math.abs(parseFloat(args[0] || 0)),
    "log10": (args) => Math.log10(parseFloat(args[0] || 0)),
    "log": (args) => Math.log(parseFloat(args[0] || 0)),
    "round": (args) => Math.round(parseFloat(args[0] || 0)),
    "concat": (args) => args.join(""),
    "pow": (args) => Math.pow(parseFloat(args[0] || 0), parseFloat(args[1] || 0)),
    "mod": (args) => parseFloat(args[0] || 0) % parseFloat(args[1] || 1),
};

const sqlConstants = { "e": Math.E, "pi": Math.PI };

// --- 2. PARSER ---

function isColumnName(token) {
    // Check against all keys in our expanded registry + constants
    const reserved = Object.keys(sqlFunctions)
        .concat(Object.keys(sqlConstants))
        .concat(["eval", "select", "from", "as"]);

    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token) && !reserved.includes(token.toLowerCase());
}

function getSelectTokens(sql) {
    const lower = sql.toLowerCase();
    const s = lower.indexOf("select");
    const f = lower.indexOf("from");
    const expr = sql.slice(s + 6, f).trim();

    let result = [];
    let depth = 0;
    let buf = "";

    for (let ch of expr) {
        if (ch === "(") depth++;
        if (ch === ")") depth--;
        if (ch === "," && depth === 0) {
            result.push(parseAlias(buf.trim()));
            buf = "";
        } else {
            buf += ch;
        }
    }
    if (buf) result.push(parseAlias(buf.trim()));
    return result;
}

// Helper to split "expression AS alias"
function parseAlias(token) {
    const parts = token.split(/\s+as\s+/i);
    if (parts.length === 2) {
        return { expr: parts[0].trim(), alias: parts[1].trim() };
    }
    return { expr: token, alias: token }; // Default alias is the expression itself
}

function getNestedFunctionCalls(expr) {
    let stack = [], result = [];
    for (let i = 0; i < expr.length; i++) {
        if (expr[i] === "(") {
            let j = i - 1;
            while (j >= 0 && /[a-zA-Z0-9_]/.test(expr[j])) j--;
            stack.push({ fn: expr.slice(j + 1, i), open: i });
        }
        if (expr[i] === ")") {
            const last = stack.pop();
            const rawArgs = expr.slice(last.open + 1, i);
            let args = [], argBuf = "", argDepth = 0;
            for (let char of rawArgs) {
                if (char === "(") argDepth++;
                if (char === ")") argDepth--;
                if (char === "," && argDepth === 0) {
                    args.push(argBuf.trim());
                    argBuf = "";
                } else { argBuf += char; }
            }
            if (argBuf) args.push(argBuf.trim());
            const mappedArgs = args.map(a => isColumnName(a) ? "$" + a : a).join(",");
            result.push({
                fn: last.fn,
                args: mappedArgs,
                fullMatch: expr.slice(last.open - last.fn.length, i + 1)
            });
        }
    }
    return result;
}

// --- 3. BUILDER ---

function buildSelectPlan(selectTokens) {
    let pipeline = [], projection = [], aliases = [], tempIndex = 0;

    for (const tokenObj of selectTokens) {
        const token = tokenObj.expr;
        const finalAlias = tokenObj.alias;

        if (isColumnName(token)) {
            projection.push(token);
            aliases.push(finalAlias);
            continue;
        }

        if (/[+\-*/]/.test(token) && !token.includes("(")) {
            const temp = `$t${tempIndex++}`;
            pipeline.push({ out: temp, fn: "eval", args: token });
            projection.push(temp);
            aliases.push(finalAlias);
            continue;
        }

        const calls = getNestedFunctionCalls(token);
        if (calls.length > 0) {
            let lastTemp = null;
            let currentTokenWorkingString = token;

            for (let i = 0; i < calls.length; i++) {
                const call = calls[i];
                const temp = `$t${tempIndex++}`;

                const startIdx = currentTokenWorkingString.indexOf(call.fn + "(") + call.fn.length + 1;
                let d = 1, endIdx = startIdx;
                while (d > 0 && endIdx < currentTokenWorkingString.length) {
                    if (currentTokenWorkingString[endIdx] === "(") d++;
                    if (currentTokenWorkingString[endIdx] === ")") d--;
                    endIdx++;
                }
                const stepArgsRaw = currentTokenWorkingString.slice(startIdx, endIdx - 1);

                const stepArgs = stepArgsRaw.split(",").map(a => {
                    a = a.trim();
                    return (isColumnName(a)) ? "$" + a : a;
                }).join(",");

                pipeline.push({ out: temp, fn: call.fn, args: stepArgs });
                const fullCallText = currentTokenWorkingString.slice(startIdx - call.fn.length - 1, endIdx);
                currentTokenWorkingString = currentTokenWorkingString.replace(fullCallText, temp);

                lastTemp = temp;
            }
            projection.push(lastTemp);
            aliases.push(finalAlias);
        }
    }
    return { pipeline, projection, aliases };
}
// --- 4. EXECUTION ---

function evaluateExpression(expr, row, vars = {}) {
    let evalStr = expr.replace(/\$t\d+|[a-zA-Z_][a-zA-Z0-9_]*/g, (match) => {
        const m = match.toLowerCase();
        
        // 1. If it's a boolean literal, return it as-is for JS to use
        if (m === "true" || m === "false") return m; 
        
        // 2. If it's a function
        if (sqlFunctions[m]) return `sqlFunctions.${m}`;
        
        // 3. If it's a variable or column
        if (vars[match] !== undefined) return JSON.stringify(vars[match]);
        if (row[match] !== undefined) return JSON.stringify(row[match]);
        if (sqlConstants[m] !== undefined) return sqlConstants[m];
        
        return match; 
    });

    // ... (rest of your function call replacement logic)
}

function executePlan(plan, row) {
    let vars = {};
    for (const step of plan.pipeline) {
        const rawArgs = step.args.split(",").map(a => a.trim());
        const getVal = (arg) => {
            if (!arg) return "";
            if (arg.startsWith("$t")) return vars[arg];
            if (arg.startsWith("$")) return row[arg.slice(1)];
            if (sqlConstants[arg.toLowerCase()] !== undefined) return sqlConstants[arg.toLowerCase()];
            if (/^['"].*['"]$/.test(arg)) return arg.replace(/['"]/g, "");
            if (/[+\-*/]/.test(arg)) return evaluateExpression(arg, row, vars);
            return arg;
        };

        const resolvedArgs = rawArgs.map(getVal);
        const fnName = step.fn.toLowerCase();

        if (fnName === "eval") {
            vars[step.out] = evaluateExpression(step.args, row, vars);
        } else if (sqlFunctions[fnName]) {
            vars[step.out] = sqlFunctions[fnName](resolvedArgs);
        }
    }

    const obj = {};
    plan.projection.forEach((p, i) => {
        const alias = plan.aliases[i];
        obj[alias] = (typeof p === "string" && p.startsWith("$t")) ? vars[p] : row[p];
    });
    return obj;
}

// --- 5. FORMATTER ---

function formatAsMySQLTable(rows) {
    if (!rows.length) return "Empty Set";
    const columns = Object.keys(rows[0]);
    const widths = {};
    columns.forEach(col => {
        widths[col] = Math.max(col.length, ...rows.map(r => String(r[col] ?? "").length));
    });
    const line = () => "+" + columns.map(c => "-".repeat(widths[c] + 2)).join("+") + "+";
    const rowStr = (vals) => "| " + vals.map((v, i) => String(v ?? "").padEnd(widths[columns[i]])).join(" | ") + " |";
    let output = line() + "\n" + rowStr(columns) + "\n" + line() + "\n";
    rows.forEach(r => output += rowStr(columns.map(c => r[c])) + "\n");
    return output + line();
}

function parseSQL(sql) {
    const lower = sql.toLowerCase();
    const selectIdx = lower.indexOf("select");
    const fromIdx = lower.indexOf("from");
    const whereIdx = lower.indexOf("where");

    // 1. Extract SELECT tokens
    const selectPart = sql.slice(selectIdx + 6, fromIdx).trim();
    const selectTokens = getSelectTokensFromPart(selectPart);

    // 2. Extract WHERE part (if exists)
    let whereClause = null;
    if (whereIdx !== -1) {
        whereClause = sql.slice(whereIdx + 5).trim();
    }

    return { selectTokens, whereClause };
}

// Refactored helper to parse tokens
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

// ... (Keep sqlFunctions and sqlConstants from previous steps) ...

function filterRow(whereClause, row) {
    if (!whereClause) return true;

    // 1. Handle SQL operators translation
    let jsCriteria = whereClause
        // Handle Equality (=) and Inequality (<>)
        .replace(/([^<>!])=([^=])/g, '$1 == $2')
        .replace(/<>/g, '!=')
        // Handle Logical Operators (Case Insensitive)
        .replace(/\bAND\b/gi, '&&')
        .replace(/\bOR\b/gi, '||')
        .replace(/\bNOT\b/gi, '!');

    // 2. Evaluate using your existing expression engine
    try {
        const result = evaluateExpression(jsCriteria, row, {});
        return Boolean(result);
    } catch (e) {
        return false;
    }
}

// --- MAIN EXECUTION ---
function runQuery(sql, sourceData) {
    const { selectTokens, whereClause } = parseSQL(sql);
    const plan = buildSelectPlan(selectTokens);

    // 1. Filter rows first (WHERE)
    const filteredData = sourceData.filter(row => filterRow(whereClause, row));

    // 2. Transform rows (SELECT)
    const result = filteredData.map(row => executePlan(plan, row));

    return result;
}

// --- RUNNING THE TEST ---

// let sqlQuery = `select 
//     name, 
//     age, 
//     city AS location 
// from data.$friends 
// where (age > 25 AND age < 40) OR city='Manhattan'`;


let sqlQuery = `select 
    name, 
    age, 
    city AS location 
from data.$friends 
where contains(city,'man')`;

const finalResult = runQuery(sqlQuery, data.friends);
console.log(formatAsMySQLTable(finalResult));


