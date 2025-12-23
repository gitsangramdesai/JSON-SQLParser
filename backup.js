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

const sqlFunctions = {
    "upper": (args) => String(args[0] ?? "").toUpperCase(),
    "lower": (args) => String(args[0] ?? "").toLowerCase(),
    "substr": (args) => {
        const str = String(args[0] ?? "");
        const start = (parseInt(args[1]) || 1) - 1;
        const len = args[2] ? parseInt(args[2]) : undefined;
        return str.substr(start, len);
    },
    "abs": (args) => Math.abs(parseFloat(args[0] || 0)),
    "log10": (args) => Math.log10(parseFloat(args[0] || 0)),
    "round": (args) => Math.round(parseFloat(args[0] || 0)),
    "concat": (args) => args.join("")
};
// --- PARSING ENGINE ---

function getSelectTokens(sql) {
    const lower = sql.toLowerCase();
    const s = lower.indexOf("select");
    const f = lower.indexOf("from");
    const expr = sql.slice(s + 6, f).trim();
    let result = [], depth = 0, buf = "";
    for (let ch of expr) {
        if (ch === "(") depth++;
        if (ch === ")") depth--;
        if (ch === "," && depth === 0) {
            result.push(buf.trim());
            buf = "";
        } else { buf += ch; }
    }
    if (buf) result.push(buf.trim());
    return result;
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


function isColumnName(token) {
    const reserved = Object.keys(sqlFunctions).concat(["eval", "select", "from"]);
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token) && !reserved.includes(token.toLowerCase());
}

function buildSelectPlan(selectTokens) {
    let pipeline = [], projection = [], aliases = [], tempIndex = 0;

    for (const token of selectTokens) {
        if (isColumnName(token)) {
            projection.push(token);
            aliases.push(token);
            continue;
        }

        // Detect Arithmetic (not inside a function)
        if (/[+\-*/]/.test(token) && !token.includes("(")) {
            const temp = `$t${tempIndex++}`;
            pipeline.push({ out: temp, fn: "eval", args: token });
            projection.push(temp);
            aliases.push(token);
            continue;
        }

        const calls = getNestedFunctionCalls(token);
        if (calls.length > 0) {
            let lastTemp = null;
            for (let i = 0; i < calls.length; i++) {
                const call = calls[i];
                const temp = `$t${tempIndex++}`;
                let stepArgs = call.args;
                for (let j = 0; j < i; j++) {
                    stepArgs = stepArgs.replace(calls[j].fullMatch, `$t${tempIndex - (i - j) - 1}`);
                }
                pipeline.push({ out: temp, fn: call.fn, args: stepArgs });
                lastTemp = temp;
            }
            projection.push(lastTemp);
            aliases.push(token);
        }
    }
    return { pipeline, projection, aliases };
}

// --- EXECUTION ENGINE ---

function evaluateExpression(expr, row, vars = {}) {
    let evalStr = expr.replace(/\$t\d+|[a-zA-Z_][a-zA-Z0-9_]*/g, (match) => {
        if (vars[match] !== undefined) return JSON.stringify(vars[match]);
        if (row[match] !== undefined) return JSON.stringify(row[match]);
        return match; 
    });
    try { return Function(`return ${evalStr}`)(); } catch (e) { return NaN; }
}



function executePlan(plan, row) {
    let vars = {};
    for (const step of plan.pipeline) {
        const rawArgs = step.args.split(",").map(a => a.trim());
        const getVal = (arg) => {
            if (!arg) return "";
            if (arg.startsWith("$t")) return vars[arg];
            if (arg.startsWith("$")) return row[arg.slice(1)];
            return arg.replace(/['"]/g, ""); // Handles literals/numbers
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
        obj[plan.aliases[i]] = (typeof p === "string" && p.startsWith("$t")) ? vars[p] : row[p];
    });
    return obj;
}
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
let sqlQuery = `select 
    city,
    name,
    age-2,
    log10(100),
    substr(upper(name), 1, 1),
    upper(substr(name, 1, 1)) 
from data.$friends`;

sqlQuery = `select 
    lpad(name, 10, '.') AS padded_name,
    rpad(age, 5, '*') AS padded_age,
    ltrim('   hello') AS clean_left
from data.$friends 
where contains(city,'man')`;

const plan = buildSelectPlan(getSelectTokens(sqlQuery));
const result = data.friends.map(row => executePlan(plan, row));
console.log(formatAsMySQLTable(result));