const chevrotain = require("chevrotain");
const readline = require('readline');
const { createToken, Lexer, EmbeddedActionsParser } = chevrotain;

// --- TOKENS ---
const WhiteSpace = createToken({ name: "WhiteSpace", pattern: /\s+/, group: Lexer.SKIPPED });
const Select = createToken({ name: "Select", pattern: /SELECT/i });
const From = createToken({ name: "From", pattern: /FROM/i });
const Join = createToken({ name: "Join", pattern: /JOIN/i });
const On = createToken({ name: "On", pattern: /ON/i });
const Paginate = createToken({ name: "Paginate", pattern: /PAGINATE/i });
const Comma = createToken({ name: "Comma", pattern: /,/ });
const Equals = createToken({ name: "Equals", pattern: /=/ });
const Identifier = createToken({ name: "Identifier", pattern: /[a-zA-Z_][a-zA-Z0-9._]*/ });

const allTokens = [WhiteSpace, Select, From, Join, On, Paginate, Comma, Equals, Identifier];
const SqlLexer = new Lexer(allTokens);

// --- PARSER ---
class SqlParser extends EmbeddedActionsParser {
    constructor() {
        super(allTokens);
        const $ = this;
        $.RULE("selectStatement", () => {
            $.CONSUME(Select);
            const selectTokens = $.SUBRULE($.columnList);
            $.CONSUME(From);
            const baseTable = $.CONSUME(Identifier).image;
            const joins = [];
            $.MANY(() => {
                $.CONSUME(Join);
                const tablePath = $.CONSUME2(Identifier).image;
                $.CONSUME(On);
                const left = $.CONSUME3(Identifier).image;
                $.CONSUME(Equals);
                const right = $.CONSUME4(Identifier).image;
                joins.push({ tablePath, condition: [left, right] });
            });
            let shouldPaginate = false;
            $.OPTION(() => { $.CONSUME(Paginate); shouldPaginate = true; });
            return { selectTokens, baseTable, joins, shouldPaginate };
        });
        $.RULE("columnList", () => {
            const cols = [$.SUBRULE($.columnItem)];
            $.MANY(() => { $.CONSUME(Comma); cols.push($.SUBRULE2($.columnItem)); });
            return cols;
        });
        $.RULE("columnItem", () => {
            const expr = $.CONSUME(Identifier).image;
            const alias = expr.includes('.') ? expr.split('.').pop() : expr;
            return { expr, alias };
        });
        this.performSelfAnalysis();
    }
}
const parser = new SqlParser();

// --- PAGER & FORMATTER ---
function formatAsMySQLTable(rows) {
    if (!rows.length) return "Empty set";
    const columns = Object.keys(rows[0]);
    const widths = {};
    columns.forEach(col => widths[col] = Math.max(col.length, ...rows.map(r => String(r[col]).length)));
    const line = () => "+" + columns.map(c => "-".repeat(widths[c] + 2)).join("+") + "+";
    const rowStr = (vals) => "| " + vals.map((v, i) => String(v).padEnd(widths[columns[i]])).join(" | ") + " |";
    let output = line() + "\n" + rowStr(columns) + "\n" + line() + "\n";
    rows.forEach(r => output += rowStr(columns.map(c => r[c])) + "\n");
    return output + line();
}

async function startPager(rows, pageSize = 5) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    let offset = 0;
    while (offset < rows.length) {
        console.clear();
        const page = rows.slice(offset, offset + pageSize);
        console.log(`\n--- Rows ${offset + 1} to ${Math.min(offset + pageSize, rows.length)} of ${rows.length} ---`);
        console.log(formatAsMySQLTable(page));
        if (offset + pageSize >= rows.length) {
            await new Promise(res => rl.question('End of data. Press [ENTER] to exit.', res));
            break;
        }
        const input = await new Promise(res => rl.question('Press [ENTER] for next, or ":q" to quit: ', res));
        if (input.toLowerCase() === ':q') break;
        offset += pageSize;
    }
    rl.close();
}

// --- ENGINE ---
async function executeQuery(sql, rootData) {
    const lexResult = SqlLexer.tokenize(sql);
    parser.input = lexResult.tokens;
    const parsed = parser.selectStatement();

    let rows = (rootData[parsed.baseTable] || []).map(r => {
        const ns = {};
        Object.keys(r).forEach(k => ns[`${parsed.baseTable}.${k}`] = r[k]);
        return { ...r, ...ns };
    });

    parsed.joins.forEach(join => {
        const nextData = rootData[join.tablePath] || [];
        const [lCol, rCol] = join.condition;
        const newRows = [];
        rows.forEach(exRow => {
            const matches = nextData.filter(m => String(exRow[lCol] || exRow[lCol.split('.').pop()]) === String(m[rCol] || m[rCol.split('.').pop()]));
            matches.forEach(m => {
                const nsMatch = {};
                Object.keys(m).forEach(k => nsMatch[`${join.tablePath}.${k}`] = m[k]);
                newRows.push({ ...exRow, ...m, ...nsMatch });
            });
        });
        rows = newRows;
    });

    const results = rows.map(row => {
        const res = {};
        parsed.selectTokens.forEach(t => res[t.alias] = row[t.expr] ?? row[t.alias] ?? "NULL");
        return res;
    });

    if (parsed.shouldPaginate) {
        await startPager(results, 5);
    } else {
        console.log(formatAsMySQLTable(results));
    }
}

// --- DATA ---
const data = {
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

// --- TEST ---
// Use PAGINATE keyword to trigger the interactive mode
const sql = `SELECT friends.name, cities.cityName, countries.countryName 
             FROM friends 
             JOIN cities ON city = cityName 
             JOIN countries ON countryCode = code 
             PAGINATE`;

executeQuery(sql, data);