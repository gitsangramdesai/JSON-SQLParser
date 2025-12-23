Building a Lightweight SQL Engine for In-Memory JSON Data

Have you ever wished you could query your JavaScript objects using standard SQL syntax? While libraries like AlaSQL exist, there is a certain magic (and performance benefit) in building a custom, lightweight execution pipeline tailored to your specific data needs.

In this post, I’ll walk through the architecture of JSON-SQLParser, a project I developed to bring the power of SQL—including nested functions, math libraries, and complex filtering—directly to JSON data structures.
The Challenge

Most in-memory filtering involves messy, nested .filter() and .map() chains that are hard to read and even harder to change dynamically. I wanted a way to pass a string like:
SQL

SELECT name, lpad(name, 10, '.') AS padded, age 
FROM data.$friends 
WHERE age > 25 AND contains(city, 'New')

And have it return a beautifully formatted result set.
The Architecture: A 3-Step Pipeline

The engine doesn't just "run" the string; it follows a professional compiler-style pipeline:
1. The Parser (Tokenization)

First, the engine breaks the SQL string into "Tokens." It identifies what is a column, what is a function call, and what is an alias (using the AS keyword).

    Nested Support: The parser uses a stack-based approach to handle nested functions like upper(substr(name, 1, 3)).

2. The Execution Plan (The Blueprint)

Instead of executing on the fly, the engine builds a Plan. This is a series of steps that calculate intermediate "temporary variables" (like $t0, $t1). This prevents redundant calculations and allows us to handle complex dependencies in the SELECT clause.
3. The Executor (Virtual Machine)

The executor takes the Plan and the Data. It uses a custom evaluateExpression function that safely maps SQL logic into JavaScript operations. It handles:

    Math Library: Automatic integration of the JS Math object (sqrt, sin, log, etc.).
    String Utilities: Custom implementations of lpad, trim, instr, and contains.

Key Features
🚀 Case-Insensitive String Searching

In standard JS, .includes() is case-sensitive. My engine's contains() and instr() functions use .toLocaleLowerCase() internally, making queries more robust and user-friendly.
🧮 Dynamic Math Integration

By dynamically reducing the JavaScript Math object, the engine supports dozens of mathematical functions out-of-the-box without manual boilerplate code.
🔍 Advanced WHERE Logic

The engine translates SQL’s AND, OR, and NOT into JavaScript’s &&, ||, and !. This allows for complex conditional filtering that feels exactly like writing a Postgres or MySQL query.
Putting it to the Test

Using a sample dataset of "friends," we ran a Master Query to stress-test the pipeline:
JavaScript

let sqlQuery = `select 
    name, 
    lpad(name, 12, '-') AS formatted_name,
    age,
    instr(city, 'New') AS city_index,
    upper(city) AS location 
from data.$friends 
where contains(city, 'man') OR (age > 20 AND length(name) <= 5)`;

The Result:

The output is rendered in a classic MySQL-style CLI table, proving that the aliasing and formatting logic work in harmony:

+----------+----------------+-----+------------+-----------+
| name     | formatted_name | age | city_index | location  |
+----------+----------------+-----+------------+-----------+
| Joe      | ---------Joe   | 32  | 1          | NEW YORK  |
| Robert   | ------Robert   | 45  | 0          | MANHATTAN |
+----------+----------------+-----+------------+-----------+

What's Next?

The current version of JSON-SQLParser is a strong proof-of-concept for high-performance in-memory querying. Future updates will focus on:

    ORDER BY: To allow sorting of the result sets.

    LIMIT: For pagination support.

    JOINs: To link multiple JSON arrays together.

Building this engine was a deep dive into string parsing and execution logic. It proves that with just a few hundred lines of vanilla JavaScript, you can create powerful data tools that bridge the gap between SQL and JSON.

Check out the full source code here: GitHub - JSON-SQLParser
    https://github.com/gitsangramdesai/JSON-SQLParser
This code achieves:

    Flexible Parsing: Dynamically extracts columns and functions even when nested deep.

    Aliasing: Supports clean, user-defined headers using AS.

    Powerful Filtering: The WHERE clause handles complex logic and string searching.

    Extensible Functions: Adding a new function is as simple as adding a key-value pair to the sqlFunctions registry.

    Professional Output: The built-in formatter produces a standard CLI-style database table.

If you found this project interesting, feel free to star the repo or reach out with suggestions!