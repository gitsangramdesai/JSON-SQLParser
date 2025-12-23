1. The "Basics" (Columns & Simple Math)

The very first version focused on selecting raw data and performing basic arithmetic directly on columns.
JavaScript

let sqlQuery = `select name, city, age - 2 from data.$friends`;

2. The "Math Engine" (Registry & Constants)

We then added support for a function registry and mathematical constants like e.
JavaScript

let sqlQuery = `select name, abs(age - 35), log(e), log10(100) from data.$friends`;

3. The "Deep Pipeline" (Nested Functions)

This version proved that the engine could handle functions inside functions without breaking the column names.
JavaScript

let sqlQuery = `select name, upper(lower(substr(name, 1, 1))) from data.$friends`;

4. The "User Experience" (Aliases)

We added the AS keyword to make the final table headers look professional.
JavaScript

let sqlQuery = `select name, age - 35 AS age_diff, upper(city) AS location from data.$friends`;

5. The "Logic Filter" (WHERE Clause & Boolean)

We implemented the WHERE clause, supporting logical operators (AND, OR) and boolean literals.
JavaScript

let sqlQuery = `select name, age from data.$friends where age > 25 AND age < 40`;

6. The "Search Expert" (Case-Insensitive Search)

You improved the contains and instr functions to ignore case, making the filter more robust.
JavaScript

let sqlQuery = `select name, city from data.$friends where contains(city, 'man') = true`;

7. The "Text Formatter" (Padding & Length)

The final stage added advanced string manipulation for reporting and data cleanup.
JavaScript

let sqlQuery = `select name, lpad(name, 10, '.') AS padded, length(city) AS city_len from data.$friends where length(name) > 5`;

All Features in One "Master" Statement

This is the most complex query your engine currently supports, combining almost everything we've worked on today:
JavaScript

let sqlQuery = `select 
    name, 
    lpad(name, 12, '-') AS formatted_name,
    age,
    instr(city, 'New') AS city_index,
    upper(city) AS location 
from data.$friends 
where contains(city, 'man') OR (age > 20 AND length(name) <= 5)`;