SQL Query Engine – Test Coverage

This document outlines the incremental feature validation of the custom SQL-like query engine, demonstrating how capabilities were added and tested step-by-step.

+-------+--------------------+---------------------------------------------------------------+-----------------------------------------------+
| Phase | Feature Focus      | Query Tested                                                  | Capabilities Validated                         |
+-------+--------------------+---------------------------------------------------------------+-----------------------------------------------+
| 1     | Basics             | SELECT name, city, age - 2 FROM data.$friends                | Column projection, arithmetic expressions     |
| 2     | Math Engine        | SELECT name, abs(age-35), log(e), log10(100)                 | Function registry, constants, math functions  |
| 3     | Deep Pipeline      | SELECT name, upper(lower(substr(name,1,1)))                  | Nested function execution                     |
| 4     | User Experience    | SELECT name, age-35 AS age_diff, upper(city) AS location     | Aliasing (AS), expressions + functions        |
| 5     | Logic Filter       | SELECT name, age FROM data.$friends WHERE age>25 AND age<40  | WHERE clause, comparisons, logical AND        |
| 6     | Search Expert      | SELECT name, city FROM data.$friends                         | Case-insensitive search, boolean evaluation   |
|       |                    | WHERE contains(city,'man') = true                            |                                               |
| 7     | Text Formatter     | SELECT name, lpad(name,10,'.') AS padded,                    | String padding, length, function-based WHERE  |
|       |                    | length(city) AS city_len FROM data.$friends                  |                                               |
|       |                    | WHERE length(name) > 5                                       |                                               |
| 8     | Master Query       | Full combined SELECT + WHERE + functions                     | End-to-end engine validation                  |
+-------+--------------------+---------------------------------------------------------------+-----------------------------------------------+


Master Query (Full Coverage Test)

This query validates nearly all supported features in a single execution.

SELECT
    name,
    lpad(name, 12, '-') AS formatted_name,
    age,
    instr(city, 'New') AS city_index,
    upper(city) AS location
FROM data.$friends
WHERE contains(city, 'man')
   OR (age > 20 AND length(name) <= 5);


Supported Features (Validated)

        SELECT with multiple expressions

        Column arithmetic

        Function registry (math + string)

        Nested function execution

        Constants (e, pi)

        Aliasing with AS

        WHERE clause

        Logical operators (AND, OR)

        Case-insensitive search functions

        String formatting utilities


This engine successfully demonstrates a pipeline-based SQL execution model, capable of evaluating expressions, nested functions, and logical conditions on in-memory JavaScript data structures.


Optional Next Enhancements:

        ORDER BY

        LIMIT

        JOIN

        GROUP BY