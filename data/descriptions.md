# Test Files.

## test-any-engine.csv

- `key1`
    - `VARCHAR` in any cases.
- `value1`
    - `INTEGER` in any cases.
- `value2`
    - `VARCHAR` in any cases because of a zero-starting integer.
- `value3`
    - `INTEGER` normally.
    - `VARCHAR` when NULL value is not ''.
- `value4`
    - `VARCHAR` normally.
    - `INTEGER` when NULL value is NULL.
- `value5`
    - `VARCHAR` normally.
    - `INTEGER` when only 2 rows are used for type inference.
