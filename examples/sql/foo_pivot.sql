CREATE TABLE foo_pivot AS
SELECT %(foo_pivot_select)s
FROM %(foo_pivot_from)s
WHERE %(foo_pivot_where)s