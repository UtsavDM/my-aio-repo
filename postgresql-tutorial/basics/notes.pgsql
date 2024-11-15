-- Generic notes
SELECT COUNT(*) FROM table; -- NULLs will be counted in COUNT(*)
SELECT COUNT(column_name) FROM table; -- NULLs will NOT be counted in COUNT(column_name)
