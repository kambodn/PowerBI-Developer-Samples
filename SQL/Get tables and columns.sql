USE [db]
SELECT
    t.TABLE_SCHEMA AS SchemaName,
    t.TABLE_NAME AS TableName,
    c.COLUMN_NAME AS ColumnName,
    c.DATA_TYPE AS DataType,
    c.CHARACTER_MAXIMUM_LENGTH AS MaxLength,
    c.IS_NULLABLE AS IsNullable
FROM
    INFORMATION_SCHEMA.TABLES t
    JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
WHERE
    t.TABLE_TYPE = 'BASE TABLE'
    AND SUBSTRING(t.TABLE_NAME, 1, 5) = 'KENYA'
ORDER BY
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.ORDINAL_POSITION;