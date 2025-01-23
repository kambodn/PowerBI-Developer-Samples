-- Specify the read-only intent for this query
DECLARE @ApplicationIntent NVARCHAR(30) = 'ReadOnly';

-- Your SQL query goes here



SELECT TOP(1000) * FROM [vw_rollingsalecombined]




-- Optional hint to ensure read-only routing
OPTION (QUERYTRACEON 9481); 

-- Reset the ApplicationIntent to its default value (optional)
SET @ApplicationIntent = NULL;
