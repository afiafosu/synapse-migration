--CREATE USER [fabricmigration] FROM EXTERNAL PROVIDER;
--EXEC sp_addrolemember 'db_owner', [fabricmigration];

--CREATE USER [m.resendiz.sanchez@accenture.com] FROM EXTERNAL PROVIDER;
--EXEC sp_addrolemember 'db_owner', [m.resendiz.sanchez@accenture.com];

SELECT name, type_desc FROM sys.database_principals --WHERE name = 'fabricmigration';