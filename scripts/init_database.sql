/*
=============================================
Create Database and Schemas
=============================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the DB exists, it is dropped and recreated. Additionally, the script sets up 3 schemas
	within the DB: 'bronze', 'silver', and 'gold'.

WARNING:
	Running the script will drop the entire 'DatabaseWarehouse'
	database if it exists. All data in the DB will be permanently
	deleted. Proceed with caution and ensure you have proper backups.
*/
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
	END;
	GO


CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO -- GO is a like separater 
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
