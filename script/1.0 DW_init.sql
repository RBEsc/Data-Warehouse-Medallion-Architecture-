/*
==============================
	Create Database and Schemas
==============================

Purpose:
	This script will create new database titled "RBE_DW" and will check if there is an existing database
	If the database exists, it will be dropped and create a new one. 
	After creating the database, it will add bronze, silver and gold schemas.

Warnings:
	Running this script will erase the existing database titled "RBE_DW".
	Please proceed with caution and ensure proper backups are in place.

*/

USE master;
GO

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'RBE_DW')
BEGIN
	ALTER DATABASE RBE_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE RBE_DW;
END;
GO

-- Create RBE_DW
CREATE DATABASE RBE_DW;
GO

USE RBE_DW;
GO

-- CREATE Schemas - bronze, silver and gold
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO