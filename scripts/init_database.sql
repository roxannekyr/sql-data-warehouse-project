/*
Create Database and Schemas
----------------------------

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted.
*/

use master;
-- Dropping Database DataWarehouse if exists 
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
    alter database DataWarehouse set single_user with rollback immediate;
    drop database DataWarehouse;
end;
-- Re-creating the Database DataWarehouse
create database DataWarehouse;
use DataWarehouse;
-- Creating schemas for each layer (bronze, silver, gold)
create schema bronze;
create schema silver;
create schema gold;
