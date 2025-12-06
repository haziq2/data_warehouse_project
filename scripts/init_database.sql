/*
Create a database and schemas

Script purpose
This script creates a new database named 'DataWarehouse' after checking if it already exists. If it exists, it will be dropped and recreated. The script also set up three schemas within the database: bronze, silver and gold.

Warning
Running this script will drop the entire 'DataWarehouse' database if it exists. All data in the database will be permanently deleted. Proceed with caution and ensure proper backup before running the script.
*/
--------------------------------
use master;
go

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
  alter database DataWarehouse set single_user with rollback immediate;
  drop database DataWarehouse;
end;
go

create database DataWarehouse;
go

use DataWarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go
