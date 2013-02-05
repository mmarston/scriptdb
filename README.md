#ScriptDB - Database Source Control

##Description
ScriptDB is a utility that generates scripts to recreate a SQL Server database.
These scripts can be managed with a source control system to track and coordinate database changes.

##Usage
To use ScriptDB, simply provide the server and database names:

	ScriptDB "MyServer" "MyDatabase"

ScriptDB creates scripts in the current directory.

To recreate the database, pass the top level CreateDatabaseObject.sql script to sqlcmd:

	sqlcmd -S MyServer -v DBNAME=MyDB DROPDB=True -E -b -i "CreateDatabaseObjects.sql"

Expected parameters:

* DBNAME: Name of the database that is to be created.
* DROPDB:  Determines whether the scripts will first drop any existing database with the same name ("True" or "False").

##Notes and Conventions
###Ignored Files
ScriptDB does not delete any files that already exist in the output directory tree.
This allows SQL scripts that are not part of the database to be tracked in source control in the same
directory.  In order to avoid the possibility of scripts for deleted objects remaining in output directory,
ScriptDB checks for any .sql files that were not created during the current execution and prompts the user
for an action (delete, keep, or ignore).  Ignoring a file will add an entry to IgnoreFiles.txt in the root
of the output directory.

###Views
View stubs are created in Views.sql and filled out in the corresonding _view_.sql file.
When the scripts are run to create the database, Views.sql always runs first to allow anything 
that depends a view to see the correct columns (the order in which views are filled out doesn't
matter).

###Tables
Tables are scripted out into separate files in order to handle dependencies.

In the Schemas\_schema_\Tables directory:
_table_.sql creates the table with only the table column definitions.
_table_.kci.sql adds keys, constaints and indexes to the table.
_table_.fky.sql adds foreign keys.

In the Schemas\schema_\Data
_table_.sql adds data to the table.

Table scripts are run in this order:
1. All Tables\_table_.sql scripts
2. All Data\_table_.sql scripts
3. All Tables\_table_.kci.sql scripts
4. All Tables\_table.fky.sql scripts

Data is added before the creation of indexes to reduce fragmentation.
Foreign keys are added last to ensure that the data they refer to has already been inserted.

###Users
Roles are included in the scripting process while Users intentially left out.
These should be re-added in each environment that the database is recreated in.

##Dependencies
ScriptDB depends on SQL Server Management Objects (SMO), which can be downloaded here:
<http://go.microsoft.com/fwlink/?LinkID=239658&clcid=0x409>

##Unsuported SQL Server features
Datatypes added in SQL Server 2012 are not suported (HierarchyID, Geography, etc.)

The following are not included in the scripts:

* AsymmetricKeys
* Certificates
* ExtendedStoredProcedures
* Rules
* SymmetricKeys
* Triggers
* Users

##Future additions
###In Progress Features
An incomplete, experimental command line option "ssdt" will cause ScriptDB to generate scripts compatible
with SQL Server Data Tools projects.

###Ideas for Future Development
* Allow users to limit what scripts are created (specific types, names matching a regex expression...)
* Support for new SQL Server 2012 data types