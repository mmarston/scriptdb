using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Broker;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;

namespace Mercent.SqlServer.Management
{
	public class FileScripter
	{
		private List<string> fileNames = new List<string>();
		private Dictionary<string, string> fileDictionary;
		private Server server;
		private Database database;

		private static readonly string DBName = "$(DBNAME)";
		
		private string serverName;
		public string ServerName
		{
			get { return serverName; }
			set { serverName = value; }
		}

		private string databaseName;
		public string DatabaseName
		{
			get { return databaseName; }
			set { databaseName = value; }
		}
	
		private string outputDirectory = "";
		public string OutputDirectory
		{
			get { return outputDirectory; }
			set { outputDirectory = value; }
		}

		private Encoding encoding = Encoding.Default;
		public Encoding Encoding
		{
			get { return encoding; }
			set { encoding = value; }
		}	

		public void Script()
		{
			if(this.OutputDirectory.Length > 0 && !Directory.Exists(this.OutputDirectory))
				Directory.CreateDirectory(this.OutputDirectory);

			fileNames.Clear();
			
			// When using the Server(string serverName) constructor some things
			// don't work correct. In particular, some things (such as DatabaseRole.EnumRoles())
			// incorrectly query the master database (or whatever the default database is for the login)
			// rather than querying the correct database.
			// Explicitly setting the database that we want to use avoids this problem.
			SqlConnectionInfo connectionInfo = new SqlConnectionInfo();
			connectionInfo.ServerName = ServerName;
			connectionInfo.DatabaseName = DatabaseName;
			ServerConnection connection = new ServerConnection(connectionInfo);
			server = new Server(connection);
			// Set the execution mode to capture SQL (this is like saving the script
			// when editing sql objects in Management Studio).
			
			database = server.Databases[databaseName];

			PrefetchObjects();

			ScriptDatabase();
			ScriptRoles();
			ScriptSchemas();
			ScriptXmlSchemaCollections();
			ScriptServiceBrokerMessageTypes();
			ScriptServiceBrokerContracts();
			ScriptSynonyms();
			ScriptPartitionFunctions();
			ScriptPartitionSchemes();
			ScriptAssemblies();
			ScriptUserDefinedDataTypes();
			ScriptUserDefinedFunctionHeaders();
			ScriptViewHeaders();
			ScriptStoredProcedureHeaders();
			ScriptTables();
			ScriptServiceBrokerQueues();
			ScriptServiceBrokerServices();
			ScriptUserDefinedFunctionsAndViews();
			ScriptStoredProcedures();
			
			using(StreamWriter writer = new StreamWriter(Path.Combine(OutputDirectory, "CreateDatabaseObjects.sql"), false, Encoding))
			{
				writer.WriteLine(":on error exit");
				foreach(string fileName in this.fileNames)
				{
					writer.WriteLine("PRINT '{0}'", fileName);
					writer.WriteLine("GO", fileName);
					if(Path.GetExtension(fileName) == ".dat")
					{
						// Note: this won't work if the schema or table name contain a dot ('.').
						string[] tableParts = Path.GetFileNameWithoutExtension(fileName).Split(new char[]{'.'});
						string schemaName = tableParts[0];
						string tableName = tableParts[1];
						writer.WriteLine("!!bcp \"[{0}].[{1}].[{2}]\" in \"{3}\" -S $(SQLCMDSERVER) -T -n -k -E", FileScripter.DBName, schemaName, tableName, fileName);
					}
					else
					{
						writer.WriteLine(":r \"{0}\"", fileName);
					}
				}
			}

			// Here is a list of database objects that currently are not being scripted:
			//database.AsymmetricKeys;
			//database.Certificates;
			//database.ExtendedStoredProcedures;
			//database.Rules;
			//database.SymmetricKeys;
			//database.Triggers;
			//database.Users;

			fileNames.Add("CreateDatabaseObjects.sql");

			// put the filenames in a case-insensitive string dictionary
			// so that we can look them up in the PromptDeleteFiles method
			fileDictionary = new Dictionary<string, string>(fileNames.Count, StringComparer.InvariantCultureIgnoreCase);
			foreach(string fileName in fileNames)
			{
				fileDictionary.Add(fileName, fileName);
			}
			DirectoryInfo outputDirectoryInfo;
			if(OutputDirectory != "")
				outputDirectoryInfo = new DirectoryInfo(OutputDirectory);
			else
				outputDirectoryInfo = new DirectoryInfo(".");

			PromptDeleteFiles(outputDirectoryInfo, "");
		}

		private void PrefetchObjects()
		{
			Console.Write("Prefetching objects");
			ScriptingOptions prefetchOptions = new ScriptingOptions();
			prefetchOptions.AllowSystemObjects = false;
			prefetchOptions.ClusteredIndexes = true;
			prefetchOptions.DriChecks = true;
			prefetchOptions.DriClustered = true;
			prefetchOptions.DriDefaults = true;
			prefetchOptions.DriIndexes = true;
			prefetchOptions.DriNonClustered = true;
			prefetchOptions.DriPrimaryKey = true;
			prefetchOptions.DriUniqueKeys = true;
			prefetchOptions.FullTextIndexes = true;
			prefetchOptions.Indexes = true;
			prefetchOptions.NonClusteredIndexes = true;
			prefetchOptions.Permissions = true;
			prefetchOptions.Statistics = true;
			prefetchOptions.Triggers = true;
			prefetchOptions.XmlIndexes = true;
			prefetchOptions.DriForeignKeys = true;

			database.PrefetchObjects(typeof(UserDefinedType), prefetchOptions);
			Console.Write('.');

			PrefetchRoles();
			Console.Write('.');
			PrefetchFullTextCatalogs();
			Console.Write('.');
			PrefetchStoredProcedures(prefetchOptions);
			Console.Write('.');
			// Set the column fields to initialize.
			// Used to prefetch view and udf columns.
			// We manually prefetch the columns because the Database.PrefetchObjects()
			// method does not prefetch all of the column information that we need.
			// If we did not prefetch the columns here then it would query
			// each column individually when we script out headers.
			server.SetDefaultInitFields(typeof(Column),
				"DataType",
				"DataTypeSchema",
				"Length",
				"NumericPrecision",
				"NumericScale",
				"SystemType",
				"Collation",
				"XmlDocumentConstraint",
				"XmlSchemaNamespace",
				"XmlSchemaNamespaceSchema");

			PrefetchViews(prefetchOptions);
			Console.Write('.');
			PrefetchUserDefinedFunctions(prefetchOptions);
			Console.Write('.');
			database.PrefetchObjects(typeof(PartitionFunction), prefetchOptions);
			Console.Write('.');
			database.PrefetchObjects(typeof(PartitionScheme), prefetchOptions);
			Console.Write('.');
			database.PrefetchObjects(typeof(UserDefinedAggregate), prefetchOptions);
			Console.Write('.');
			PrefetchTables(prefetchOptions);
			Console.Write('.');
			PrefetchSynonyms();
			Console.Write('.');
			PrefetchServiceBrokerMessageTypes();
			Console.Write('.');
			PrefetchServiceBrokerContracts();
			Console.Write('.');
			PrefetchServiceBrokerQueues();
			Console.Write('.');
			PrefetchServiceBrokerServices();
			Console.Write('.');
			PrefetchAssemblies(prefetchOptions);
			Console.Write('.');
			database.PrefetchObjects(typeof(XmlSchemaCollection), prefetchOptions);
			Console.WriteLine('.');
		}

		private void PrefetchAssemblies(ScriptingOptions prefetchOptions)
		{
			server.SetDefaultInitFields(typeof(SqlAssembly), "AssemblySecurityLevel");
			database.Assemblies.Refresh();
			database.PrefetchObjects(typeof(SqlAssembly), prefetchOptions);
		}

		private void PrefetchFullTextCatalogs()
		{
			server.SetDefaultInitFields
			(
				typeof(FullTextCatalog),
				new string[]
				{
					"IsAccentSensitive",
					"IsDefault"
				}
			);
			database.FullTextCatalogs.Refresh();
		}

		private void PrefetchRoles()
		{
			server.SetDefaultInitFields(typeof(DatabaseRole), true);
			database.Roles.Refresh();
		}

		private void PrefetchServiceBrokerContracts()
		{
			server.SetDefaultInitFields(typeof(ServiceContract), true);
			database.ServiceBroker.ServiceContracts.Refresh();
		}

		private void PrefetchServiceBrokerMessageTypes()
		{
			server.SetDefaultInitFields(typeof(MessageType), true);
			database.ServiceBroker.MessageTypes.Refresh();
		}

		private void PrefetchServiceBrokerQueues()
		{
			server.SetDefaultInitFields(typeof(ServiceQueue), true);
			database.ServiceBroker.Queues.Refresh();
		}

		private void PrefetchServiceBrokerServices()
		{
			server.SetDefaultInitFields(typeof(BrokerService), true);
			database.ServiceBroker.Services.Refresh();
		}

		private void PrefetchStoredProcedures(ScriptingOptions prefetchOptions)
		{
			server.SetDefaultInitFields(typeof(StoredProcedureParameter), true);
			server.SetDefaultInitFields(typeof(StoredProcedure), true);

			foreach(StoredProcedure procedure in database.StoredProcedures)
			{
				if(!procedure.IsSystemObject && procedure.ImplementationType == ImplementationType.SqlClr)
				{
					procedure.Parameters.Refresh();
				}
			}

			database.PrefetchObjects(typeof(StoredProcedure), prefetchOptions);

			string sqlCommand = "SELECT o.[object_id], parameter_id, default_value\r\n"
				+ "FROM " + MakeSqlBracket(database.Name) + ".sys.objects AS o\r\n"
				+ "\tJOIN " + MakeSqlBracket(database.Name) + ".sys.parameters AS p ON p.[object_id] = o.[object_id]\r\n"
				+ "WHERE o.is_ms_shipped = 0 AND o.type = 'PC' AND p.has_default_value = 1\r\n"
				+ "ORDER BY o.[object_id]";

			StoredProcedureCollection procedures = database.StoredProcedures;
			using(SqlDataReader reader = ExecuteReader(sqlCommand))
			{
				StoredProcedure procedure = null;
				while(reader.Read())
				{
					int objectId = reader.GetInt32(0);
					int parameterId = reader.GetInt32(1);
					object sqlValue = reader.GetSqlValue(2);

					if(procedure == null || procedure.ID != objectId)
						procedure = procedures.ItemById(objectId);

					StoredProcedureParameter parameter = procedure.Parameters.ItemById(parameterId);
					DataType dataType = parameter.DataType;
					SqlDataType sqlDataType;
					if(dataType.SqlDataType == SqlDataType.UserDefinedDataType)
						sqlDataType = GetBaseSqlDataType(dataType);
					else
						sqlDataType = dataType.SqlDataType;
					parameter.DefaultValue = GetSqlLiteral(sqlValue, sqlDataType);
				}
			}
		}

		private void PrefetchSynonyms()
		{
			server.SetDefaultInitFields(typeof(Synonym), true);
			database.Synonyms.Refresh();
		}

		private void PrefetchTables(ScriptingOptions prefetchOptions)
		{
			server.SetDefaultInitFields(typeof(Table), "RowCount");
			database.Tables.Refresh();
			database.PrefetchObjects(typeof(Table), prefetchOptions);
		}

		private void PrefetchUserDefinedFunctions(ScriptingOptions prefetchOptions)
		{
			server.SetDefaultInitFields(typeof(UserDefinedFunctionParameter), true);

			server.SetDefaultInitFields(typeof(UserDefinedFunction), true);

			// Prefetch the columns for each non-system, non-scalar function.
			// Prefetch the parameters for clr functions.
			foreach(UserDefinedFunction function in database.UserDefinedFunctions)
			{
				if(!function.IsSystemObject)
				{
					// Prefetch the columns for scripting out udf headers
					if(function.FunctionType != UserDefinedFunctionType.Scalar)
						function.Columns.Refresh();
					// Prefetch the parameters for scripting out clr functions
					if(function.ImplementationType == ImplementationType.SqlClr)
						function.Parameters.Refresh();
				}
			}

			database.PrefetchObjects(typeof(UserDefinedFunction), prefetchOptions);

			string sqlCommand = "SELECT o.[object_id], parameter_id, default_value\r\n"
				+ "FROM " + MakeSqlBracket(database.Name) + ".sys.objects AS o\r\n"
				+ "\tJOIN " + MakeSqlBracket(database.Name) + ".sys.parameters AS p ON p.[object_id] = o.[object_id]\r\n"
				+ "WHERE o.is_ms_shipped = 0 AND o.type IN ('FN', 'FS', 'FT') AND p.has_default_value = 1\r\n"
				+ "ORDER BY o.[object_id]";

			UserDefinedFunctionCollection functions = database.UserDefinedFunctions;
			using(SqlDataReader reader = ExecuteReader(sqlCommand))
			{
				UserDefinedFunction function = null;
				while(reader.Read())
				{
					int objectId = reader.GetInt32(0);
					int parameterId = reader.GetInt32(1);
					object sqlValue = reader.GetSqlValue(2);

					if(function == null || function.ID != objectId)
						function = functions.ItemById(objectId);

					UserDefinedFunctionParameter parameter = function.Parameters.ItemById(parameterId);
					DataType dataType = parameter.DataType;
					SqlDataType sqlDataType;
					if(dataType.SqlDataType == SqlDataType.UserDefinedDataType)
						sqlDataType = GetBaseSqlDataType(dataType);
					else
						sqlDataType = dataType.SqlDataType;
					parameter.DefaultValue = GetSqlLiteral(sqlValue, sqlDataType);
				}
			}
		}

		private void PrefetchViews(ScriptingOptions prefetchOptions)
		{
			server.SetDefaultInitFields(typeof(View),
				"IsSchemaBound",
				"IsSystemObject");

			// Prefetch the columns for each non-system view
			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
				{
					view.Columns.Refresh();
				}
			}

			database.PrefetchObjects(typeof(View), prefetchOptions);
		}

		private void PromptDeleteFiles(DirectoryInfo dirInfo, string relativeDir)
		{
			string relativeName;
			foreach(FileInfo fileInfo in dirInfo.GetFiles())
			{
				relativeName = Path.Combine(relativeDir, fileInfo.Name);
				if(!fileDictionary.ContainsKey(relativeName))
					Console.WriteLine("Extra file: {0}", relativeName);
			}
			foreach(DirectoryInfo subDirInfo in dirInfo.GetDirectories())
			{
				if((subDirInfo.Attributes & FileAttributes.Hidden) != FileAttributes.Hidden)
					PromptDeleteFiles(subDirInfo, subDirInfo.Name);
			}
		}

		private void ScriptAssemblies()
		{
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.AppendToFile = false;
			options.Encoding = this.Encoding;
			options.Permissions = true;
			
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

			if(database.Assemblies.Count > 0)
			{
				SqlExecutionModes previousModes = server.ConnectionContext.SqlExecutionModes;
				try
				{
					server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;

					string relativeDir = "Assemblies";
					string dir = Path.Combine(OutputDirectory, relativeDir);
					if(!Directory.Exists(dir))
						Directory.CreateDirectory(dir);

					UrnCollection assemblies = new UrnCollection();

					SqlSmoObject[] objects = new SqlSmoObject[1];
					DependencyTree tree;
					foreach(SqlAssembly assembly in database.Assemblies)
					{
						// It doesn't seem to script AssemblySecurityLevel unless it has been accessed first!
						AssemblySecurityLevel securityLevel = assembly.AssemblySecurityLevel;

						string filename = Path.Combine(relativeDir, assembly.Name + ".sql");
						options.FileName = Path.Combine(OutputDirectory, filename);
						scripter.Options.AppendToFile = false;
						objects[0] = assembly;

						Console.WriteLine(options.FileName);
						scripter.ScriptWithList(objects);
						// Check if the assembly is visible.
						// If the assembly is visible then it can have CLR objects.
						// If the assembly is not visible then it is intended to be called from
						// other assemblies.
						if(assembly.IsVisible)
						{
							tree = scripter.DiscoverDependencies(objects, DependencyType.Children);

							// tree.FirstChild is the assembly and tree.FirstChild.FirstChild is the first dependent object
							if(tree.HasChildNodes && tree.FirstChild.HasChildNodes)
							{
								IDictionary<string, Urn> sortedChildren = new SortedDictionary<string, Urn>(StringComparer.InvariantCultureIgnoreCase);
								// loop through the children, which should be the SQL CLR objects such
								// as user defined functions, user defined types, etc.
								for(DependencyTreeNode child = tree.FirstChild.FirstChild; child != null; child = child.NextSibling)
								{
									// Make sure the object isn't another SqlAssembly that depends on this assembly
									// because we don't want to include the script for the other assembly in the 
									// script for this assembly
									if(child.Urn.Type != "SqlAssembly")
									{
										sortedChildren.Add(child.Urn.Value, child.Urn);
									}
								}
								// script out the dependent objects, appending to the file
								scripter.Options.AppendToFile = true;
								Urn[] children = new Urn[sortedChildren.Count];
								sortedChildren.Values.CopyTo(children, 0);
								scripter.ScriptWithList(children);
							}
						}
						else
						{
							// The create script doesn't include VISIBILITY (this appears
							// to be a bug in SQL SMO) here we reset it and call Alter()
							// to generate an alter statement.
							assembly.IsVisible = true;
							assembly.IsVisible = false;
							server.ConnectionContext.CapturedSql.Clear();
							assembly.Alter();
							StringCollection batches = server.ConnectionContext.CapturedSql.Text;
							// Remove the first string, which is a USE statement to set the database context
							batches.RemoveAt(0);
							WriteBatches(options.FileName, true, batches);
						}
						assemblies.Add(assembly.Urn);
					}

					// Determine proper order of assemblies based on dependencies
					DependencyWalker walker = new DependencyWalker(server);
					tree = walker.DiscoverDependencies(assemblies, DependencyType.Parents);
					DependencyCollection dependencies = walker.WalkDependencies(tree);
					foreach(DependencyCollectionNode node in dependencies)
					{
						// Check that the dependency is an assembly that we have scripted out
						if(assemblies.Contains(node.Urn) && node.Urn.Type == "SqlAssembly")
						{
							string fileName = node.Urn.GetAttribute("Name") + ".sql";
							this.fileNames.Add(Path.Combine(relativeDir, fileName));
						}
					}
				}
				finally
				{
					server.ConnectionContext.SqlExecutionModes = previousModes;
				}
			}
		}

		private void ScriptDatabase()
		{
			string fileName = "Database.sql";
			string outputFileName = Path.Combine(this.OutputDirectory, fileName);
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = outputFileName;
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			
			options.AllowSystemObjects = false;
			options.FullTextCatalogs = true;
			options.IncludeIfNotExists = true;
			options.NoFileGroup = true;


			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			Console.WriteLine(outputFileName);

			// Set the value of the internal ScriptName property used when scripting the database.
			// This the same property that the Transfer object sets to create the destination database.
			// The alternative (which I had previously used) was to go through the script and replace
			// the old database name with the new database name.
			typeof(Database).InvokeMember("ScriptName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.SetProperty, null, database, new string[] { FileScripter.DBName }, null);

			scripter.ScriptWithList(new SqlSmoObject[] { database });
			SqlCommand command = new SqlCommand
			(
				"SELECT @isReadCommittedSnapshotOn = is_read_committed_snapshot_on\n"
				+ "FROM sys.databases\n"
				+ "WHERE database_id = @databaseID;"
			);
			SqlParameterCollection parameters = command.Parameters;
			parameters.AddWithValue("@databaseID", database.ID);
			SqlParameter isReadCommittedSnapshotOnParameter = parameters.Add("@isReadCommittedSnapshotOn", SqlDbType.Bit);
			isReadCommittedSnapshotOnParameter.Direction = ParameterDirection.Output;
			ExecuteNonQuery(command);
			bool isReadCommittedSnapshotOn = (bool)isReadCommittedSnapshotOnParameter.Value;
			using(TextWriter writer = new StreamWriter(outputFileName, true, Encoding))
			{
				writer.WriteLine("ALTER DATABASE [{0}] SET READ_COMMITTED_SNAPSHOT {1}", FileScripter.DBName, (isReadCommittedSnapshotOn ? "ON" : "OFF"));
				writer.WriteLine("GO");
				writer.WriteLine("USE [{0}]", FileScripter.DBName);
				writer.WriteLine("GO");
			}
			this.fileNames.Add(fileName);
		}

		private void ScriptTables()
		{
			ScriptingOptions tableOptions = new ScriptingOptions();
			tableOptions.Encoding = this.Encoding;

			Scripter tableScripter = new Scripter(server);
			tableScripter.Options = tableOptions;
			tableScripter.PrefetchObjects = false;
			
			// this list might be able to be trimmed down because
			// some of the options may overlap (e.g. DriIndexes and Indexes).
			ScriptingOptions kciOptions = new ScriptingOptions();
			kciOptions.Encoding = this.Encoding;
			kciOptions.PrimaryObject = false;
			kciOptions.ClusteredIndexes = true;
			kciOptions.DriChecks = true;
			kciOptions.DriClustered = true;
			kciOptions.DriDefaults = true;
			kciOptions.DriIncludeSystemNames = true;
			kciOptions.DriIndexes = true;
			kciOptions.DriNonClustered = true;
			kciOptions.DriPrimaryKey = true;
			kciOptions.DriUniqueKeys = true;
			kciOptions.FullTextIndexes = true;
			kciOptions.Indexes = true;
			kciOptions.NonClusteredIndexes = true;
			kciOptions.Permissions = true;
			kciOptions.Statistics = true;
			kciOptions.Triggers = true;
			kciOptions.XmlIndexes = true;

			Scripter kciScripter = new Scripter(server);
			kciScripter.Options = kciOptions;
			kciScripter.PrefetchObjects = false;

			ScriptingOptions fkyOptions = new ScriptingOptions();
			fkyOptions.Encoding = this.Encoding;
			fkyOptions.DriForeignKeys = true;
			fkyOptions.DriIncludeSystemNames = true;
			fkyOptions.PrimaryObject = false;
			fkyOptions.SchemaQualifyForeignKeysReferences = true;

			Scripter fkyScripter = new Scripter(server);
			fkyScripter.Options = fkyOptions;
			fkyScripter.PrefetchObjects = false;

			string relativeDir = "Tables";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);
			
			string relativeDataDir = "Data";
			string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
			if(!Directory.Exists(dataDir))
				Directory.CreateDirectory(dataDir);

			List<string> tabFileNames = new List<string>();
			List<string> kciFileNames = new List<string>();
			List<string> fkyFileNames = new List<string>();

			SqlSmoObject[] objects = new SqlSmoObject[1];

			foreach (Table table in database.Tables)
			{
				if (!table.IsSystemObject)
				{
					objects[0] = table;
					string fileName = Path.Combine(relativeDir, table.Schema + "." + table.Name + ".tab");
					tabFileNames.Add(fileName);
					string outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, tableScripter.ScriptWithList(objects));

					fileName = Path.ChangeExtension(fileName, ".kci");
					kciFileNames.Add(fileName);
					outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, kciScripter.ScriptWithList(objects));

					fileName = Path.ChangeExtension(fileName, ".fky");
					fkyFileNames.Add(fileName);
					outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, fkyScripter.ScriptWithList(objects));

					// If the table has more than 50,000 rows then we will use BCP.
					if(table.RowCount > 50000)
					{
						fileName = Path.Combine(relativeDataDir, table.Schema + "." + table.Name + ".dat");
						tabFileNames.Add(fileName);
						outputFileName = Path.Combine(OutputDirectory, fileName);
						Console.WriteLine(outputFileName);
						BulkCopyTableData(table, outputFileName);
					}
					else if(table.RowCount > 0)
					{
						fileName = Path.Combine(relativeDataDir, table.Schema + "." + table.Name + ".sql");
						tabFileNames.Add(fileName);
						outputFileName = Path.Combine(OutputDirectory, fileName);
						Console.WriteLine(outputFileName);
						ScriptTableData(table, outputFileName);
					}
				}
			}

			fileNames.AddRange(tabFileNames);
			fileNames.AddRange(kciFileNames);
			fileNames.AddRange(fkyFileNames);
		}

		private void ScriptTableData(Table table, string fileName)
		{
			int maxBatchSize = 1000;
			int divisor = 255;
			int remainder = 7;

			bool hasIdentityColumn = false;
			StringBuilder selectColumnListBuilder = new StringBuilder();
			StringBuilder insertColumnListBuilder = new StringBuilder();
			string columnDelimiter = null;
			IDictionary<int, SqlDataType> readerColumnsSqlDataType = new SortedList<int, SqlDataType>(table.Columns.Count);
			int columnCount = 0;
			int columnOrdinal;
			// We compute the checksum so that we somewhat randomly break the data into batches.
			// The same rows of data (assuming the data in the row hasn't changed) will generate
			// the same checksum so the breaks in the batches will be at the same place each time.
			// Previously we just inserted breaks in batches based on batch size. That resulted in
			// the undesired effect that when one row is added, deleted, or moved, the boundary
			// for all subsequent batches changes, causing numerous other lines of data to be changed.
			// The new process using the checksum is more friendly to source control.
			string checksumColumnList;
			string orderByClause = GetOrderByClauseForTable(table, out checksumColumnList);
			selectColumnListBuilder.AppendFormat("BINARY_CHECKSUM({0}),\r\n\t", checksumColumnList);
			foreach(Column column in table.Columns)
			{
				if(!column.Computed && column.DataType.SqlDataType != SqlDataType.Timestamp)
				{
					if(columnDelimiter != null)
					{
						selectColumnListBuilder.Append(columnDelimiter);
						insertColumnListBuilder.Append(columnDelimiter);
					}
					else
						columnDelimiter = ",\r\n\t";

					string columnName = MakeSqlBracket(column.Name);
					selectColumnListBuilder.Append(columnName);
					insertColumnListBuilder.Append(columnName);

					SqlDataType sqlDataType = column.DataType.SqlDataType;
					columnOrdinal = ++columnCount;
					switch(sqlDataType)
					{
						case SqlDataType.UserDefinedType:
							selectColumnListBuilder.Append(".ToString() AS ");
							selectColumnListBuilder.Append(columnName);
							break;
						case SqlDataType.UserDefinedDataType:
							sqlDataType = GetBaseSqlDataType(column.DataType);
							break;
						case SqlDataType.Variant:
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}, 'BaseType') AS sysname)", columnDelimiter, columnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}, 'Precision') AS int)", columnDelimiter, columnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}, 'Scale') AS int)", columnDelimiter, columnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}, 'Collation') AS sysname)", columnDelimiter, columnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}, 'MaxLength') AS int)", columnDelimiter, columnName);
							columnCount += 5;
							break;
					}

					readerColumnsSqlDataType[columnOrdinal] = sqlDataType;

					if(column.Identity)
						hasIdentityColumn = true;
				}
			}

			string tableNameWithSchema = String.Format("{0}.{1}", MakeSqlBracket(table.Schema), MakeSqlBracket(table.Name));
			string tableNameWithDatabase = String.Format("{0}.{1}", MakeSqlBracket(database.Name), tableNameWithSchema);
			string selectColumnList = selectColumnListBuilder.ToString();
			string insertColumnList = insertColumnListBuilder.ToString();
			string selectClause = String.Format("SELECT\r\n\t{0}", selectColumnList);
			string fromClause = String.Format("FROM {0}", tableNameWithDatabase);
			string selectCommand = String.Format("{0}\r\n{1}\r\n{2}", selectClause, fromClause, orderByClause);

			using(SqlDataReader reader = ExecuteReader(selectCommand))
			{
				using(TextWriter writer = new StreamWriter(fileName, false, this.Encoding))
				{
					if(hasIdentityColumn)
						writer.WriteLine("SET IDENTITY_INSERT {0} ON;\r\nGO", tableNameWithSchema);

					object[] values = new object[reader.FieldCount];
					bool isFirstBatch = true;
					int rowCount = 0;
					while(reader.Read())
					{
						int checksum = reader.GetInt32(0);
						if(checksum % divisor == remainder || rowCount % maxBatchSize == 0)
						{
							// Reset rowCount for the start of a new batch.
							rowCount = 0;
							// If this isn't the first batch then we want to output "GO" to separate the batches.
							if(isFirstBatch)
								isFirstBatch = false;
							else
								writer.WriteLine("GO");
							writer.Write("INSERT {0}\r\n(\r\n\t{1}\r\n)\r\nSELECT\r\n\t", tableNameWithSchema, insertColumnList);
						}
						else
							writer.Write("UNION ALL SELECT\r\n\t");
						reader.GetSqlValues(values);
						columnDelimiter = null;
						foreach(KeyValuePair<int, SqlDataType> readerColumnSqlDataType in readerColumnsSqlDataType)
						{
							int readerOrdinal = readerColumnSqlDataType.Key;
							SqlDataType sqlDataType = readerColumnSqlDataType.Value;
							object sqlValue = values[readerOrdinal];

							if(columnDelimiter != null)
								writer.Write(columnDelimiter);
							else
								columnDelimiter = ",\r\n\t";

							writer.Write(MakeSqlBracket(reader.GetName(readerOrdinal)));
							writer.Write(" = ");
							if(sqlDataType == SqlDataType.Variant)
							{
								SqlString baseType = (SqlString)values[readerOrdinal + 1];
								SqlInt32 precision = (SqlInt32)values[readerOrdinal + 2];
								SqlInt32 scale = (SqlInt32)values[readerOrdinal + 3];
								SqlString collation = (SqlString)values[readerOrdinal + 4];
								SqlInt32 maxLength = (SqlInt32)values[readerOrdinal + 5];
								writer.Write(GetSqlVariantLiteral(sqlValue, baseType, precision, scale, collation, maxLength));
							}
							else
							{
								writer.Write(GetSqlLiteral(sqlValue, sqlDataType));
							}

						}
						writer.WriteLine();
						rowCount++;
					}
					if(hasIdentityColumn)
						writer.WriteLine("GO\r\nSET IDENTITY_INSERT {0} OFF;\r\nGO", tableNameWithSchema);
				}
			}
		}

		private void BulkCopyTableData(Table table, string fileName)
		{
			// bcp [database].[schema].[table] out filename -S servername -T -n
			string bcpArguments = String.Format
			(
				"\"{0}.{1}.{2}\" out \"{3}\" -S {4} -T -n",
				MakeSqlBracket(this.DatabaseName),
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				fileName,
				this.ServerName
			);

			ProcessStartInfo bcpStartInfo = new ProcessStartInfo("bcp.exe", bcpArguments);
			bcpStartInfo.CreateNoWindow = true;
			bcpStartInfo.UseShellExecute = false;
			bcpStartInfo.RedirectStandardError = true;
			bcpStartInfo.RedirectStandardOutput = true;

			Process bcpProcess = new Process();
			bcpProcess.StartInfo = bcpStartInfo;
			bcpProcess.OutputDataReceived += new DataReceivedEventHandler(bcpProcess_OutputDataReceived);
			bcpProcess.ErrorDataReceived += new DataReceivedEventHandler(bcpProcess_ErrorDataReceived);
			bcpProcess.Start();
			bcpProcess.BeginErrorReadLine();
			bcpProcess.BeginOutputReadLine();
			bcpProcess.WaitForExit();
		}

		void bcpProcess_ErrorDataReceived(object sender, DataReceivedEventArgs e)
		{
			Console.Error.WriteLine(e.Data);
		}

		void bcpProcess_OutputDataReceived(object sender, DataReceivedEventArgs e)
		{
			Console.Out.WriteLine(e.Data);
		}

		private SqlDataReader ExecuteReader(string commandText)
		{
			SqlConnection connection = new SqlConnection(server.ConnectionContext.ConnectionString);
			SqlCommand command = new SqlCommand(commandText, connection);
			connection.Open();
			try
			{
				return command.ExecuteReader(CommandBehavior.CloseConnection);
			}
			catch(Exception)
			{
				connection.Close();
				throw;
			}
		}

		/// <summary>
		/// Executes the command on the server.
		/// </summary>
		/// <remarks>
		/// This method takes care of setting, opening, and closing the connection.
		/// </remarks>
		private int ExecuteNonQuery(SqlCommand command)
		{
			SqlConnection connection = new SqlConnection(server.ConnectionContext.ConnectionString);
			command.Connection = connection;
			connection.Open();
			try
			{
				return command.ExecuteNonQuery();
			}
			finally
			{
				connection.Close();
				command.Connection = null;
			}
		}

		private void ScriptUserDefinedFunctionsAndViews()
		{
			UrnCollection schemaBoundUrns = new UrnCollection();
			List<string> nonSchemaBoundFileNames = new List<string>();

			string functionRelativeDir = "Functions";
			string viewRelativeDir = "Views";
			
			ScriptUserDefinedFunctions(functionRelativeDir, schemaBoundUrns, nonSchemaBoundFileNames);
			ScriptViews(viewRelativeDir, schemaBoundUrns, nonSchemaBoundFileNames);

			// If there are any schema bound functions or views then
			// we need to create them in dependency order.
			if(schemaBoundUrns.Count > 0)
			{
				DependencyWalker walker = new DependencyWalker(server);
				DependencyTree tree = walker.DiscoverDependencies(schemaBoundUrns, DependencyType.Parents);
				DependencyCollection dependencies = walker.WalkDependencies(tree);
				foreach(DependencyCollectionNode node in dependencies)
				{
					// Check that the dependency is another schema bound function or view
					if(schemaBoundUrns.Contains(node.Urn))
					{
						string filename;
						switch(node.Urn.Type)
						{
							case "View":
								filename = node.Urn.GetAttribute("Schema") + "." + node.Urn.GetAttribute("Name") + ".viw";
								this.fileNames.Add(Path.Combine(viewRelativeDir, filename));
								break;
							case "UserDefinedFunction":
								filename = node.Urn.GetAttribute("Schema") + "." + node.Urn.GetAttribute("Name") + ".udf";
								this.fileNames.Add(Path.Combine(functionRelativeDir, filename));
								break;
						}
					}
				}
			}

			// Add all non-schema bound functions and view file names after the schema bound ones
			this.fileNames.AddRange(nonSchemaBoundFileNames);
		}

		private void ScriptViewHeaders()
		{
			IList<View> views = new List<View>();
			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
					views.Add(view);
			}

			if(views.Count == 0)
				return;

			string relativeDir = "Views";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Views.sql");
			string outputFileName = Path.Combine(OutputDirectory, fileName);
			Console.WriteLine(outputFileName);
			using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				foreach(View view in views)
				{
					writer.WriteLine(view.TextHeader.Trim());
					writer.Write("SELECT\r\n\t");
					string delimiter = null;
					foreach(Column column in view.Columns)
					{
						if(delimiter == null)
							delimiter = ",\r\n\t";
						else
							writer.Write(delimiter);
						string dataTypeAsString = GetDataTypeAsString(column.DataType);
						if(String.IsNullOrEmpty(column.Collation))
							writer.Write("CAST(NULL AS {0}) AS {1}", dataTypeAsString, MakeSqlBracket(column.Name));
						else
							writer.Write("CAST(NULL AS {0}) COLLATE {1} AS {2}", dataTypeAsString, column.Collation, MakeSqlBracket(column.Name));
					}
					writer.WriteLine(";");
					writer.WriteLine("GO");
				}
			}
			this.fileNames.Add(fileName);
		}

		private void ScriptViews(string relativeDir, UrnCollection schemaBoundUrns, ICollection<string> nonSchemaBoundFileNames)
		{
			IList<View> views = new List<View>();
			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
					views.Add(view);
			}

			if(views.Count == 0)
				return;

			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;

			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.Encoding = Encoding;
			viewOptions.FullTextIndexes = true;
			viewOptions.Indexes = true;
			viewOptions.Permissions = true;
			viewOptions.Statistics = true;
			viewOptions.PrimaryObject = false;

			Scripter viewScripter = new Scripter(server);
			viewScripter.Options = viewOptions;
			viewScripter.PrefetchObjects = false;

			ScriptingOptions triggerOptions = new ScriptingOptions();
			triggerOptions.Encoding = Encoding;
			triggerOptions.PrimaryObject = false;
			triggerOptions.Triggers = true;

			Scripter triggerScripter = new Scripter(server);
			triggerScripter.Options = triggerOptions;
			triggerScripter.PrefetchObjects = false;

			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach(View view in views)
			{
				string fileName = Path.Combine(relativeDir, view.Schema + "." + view.Name + ".viw");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				Console.WriteLine(outputFileName);
				StringCollection script = new StringCollection();
				script.Add("SET ANSI_NULLS " + (view.AnsiNullsStatus ? "ON" : "OFF"));
				script.Add("SET QUOTED_IDENTIFIER " + (view.QuotedIdentifierStatus ? "ON" : "OFF"));
				script.Add(view.ScriptHeader(true) + view.TextBody);
				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					WriteBatches(writer, script);
					objects[0] = view;
					WriteBatches(writer, viewScripter.ScriptWithList(objects));
				}
				if(view.IsSchemaBound)
					schemaBoundUrns.Add(view.Urn);
				else
					nonSchemaBoundFileNames.Add(fileName);

				foreach(Trigger trigger in view.Triggers)
				{
					fileName = Path.Combine(relativeDir, view.Schema + "." + trigger.Name + ".trg"); // is the trigger schema the same as the view?
					outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(outputFileName);
					using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
					{
						objects[0] = trigger;
						triggerScripter.Options = dropOptions;
						WriteBatches(writer, triggerScripter.ScriptWithList(objects));
						triggerScripter.Options = triggerOptions;
						WriteBatches(writer, triggerScripter.ScriptWithList(objects));
					}
					nonSchemaBoundFileNames.Add(fileName);
				}
			}
		}

		private void ScriptServiceBrokerMessageTypes()
		{
			UrnCollection urns = new UrnCollection();
			foreach(MessageType messageType in database.ServiceBroker.MessageTypes)
			{
				// this is a hack to only get user defined message types, not built in ones
				if(messageType.ID >= 65536)
				{
					urns.Add(messageType.Urn);
				}
			}

			if(urns.Count == 0)
				return;

			string relativeDir = @"Service Broker";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Message Types.sql");
			Console.WriteLine(fileName);
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			this.fileNames.Add(fileName);
		}

		private void ScriptServiceBrokerContracts()
		{
			UrnCollection urns = new UrnCollection();
			foreach(ServiceContract contract in database.ServiceBroker.ServiceContracts)
			{
				// this is a hack to only get user defined contracts, not built in ones
				if(contract.ID >= 65536)
				{
					urns.Add(contract.Urn);
				}
			}

			if(urns.Count == 0)
				return;

			string relativeDir = @"Service Broker";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Contracts.sql");
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			Console.WriteLine(options.FileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			this.fileNames.Add(fileName);
		}

		private void ScriptServiceBrokerQueues()
		{
			// Get a list of IDs for Queues that are not system queues
			List<int> nonSystemQueueIds = new List<int>();
			string sqlCommand = String.Format("select object_id from {0}.sys.service_queues WHERE is_ms_shipped = 0 ORDER BY object_id", MakeSqlBracket(database.Name));
			using(SqlDataReader reader = ExecuteReader(sqlCommand))
			{
				while(reader.Read())
				{
					nonSystemQueueIds.Add(reader.GetInt32(0));
				}
			}

			if(nonSystemQueueIds.Count == 0)
				return;

			UrnCollection urns = new UrnCollection();
			foreach(ServiceQueue queue in database.ServiceBroker.Queues)
			{
				// Check if the ID was found in the list of nonSystemQueueIds
				if(nonSystemQueueIds.BinarySearch(queue.ID) >= 0)
				{
					urns.Add(queue.Urn);
				}
			}

			string relativeDir = @"Service Broker";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Queues.sql");
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			Console.WriteLine(options.FileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			this.fileNames.Add(fileName);
		}

		private void ScriptServiceBrokerServices()
		{
			UrnCollection urns = new UrnCollection();
			foreach(BrokerService service in database.ServiceBroker.Services)
			{
				// this is a hack to only get user defined contracts, not built in ones
				if(service.ID >= 65536)
				{
					urns.Add(service.Urn);
				}
			}

			if(urns.Count == 0)
				return;

			string relativeDir = @"Service Broker";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Services.sql");
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			Console.WriteLine(options.FileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.ScriptWithList(urns);
			scripter.PrefetchObjects = false;
			this.fileNames.Add(fileName);
		}

		private void ScriptStoredProcedureHeaders()
		{
			IList<StoredProcedure> sprocs = new List<StoredProcedure>();
			foreach(StoredProcedure sproc in database.StoredProcedures)
			{
				if(!sproc.IsSystemObject && sproc.ImplementationType == ImplementationType.TransactSql)
					sprocs.Add(sproc);
			}

			if(sprocs.Count == 0)
				return;

			string relativeDir = "Stored Procedures";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Stored Procedures.sql");
			string outputFileName = Path.Combine(OutputDirectory, fileName);
			Console.WriteLine(outputFileName);
			using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				foreach(StoredProcedure sproc in sprocs)
				{
					writer.WriteLine(sproc.TextHeader.Trim());
					writer.WriteLine("GO");
				}
			}
			this.fileNames.Add(fileName);
		}

		private void ScriptStoredProcedures()
		{
			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;
			
			ScriptingOptions options = new ScriptingOptions();
			options.Permissions = true;

			IList<StoredProcedure> sprocs = new List<StoredProcedure>();
			foreach(StoredProcedure sproc in database.StoredProcedures)
			{
				if(!sproc.IsSystemObject && sproc.ImplementationType == ImplementationType.TransactSql)
					sprocs.Add(sproc);
			}

			if(sprocs.Count == 0)
				return;

			string relativeDir = "Stored Procedures";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			options.PrimaryObject = false;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.Options.PrimaryObject = false;
			
			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach (StoredProcedure sproc in sprocs)
			{
				string fileName = Path.Combine(relativeDir, sproc.Schema + "." + sproc.Name + ".prc");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				Console.WriteLine(outputFileName);
				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					objects[0] = sproc;
					scripter.Options = dropOptions;
					WriteBatches(writer, scripter.ScriptWithList(objects));
					scripter.Options = options;
					WriteBatches(writer, scripter.ScriptWithList(objects));
				}
				this.fileNames.Add(fileName);
			}
		}

		private void ScriptUserDefinedFunctionHeaders()
		{
			IList<UserDefinedFunction> udfs = new List<UserDefinedFunction>();
			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(!udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
				{
					udfs.Add(udf);
				}
			}

			if(udfs.Count == 0)
				return;

			string relativeDir = "Functions";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Functions.sql");
			string outputFileName = Path.Combine(OutputDirectory, fileName);
			Console.WriteLine(outputFileName);
			using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				foreach(UserDefinedFunction udf in udfs)
				{
					writer.WriteLine(udf.TextHeader.Trim());
					switch(udf.FunctionType)
					{
						case UserDefinedFunctionType.Inline:
							writer.Write("RETURN SELECT\r\n\t");
							string delimiter = null;
							foreach(Column column in udf.Columns)
							{
								if(delimiter == null)
									delimiter = ",\r\n\t";
								else
									writer.Write(delimiter);
								string dataTypeAsString = GetDataTypeAsString(column.DataType);
								if(String.IsNullOrEmpty(column.Collation))
									writer.Write("CAST(NULL AS {0}) AS {1}", dataTypeAsString, MakeSqlBracket(column.Name));
								else
									writer.Write("CAST(NULL AS {0}) COLLATE {1} AS {2}", dataTypeAsString, column.Collation, MakeSqlBracket(column.Name));
							}
							writer.WriteLine(';');
							break;
						case UserDefinedFunctionType.Scalar:
							writer.WriteLine("BEGIN\r\n\tRETURN NULL;\r\nEND;");
							break;
						case UserDefinedFunctionType.Table:
							writer.WriteLine("BEGIN\r\n\tRETURN;\r\nEND;");
							break;
					}
					writer.WriteLine("GO");
				}
			}
			this.fileNames.Add(fileName);
		}

		private void ScriptUserDefinedFunctions(string relativeDir, UrnCollection schemaBoundUrns, ICollection<string> nonSchemaBoundFileNames)
		{
			IList<UserDefinedFunction> udfs = new List<UserDefinedFunction>();
			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(!udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
				{
					udfs.Add(udf);
				}
			}

			if(udfs.Count == 0)
				return;

			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			ScriptingOptions options = new ScriptingOptions();
			options.Encoding = this.Encoding;
			options.Permissions = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

			options.PrimaryObject = false;

			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach(UserDefinedFunction udf in udfs)
			{
				string fileName = Path.Combine(relativeDir, udf.Schema + "." + udf.Name + ".udf");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				Console.WriteLine(outputFileName);
				StringCollection script = new StringCollection();
				script.Add("SET ANSI_NULLS " + (udf.AnsiNullsStatus ? "ON" : "OFF"));
				script.Add("SET QUOTED_IDENTIFIER " + (udf.QuotedIdentifierStatus ? "ON" : "OFF"));
				script.Add(udf.ScriptHeader(true) + udf.TextBody);
				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					WriteBatches(writer, script);
					objects[0] = udf;
					WriteBatches(writer, scripter.ScriptWithList(objects));
				}
				if(udf.IsSchemaBound)
					schemaBoundUrns.Add(udf.Urn);
				else
					nonSchemaBoundFileNames.Add(fileName);
			}
		}

		private void ScriptPartitionFunctions()
		{
			if(database.PartitionFunctions.Count > 0)
			{
				string relativeDir = "Storage";
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);
				
				string fileName = Path.Combine(relativeDir, "PartitionFunctions.sql");
				ScriptingOptions options = new ScriptingOptions();
				options.FileName = Path.Combine(OutputDirectory, fileName);
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.AllowSystemObjects = false;
				
				Console.WriteLine(options.FileName);

				Transfer transfer = new Transfer(database);
				transfer.Options = options;
				transfer.CopyAllObjects = false;
				transfer.CopyAllPartitionFunctions = true;
				transfer.ScriptTransfer();
				this.fileNames.Add(fileName);
			}
		}

		private void ScriptPartitionSchemes()
		{
			if(database.PartitionSchemes.Count > 0)
			{
				string relativeDir = "Storage";
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);

				string fileName = Path.Combine(relativeDir, "PartitionSchemas.sql");
				ScriptingOptions options = new ScriptingOptions();
				options.FileName = Path.Combine(OutputDirectory, fileName);
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.AllowSystemObjects = false;

				Console.WriteLine(options.FileName);

				Transfer transfer = new Transfer(database);
				transfer.Options = options;
				transfer.CopyAllObjects = false;
				transfer.CopyAllPartitionSchemes = true;
				transfer.ScriptTransfer();
				this.fileNames.Add(fileName);
			}
		}
		
		private void ScriptRoles()
		{
			UrnCollection urns = new UrnCollection();

			foreach(DatabaseRole role in database.Roles)
			{
				if(!role.IsFixedRole)
					urns.Add(role.Urn);
			}

			if(urns.Count == 0)
				return;

			string fileName = "Roles.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(this.OutputDirectory, fileName);
			options.ToFileOnly = true;
			options.Encoding = Encoding;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;

			Console.WriteLine(options.FileName);

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);

			// script out role membership (only members that are roles)
			using(TextWriter writer = new StreamWriter(Path.Combine(this.OutputDirectory, fileName), true, Encoding))
			{
				Type databaseRoleType = typeof(DatabaseRole);
				MethodInfo scriptAddToRoleMethod = databaseRoleType.GetMethod("ScriptAddToRole", BindingFlags.Instance | BindingFlags.NonPublic, null, new Type[] { typeof(string), typeof(ScriptingOptions) }, null);
				foreach(DatabaseRole role in database.Roles)
				{
					// We only want to script out members that are roles.
					// For some reason role.EnumRoles() throws an exception here.
					// So use role.EnumMembers() and then check that the member is a role.
					foreach(string member in role.EnumRoles())
					{
						if(database.Roles.Contains(member))
						{
							writer.Write("EXEC ");
							writer.WriteLine(scriptAddToRoleMethod.Invoke(role, new object[]{member, options}));
							writer.WriteLine("GO");
						}
					}
				}
				// Script out database permissions (e.g. GRANT CREATE TABLE TO ...)
				// I haven't found a way to script out just the permissions using public
				// methods so here I use reflection to call the method that scripts permissions.
				StringCollection permissionScript = new StringCollection();
				Type databaseType = typeof(Database);
				
				databaseType.InvokeMember("AddScriptPermission", BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.NonPublic, null, database, new object[] { permissionScript, options });
				foreach(string permission in permissionScript)
				{
					// Write out the permission as long as it isn't a grant/deny connect permission.
					// Connect permissions only apply to users and we don't script out users.
					if(!(permission.StartsWith("GRANT CONNECT TO") || permission.StartsWith("DENY CONNECT TO")))
					{
						writer.WriteLine(permission);
						writer.WriteLine("GO");
					}
				}
			}
			this.fileNames.Add(fileName);
		}


		private void ScriptSchemas()
		{
			UrnCollection urns = new UrnCollection();
			foreach(Schema schema in database.Schemas)
			{
				// Get user schemas - note the check on ID was taken from the
				// Transfer.GetObjectList() method using reflection.
				if(schema.ID > 4 && (schema.ID < 0x4000 || schema.ID >= 0x4010))
					urns.Add(schema.Urn);
			}

			if(urns.Count == 0)
				return;

			string fileName = "Schemas.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			options.ToFileOnly = true;
			options.Encoding = Encoding;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;

			Console.WriteLine(options.FileName);

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			
			this.fileNames.Add(fileName);
		}

		private void ScriptSynonyms()
		{
			if(database.Synonyms.Count > 0)
			{
				string fileName = "Synonyms.sql";
				ScriptingOptions options = new ScriptingOptions();
				options.FileName = Path.Combine(OutputDirectory, fileName);
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.Permissions = true;
				options.AllowSystemObjects = false;
				options.IncludeIfNotExists = true;

				Console.WriteLine(options.FileName);

				Synonym[] synonyms = new Synonym[database.Synonyms.Count];
				database.Synonyms.CopyTo(synonyms, 0);

				Scripter scripter = new Scripter(server);
				scripter.Options = options;
				scripter.PrefetchObjects = false;
				scripter.ScriptWithList(synonyms);
				
				this.fileNames.Add(fileName);
			}
		}

		private void ScriptUserDefinedDataTypes()
		{
			if(database.UserDefinedDataTypes.Count > 0)
			{
				string fileName = "Types.sql";
				ScriptingOptions options = new ScriptingOptions();
				options.FileName = Path.Combine(this.OutputDirectory, fileName);
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.Permissions = true;
				options.AllowSystemObjects = false;
				options.IncludeIfNotExists = true;

				Console.WriteLine(options.FileName);

				Transfer transfer = new Transfer(database);
				transfer.Options = options;
				transfer.CopyAllObjects = false;
				transfer.CopyAllUserDefinedDataTypes = true;
				transfer.ScriptTransfer();
				this.fileNames.Add(fileName);
			}
		}

		private void ScriptXmlSchemaCollections()
		{
			List<XmlSchemaCollection> xmlSchemaCollections = new List<XmlSchemaCollection>();
			foreach(XmlSchemaCollection xmlSchemaCollection in database.XmlSchemaCollections)
			{
				// this is a hack to only get user defined xml schema collections, not built in ones
				if(xmlSchemaCollection.ID >= 65536)
				{
					xmlSchemaCollections.Add(xmlSchemaCollection);
				}
			}

			if(xmlSchemaCollections.Count == 0)
				return;

			string relativeDir = "Xml Schema Collections";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			StringBuilder sb = new StringBuilder();

			ScriptingOptions options = new ScriptingOptions();
			options.PrimaryObject = false;
			options.Permissions = true;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			
			XmlWriterSettings writerSettings = new XmlWriterSettings();
			writerSettings.ConformanceLevel = ConformanceLevel.Fragment;
			writerSettings.NewLineOnAttributes = true;
			writerSettings.Encoding = this.Encoding;
			writerSettings.Indent = true;
			writerSettings.IndentChars = "\t";

			XmlReaderSettings readerSettings = new XmlReaderSettings();
			readerSettings.ConformanceLevel = ConformanceLevel.Fragment;
			SqlSmoObject[] objects = new SqlSmoObject[1];
			
			foreach(XmlSchemaCollection xmlSchemaCollection in xmlSchemaCollections)
			{
				// this is a hack to only get user defined xml schema collections, not built in ones
				if(xmlSchemaCollection.ID >= 65536)
				{
					string fileName = Path.Combine(relativeDir, xmlSchemaCollection.Schema + "." + xmlSchemaCollection.Name + ".sql");
					string outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(outputFileName);
					using(TextReader textReader = new StringReader(xmlSchemaCollection.Text))
					{
						using(XmlReader xmlReader = XmlReader.Create(textReader, readerSettings))
						{
							sb.Length = 0;
							using(StringWriter stringWriter = new StringWriter(sb))
							{
								using(XmlWriter xmlWriter = XmlWriter.Create(stringWriter, writerSettings))
								{
									while(xmlReader.Read())
									{
										xmlWriter.WriteNode(xmlReader, false);
									}
								}
							}
						}
					}
					sb.Replace("'", "''");
					using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
					{
						writer.WriteLine("CREATE XML SCHEMA COLLECTION {0}.{1} AS N'", MakeSqlBracket(xmlSchemaCollection.Schema), MakeSqlBracket(xmlSchemaCollection.Name));
						writer.WriteLine(sb.ToString());
						writer.WriteLine("'");
						writer.WriteLine("GO");
						objects[0] = xmlSchemaCollection;
						// script out permissions
						StringCollection script = scripter.ScriptWithList(objects);
						// Remove the CREATE XML SCHEMA statement as we've already written it above (with formatted XML).
						// This appears to be a bug with SQL SMO that ignores the PrimaryObject scripting option.
						script.RemoveAt(0); 
						WriteBatches(writer, script);
					}
					this.fileNames.Add(fileName);
				}
			}
		}

		public static string MakeSqlBracket(string name)
		{
			return "[" + EscapeChar(name, ']') + "]";
		}

		public static string EscapeChar(string s, char c)
		{
			return s.Replace(new string(c, 1), new string(c, 2));
		}

		/// <summary>
		/// Gets the name of the full text catalog to use in the create script.
		/// </summary>
		/// <remarks>
		/// If the name of the full text catalog starts with the name of the database then
		/// the name of the full text catalog will be scripted out so that the old database
		/// name is replaced with the new database name.
		/// </remarks>
		public string GetFullTextCatalogScriptName(string fullTextCatalogName)
		{
			if(fullTextCatalogName != null && fullTextCatalogName.StartsWith(this.database.Name))
				return FileScripter.DBName + fullTextCatalogName.Substring(database.Name.Length);
			else
				return fullTextCatalogName;
		}

		public static string ByteArrayToHexLiteral(byte[] a)
		{
			if(a == null)
			{
				return null;
			}
			StringBuilder builder = new StringBuilder(a.Length * 2);
			builder.Append("0x");
			foreach(byte b in a)
			{
				builder.Append(b.ToString("X02", System.Globalization.CultureInfo.InvariantCulture));
			}
			return builder.ToString();
		}

		public SqlDataType GetBaseSqlDataType(DataType dataType)
		{
			if(dataType.SqlDataType != SqlDataType.UserDefinedDataType)
				return dataType.SqlDataType;

			UserDefinedDataType uddt = database.UserDefinedDataTypes[dataType.Name, dataType.Schema];
			return GetBaseSqlDataType(uddt);
		}

		public SqlDataType GetBaseSqlDataType(UserDefinedDataType uddt)
		{
			return (SqlDataType)Enum.Parse(typeof(SqlDataType), uddt.SystemType, true);
		}

		public DataType GetDataType(SqlDataType sqlDataType, int precision, int scale, int maxLength)
		{
			switch(sqlDataType)
			{
				case SqlDataType.Binary:
				case SqlDataType.Char:
				case SqlDataType.NChar:
				case SqlDataType.NVarChar:
				case SqlDataType.VarBinary:
				case SqlDataType.VarChar:
				case SqlDataType.NVarCharMax:
				case SqlDataType.VarBinaryMax:
				case SqlDataType.VarCharMax:
					return new DataType(sqlDataType, maxLength);
				case SqlDataType.Decimal:
				case SqlDataType.Numeric:
					return new DataType(sqlDataType, precision, scale);
				default:
					return new DataType(sqlDataType);
			}
		}

		public DataType GetBaseDataType(DataType dataType)
		{
			if(dataType.SqlDataType != SqlDataType.UserDefinedDataType)
				return dataType;

			UserDefinedDataType uddt = database.UserDefinedDataTypes[dataType.Name, dataType.Schema];
			SqlDataType baseSqlDataType = GetBaseSqlDataType(uddt);
			DataType baseDataType = GetDataType(baseSqlDataType, uddt.NumericPrecision, uddt.NumericScale, uddt.MaxLength);
			return baseDataType;
		}

		public string GetDataTypeAsString(DataType dataType)
		{
			StringBuilder sb = new StringBuilder();
			switch(dataType.SqlDataType)
			{
				case SqlDataType.Binary:
				case SqlDataType.Char:
				case SqlDataType.NChar:
				case SqlDataType.NVarChar:
				case SqlDataType.VarBinary:
				case SqlDataType.VarChar:
					sb.Append(MakeSqlBracket(dataType.Name));
					sb.Append('(');
					sb.Append(dataType.MaximumLength);
					sb.Append(')');
					break;
				case SqlDataType.NVarCharMax:
				case SqlDataType.VarBinaryMax:
				case SqlDataType.VarCharMax:
					sb.Append(MakeSqlBracket(dataType.Name));
					sb.Append("(max)");
					break;
				case SqlDataType.Decimal:
				case SqlDataType.Numeric:
					sb.Append(MakeSqlBracket(dataType.Name));
					sb.AppendFormat("({0},{1})", dataType.NumericPrecision, dataType.NumericScale);
					break;
				case SqlDataType.UserDefinedDataType:
					// For a user defined type, get the base data type as string
					DataType baseDataType = GetBaseDataType(dataType);
					return GetDataTypeAsString(baseDataType);
				case SqlDataType.Xml:
					sb.Append("[xml]");
					if(!String.IsNullOrEmpty(dataType.Name))
						sb.AppendFormat("({0} {1})", dataType.XmlDocumentConstraint, dataType.Name);
					break;
				default:
					sb.Append(MakeSqlBracket(dataType.Name));
					break;
			}
			return sb.ToString();
		}

		private string GetOrderByClauseForTable(Table table, out string checksumColumnList)
		{
			Index bestIndex = null;
			int bestRank = int.MaxValue;
			// Find the best index to use for the order by clause.
			// In order of priority we want to use:
			// 1) the primary key,
			// 2) the clustered index,
			// 3) a unique key,
			// or 4) a unique index
			// There could be multiple of unique keys/indexes so we go with
			// the one that comes first alphabetically. 
			foreach(Index index in table.Indexes)
			{
				int currentRank = int.MaxValue;
				if(index.IndexKeyType == IndexKeyType.DriPrimaryKey)
					currentRank = 1;
				else if(index.IsClustered)
					currentRank = 2;
				else if(index.IndexKeyType == IndexKeyType.DriUniqueKey)
					currentRank = 3;
				else if(index.IsUnique)
					currentRank = 4;
				else if(!index.IsXmlIndex)
					currentRank = 5;
				if(currentRank < bestRank ||
					(
						currentRank == bestRank
						&& String.Compare(index.Name, bestIndex.Name, false, CultureInfo.InvariantCulture) < 0
					)
				)
				{
					bestRank = currentRank;
					bestIndex = index;
				}
			}

			StringBuilder orderBy = new StringBuilder();
			orderBy.Append("ORDER BY ");

			if(bestIndex == null)
			{
				// If we didn't find an index then we sort by all non-computed columns
				string columnDelimiter = null;
				foreach(Column column in table.Columns)
				{
					if(!column.Computed)
					{
						if(columnDelimiter != null)
							orderBy.Append(columnDelimiter);
						else
							columnDelimiter = ", ";
						orderBy.Append(MakeSqlBracket(column.Name));
					}
				}
				// Checksum over all columns
				checksumColumnList = "*";
			}
			else
			{
				StringBuilder checksumColumnBuilder = new StringBuilder();
				string columnDelimiter = null;
				foreach(IndexedColumn indexColumn in bestIndex.IndexedColumns)
				{
					if(!indexColumn.IsIncluded)
					{
						if(columnDelimiter != null)
						{
							orderBy.Append(columnDelimiter);
							checksumColumnBuilder.Append(columnDelimiter);
						}
						else
							columnDelimiter = ", ";
						string bracketedColumnName = MakeSqlBracket(indexColumn.Name);
						orderBy.Append(bracketedColumnName);
						checksumColumnBuilder.Append(bracketedColumnName);
						if(indexColumn.Descending)
							orderBy.Append(" DESC");
					}
				}
				checksumColumnList = checksumColumnBuilder.ToString();
				// If the index isn't unique then add all the rest of the non-computed columns
				if(!bestIndex.IsUnique)
				{
					foreach(Column column in table.Columns)
					{
						if(!column.Computed
							&& bestIndex.IndexedColumns.Contains(column.Name)
							&& !bestIndex.IndexedColumns[column.Name].IsIncluded)
						{
							orderBy.Append(columnDelimiter);
							orderBy.Append(MakeSqlBracket(column.Name));
						}
					}
				}
			}
			return orderBy.ToString();
		}

		private string GetSqlLiteral(object sqlValue, SqlDataType sqlDataType)
		{
			if(DBNull.Value == sqlValue || (sqlValue is INullable && ((INullable)sqlValue).IsNull))
				return "NULL";
			switch(sqlDataType)
			{
				case SqlDataType.BigInt:
				case SqlDataType.Decimal:
				case SqlDataType.Int:
				case SqlDataType.Money:
				case SqlDataType.Numeric:
				case SqlDataType.SmallInt:
				case SqlDataType.SmallMoney:
				case SqlDataType.TinyInt:
					return sqlValue.ToString();
				case SqlDataType.Binary:
				case SqlDataType.Image:
				case SqlDataType.Timestamp:
				case SqlDataType.VarBinary:
				case SqlDataType.VarBinaryMax:
					return ByteArrayToHexLiteral(((SqlBinary)sqlValue).Value);
				case SqlDataType.Bit:
					return ((SqlBoolean)sqlValue).Value ? "1" : "0";
				case SqlDataType.Char:
				case SqlDataType.Text:
				case SqlDataType.UniqueIdentifier:
				case SqlDataType.VarChar:
				case SqlDataType.VarCharMax:
					return "'" + EscapeChar(sqlValue.ToString(), '\'') + "'";
				case SqlDataType.DateTime:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm:ss.fff", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.NChar:
				case SqlDataType.NText:
				case SqlDataType.NVarChar:
				case SqlDataType.NVarCharMax:
				case SqlDataType.SysName:
				case SqlDataType.UserDefinedType:
					return "N'" + EscapeChar(sqlValue.ToString(), '\'') + "'";
				case SqlDataType.Float:
					return ((SqlDouble)sqlValue).Value.ToString("r");
				case SqlDataType.Real:
					return ((SqlSingle)sqlValue).Value.ToString("r");
				case SqlDataType.SmallDateTime:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.Xml:
					XmlWriterSettings settings = new XmlWriterSettings();
					settings.OmitXmlDeclaration = true;
					settings.Indent = true;
					settings.IndentChars = "\t";
					settings.NewLineOnAttributes = true;
					using(XmlReader xmlReader = ((SqlXml)sqlValue).CreateReader())
					{
						using(StringWriter stringWriter = new StringWriter())
						{
							using(XmlWriter xmlWriter = XmlWriter.Create(stringWriter, settings))
							{
								while(xmlReader.Read())
								{
									xmlWriter.WriteNode(xmlReader, false);
								}
							}
							return "N'" + EscapeChar(stringWriter.ToString(), '\'') + "'";
						}
					}
					
				default:
					throw new ApplicationException("Unsupported type :" + sqlDataType.ToString());
			}
		}

		private string GetSqlVariantLiteral(object sqlValue, SqlString baseType, SqlInt32 precision, SqlInt32 scale, SqlString collation, SqlInt32 maxLength)
		{
			if(DBNull.Value == sqlValue || (sqlValue is INullable && ((INullable)sqlValue).IsNull))
				return "NULL";

			SqlDataType sqlDataType = (SqlDataType)Enum.Parse(typeof(SqlDataType), baseType.Value, true);
			// The SQL_VARIANT_PROPERTY MaxLength is returned in bytes.
			// For nchar and nvarchar we need to halve this to get the max length used when specifying the type.
			// Note that I also included ntext and nvarcharmax in the case statement even though they can't be used
			// in a sql_varaint type.
			int adjustedMaxLength;
			switch(sqlDataType)
			{
				case SqlDataType.NChar:
				case SqlDataType.NText:
				case SqlDataType.NVarChar:
				case SqlDataType.NVarCharMax:
					adjustedMaxLength = maxLength.Value / 2;
					break;
				default:
					adjustedMaxLength = maxLength.Value;
					break;
			}
			DataType dataType = GetDataType(sqlDataType, precision.Value, scale.Value, adjustedMaxLength);
			string literal = "CAST(CAST(" + GetSqlLiteral(sqlValue, sqlDataType) + " AS " + GetDataTypeAsString(dataType) + ")";
			if(!collation.IsNull)
				literal += " COLLATE " + collation.Value;
			literal += " AS [sql_variant])";
			return literal;
		}

		/// <summary>
		/// Writes out batches of SQL statements.
		/// </summary>
		/// <param name="writer">TextWriter to write to.</param>
		/// <param name="batches">Collection of SQL statements.</param>
		/// <remarks>
		/// Each string in the collection of SQL statements is trimmed before being written.
		/// A 'GO' statement is added after each one.
		/// </remarks>
		private void WriteBatches(TextWriter writer, StringCollection script)
		{
			foreach(string batch in script)
			{
				writer.WriteLine(batch.Trim());
				writer.WriteLine("GO");
			}
		}

		private void WriteBatches(string fileName, StringCollection script)
		{
			WriteBatches(fileName, false, script);
		}

		private void WriteBatches(string fileName, bool append, StringCollection script)
		{
			using(TextWriter writer = new StreamWriter(fileName, append, this.Encoding))
			{
				WriteBatches(writer, script);
			}
		}

	}
}
