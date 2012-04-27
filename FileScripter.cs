using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
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
		private List<ScriptFile> scriptFiles = new List<ScriptFile>();
		private HashSet<string> fileSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		private bool ignoreFileSetModified = false;
		private SortedSet<string> ignoreFileSet = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
		private Server server;
		private Database database;
		private Char allExtraFilesResponseChar = '\0';
		private Char allEmptyDirectoriesResponseChar = '\0';

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

		private SqlServerVersion targetServerVersion = SqlServerVersion.Version110;
		public SqlServerVersion TargetServerVersion
		{
			get { return targetServerVersion; }
			set { targetServerVersion = value; }
		}

		private void AddIgnoreFiles()
		{
			string ignoreFileName = Path.Combine(OutputDirectory, "IgnoreFiles.txt");
			AddScriptFile("IgnoreFiles.txt", null);
			if(File.Exists(ignoreFileName))
			{
				foreach(string line in File.ReadAllLines(ignoreFileName))
				{
					string ignoreLine = line.Trim();
					ignoreFileSet.Add(ignoreLine);
					if(ignoreLine.Contains("*"))
					{
						string directory = OutputDirectory;
						string filePattern = ignoreLine;
						string[] parts = ignoreLine.Split('\\', '/');
						if(parts.Length > 0)
						{
							string[] dirs = parts.Take(parts.Length - 1).ToArray();
							directory = Path.Combine(OutputDirectory, Path.Combine(dirs));
							filePattern = parts.Last();
						}
						if(Directory.Exists(directory))
						{
							foreach(string fileName in Directory.EnumerateFiles(directory, filePattern))
							{
								AddScriptFile(fileName, null);
							}
						}
					}
					else
						AddScriptFile(ignoreLine, null);
				}
			}
		}

		
		private void SaveIgnoreFiles()
		{
			if(ignoreFileSetModified)
			{
				string ignoreFileName = Path.Combine(OutputDirectory, "IgnoreFiles.txt");
				File.WriteAllLines(ignoreFileName, this.ignoreFileSet);
			}
		}
		
		private void AddScriptFile(ScriptFile scriptFile)
		{
			if(scriptFile == null)
				throw new ArgumentNullException("scriptFile");
			this.scriptFiles.Add(scriptFile);
			if(scriptFile.FileName != null)
				this.fileSet.Add(scriptFile.FileName);
		}

		private void AddScriptFile(string fileName)
		{
			AddScriptFile(new ScriptFile(fileName));
		}

		private void AddScriptFile(string fileName, string command)
		{
			AddScriptFile(new ScriptFile(fileName, command));
		}

		private void AddDataFile(string fileName, string schema, string table)
		{
			string command = String.Format("!!bcp \"[{0}].[{1}].[{2}]\" in \"{3}\" -S $(SQLCMDSERVER) -T -N -k -E", FileScripter.DBName, schema, table, fileName);
			AddScriptFile(fileName, command);
		}

		private void AddScriptFileRange(IEnumerable<string> fileNames)
		{
			foreach(string fileName in fileNames)
				AddScriptFile(fileName);
		}

		private void AddScriptPermission(Database db, StringCollection script, ScriptingOptions options)
		{
			object preferences = GetScriptingPreferences(options);
			typeof(Database).InvokeMember("AddScriptPermission", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null, db, new object[] { script, preferences });
		}

		private string ScriptAddToRole(DatabaseRole role, string memberOfRole, ScriptingOptions options)
		{
			object preferences = GetScriptingPreferences(options);
			return (string)typeof(DatabaseRole).InvokeMember("ScriptAddToRole", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null, role, new object[] { memberOfRole, preferences });
		}

		private void ScriptCreate(FileGroup fileGroup, StringCollection script, ScriptingOptions options)
		{
			object preferences = GetScriptingPreferences(options);
			typeof(FileGroup).InvokeMember("ScriptCreate", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null, fileGroup, new object[] { script, preferences });
		}

		private object GetScriptingPreferences(ScriptingOptions options)
		{
			return typeof(ScriptingOptions).InvokeMember("GetScriptingPreferences", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null, options, null);			
		}
		
		/// <summary>
		/// Gets the SqlServerVersion for the specified CompatibilityLevel.
		/// </summary>
		private SqlServerVersion GetSqlServerVersion(CompatibilityLevel compatibilityLevel)
		{
			switch(compatibilityLevel)
			{
				case CompatibilityLevel.Version100:
					return SqlServerVersion.Version100;
				// If the compatibility level is 90 (2005) then we target version 90
				case CompatibilityLevel.Version90:
					return SqlServerVersion.Version90;
				// If the compatibility level is 80 (2000) then we target version 80
				// If the compatibility level is 80 (2000) or less then we target version 80.
				case CompatibilityLevel.Version80:
				case CompatibilityLevel.Version70:
				case CompatibilityLevel.Version65:
				case CompatibilityLevel.Version60:
					return SqlServerVersion.Version80;
				// Default target version 110 (2012)
				default:
					return SqlServerVersion.Version110;
			}
		}

		public void Script()
		{
			if(this.OutputDirectory.Length > 0 && !Directory.Exists(this.OutputDirectory))
				Directory.CreateDirectory(this.OutputDirectory);

			scriptFiles.Clear();
			ignoreFileSet.Clear();
			ignoreFileSetModified = false;
			allEmptyDirectoriesResponseChar = '\0';
			allExtraFilesResponseChar = '\0';
			
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
			
			// We get the database object by name then create a new server object and
			// get the database object by id. This is so that the database object can
			// be initialized with the Name property having correct character case.
			// Even when database names are not case sensitive, the Urn object is.
			// In particular, when we compare Urns in the ScriptAssemblies method
			// we need to database name to have the correct case.
			database = server.Databases[databaseName];
			// Get the database ID.
			int databaseID = database.ID;
			// Create a new server connection because the old server connection has
			// cached the database object with the name we used to access it.
			server = new Server(connection);
			// Get the database object by ID.
			database = server.Databases.ItemById(databaseID);
			// Set the target server version based on the compatibility level.
			targetServerVersion = GetSqlServerVersion(database.CompatibilityLevel);

			PrefetchObjects();

			ScriptDatabase();
			ScriptFileGroups();
			ScriptFullTextCatalogs();
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
			ScriptUserDefinedTableTypes();
			ScriptSequences();
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
				foreach(ScriptFile file in this.scriptFiles.Where(f => f.Command != null))
				{
					writer.WriteLine("PRINT '{0}'", file.FileName);
					writer.WriteLine("GO");
					writer.WriteLine(file.Command);
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

			AddScriptFile("CreateDatabaseObjects.sql", null);

			DirectoryInfo outputDirectoryInfo;
			if(OutputDirectory != "")
				outputDirectoryInfo = new DirectoryInfo(OutputDirectory);
			else
				outputDirectoryInfo = new DirectoryInfo(".");

			// Prompt the user for what to do with extra files.
			// When objects are deleted from the database ensure that the user
			// wants to delete the corresponding files. There may also be other
			// files in the directory that are not scripted files.

			AddIgnoreFiles();
			PromptExtraFiles(outputDirectoryInfo, "");
			SaveIgnoreFiles();
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
			prefetchOptions.TargetServerVersion = this.TargetServerVersion;

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
			// Prefetching PartitionFunctions didn't help with SMO 2008.
			// Actually it wouldn't script out whether the range was LEFT or RIGHT (PartitionFunction.RangeType).
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
			// This fetches all non-collection properties of all assemblies at once
			// so that when we script out the assemblies it doesn't have to query
			// these properties for each assembly.
			server.SetDefaultInitFields(typeof(SqlAssembly), true);
			database.Assemblies.Refresh();
			// In addition, this fetches the permissions for all assemblies at once.
			database.PrefetchObjects(typeof(SqlAssembly), prefetchOptions);
			// When scripting assemblies it still queries for assembly files and dependencies
			// for each assembly.
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

		private void PromptExtraFiles(DirectoryInfo dirInfo, string relativeDir)
		{
			string relativeName;
			foreach(FileInfo fileInfo in dirInfo.GetFiles())
			{
				// Skip over the file if it isn't a .sql or .dat file.
				if(!String.Equals(fileInfo.Extension, ".sql", StringComparison.OrdinalIgnoreCase) && !String.Equals(fileInfo.Extension, ".dat", StringComparison.OrdinalIgnoreCase))
					continue;
				relativeName = Path.Combine(relativeDir, fileInfo.Name);
				if(!fileSet.Contains(relativeName))
				{
					Console.WriteLine("Extra file: {0}", relativeName);
					char responseChar = this.allExtraFilesResponseChar;
					if(allExtraFilesResponseChar == '\0')
					{
						Console.WriteLine("Keep, delete, or ignore this file? For all extra files? (press k, d, i, or a)");
						ConsoleKeyInfo key = Console.ReadKey(true);
						responseChar = key.KeyChar;
						if(responseChar == 'a')
						{
							Console.WriteLine("Keep, delete, or ignore all remaining extra files? (press k, d, i)");
							key = Console.ReadKey(true);
							responseChar = key.KeyChar;
							// Only accept the response char if it is k, d, or i.
							// Other characters are ignored, which is the same as keeping this file.
							if(responseChar == 'k' || responseChar == 'd' || responseChar == 'i')
								allExtraFilesResponseChar = responseChar;
						}
					}
					if(responseChar == 'd')
					{
						try
						{
							fileInfo.Delete();
							Console.WriteLine("Deleted file.");
						}
						catch(Exception ex)
						{
							Console.WriteLine("Delete failed. {0}: {1}", ex.GetType().Name, ex.Message);
						}
					}
					else if(responseChar == 'i')
					{
						ignoreFileSetModified = true;
						ignoreFileSet.Add(relativeName);
					}
				}
			}
			foreach(DirectoryInfo subDirInfo in dirInfo.GetDirectories())
			{
				if(subDirInfo.Attributes.HasFlag(FileAttributes.Hidden))
					continue;
				string relativeSubDir = Path.Combine(relativeDir, subDirInfo.Name);
				PromptExtraFiles(subDirInfo, relativeSubDir);
				// If the directory is empty and is not ignored, prompt about deleting it.
				if(!subDirInfo.EnumerateFileSystemInfos().Any() && !ignoreFileSet.Contains(relativeSubDir))
				{
					Console.WriteLine("Empty directory: {0}", relativeSubDir);
					char responseChar = this.allEmptyDirectoriesResponseChar;
					if(allEmptyDirectoriesResponseChar == '\0')
					{
						Console.WriteLine("Keep, delete, or ignore this directory? For all empty directories? (press k, d, i, or a)");
						ConsoleKeyInfo key = Console.ReadKey(true);
						responseChar = key.KeyChar;
						if(responseChar == 'a')
						{
							Console.WriteLine("Keep, delete, or ignore all remaining empty directories? (press k, d, i)");
							key = Console.ReadKey(true);
							responseChar = key.KeyChar;
							// Only accept the response char if it is k, d, or i.
							// Other characters are ignored, which is the same as keeping this directory.
							if(responseChar == 'k' || responseChar == 'd' || responseChar == 'i')
								allEmptyDirectoriesResponseChar = responseChar;
						}
					}
					if(responseChar == 'd')
					{
						try
						{
							subDirInfo.Delete();
							Console.WriteLine("Deleted directory.");
						}
						catch(Exception ex)
						{
							Console.WriteLine("Delete failed. {0}: {1}", ex.GetType().Name, ex.Message);
						}
					}
					else if(responseChar == 'i')
					{
						ignoreFileSetModified = true;
						ignoreFileSet.Add(relativeSubDir);
					}
				}

			}
		}

		private void ScriptAssemblies()
		{
			// Check to make sure that the database contains at least one assembly 
			// that is not a system object.
			bool hasNonSystemAssembly = false;
			foreach(SqlAssembly assembly in database.Assemblies)
			{
				if(!assembly.IsSystemObject)
				{
					hasNonSystemAssembly = true;
					break;
				}
			}
			
			if(!hasNonSystemAssembly)
				return;

			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.AppendToFile = false;
			options.Encoding = this.Encoding;
			options.Permissions = true;
			options.TargetServerVersion = this.TargetServerVersion;
			
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

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
					if(assembly.IsSystemObject)
						continue;

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
						AddScriptFile(Path.Combine(relativeDir, fileName));
					}
				}
			}
			finally
			{
				server.ConnectionContext.SqlExecutionModes = previousModes;
			}
		}

		private void ScriptDatabase()
		{
			string fileName = "Database.sql";
			string outputFileName = Path.Combine(this.OutputDirectory, fileName);
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = outputFileName;
			options.ToFileOnly = true;
			options.AppendToFile = true;
			options.Encoding = this.Encoding;
			options.TargetServerVersion = this.TargetServerVersion;
			
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;
			options.NoFileGroup = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			Console.WriteLine(outputFileName);

			// Add our own check to see if the database already exists so we can optionally drop it.
			// The scripter will add its own check if the database does not exist so it will create it.
			using(TextWriter writer = new StreamWriter(outputFileName,false, Encoding))
			{
				writer.WriteLine("IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}')", FileScripter.DBName);
				writer.WriteLine("BEGIN");
				writer.WriteLine("\tPRINT 'Note: the database ''{0}'' already exits. All open transactions will be rolled back and existing connections closed.';", FileScripter.DBName);
				writer.WriteLine("\tALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", FileScripter.DBName);
				writer.WriteLine("\tIF '$(DROPDB)' IN ('true', '1')", FileScripter.DBName);
				writer.WriteLine("\tBEGIN");
				writer.WriteLine("\t\tPRINT 'Dropping database ''{0}''';", FileScripter.DBName);
				writer.WriteLine("\t\tDROP DATABASE [{0}];", FileScripter.DBName);
				writer.WriteLine("\tEND");
				writer.WriteLine("END");
				writer.WriteLine("GO");
			}

			// Set the value of the internal ScriptName property used when scripting the database.
			// This the same property that the Transfer object sets to create the destination database.
			// The alternative (which I had previously used) was to go through the script and replace
			// the old database name with the new database name.
			typeof(Database).InvokeMember("ScriptName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.SetProperty, null, database, new string[] { FileScripter.DBName }, null);

			// Script out the database options.
			scripter.ScriptWithList(new SqlSmoObject[] { database });

			// Now that the datase exists, add USE statement so that all the following scripts use the database.
			using(TextWriter writer = new StreamWriter(outputFileName, true, Encoding))
			{
				writer.WriteLine("USE [{0}]", FileScripter.DBName);
				writer.WriteLine("GO");
			}

			AddScriptFile(fileName);
		}

		private void ScriptFileGroups()
		{
			List<FileGroup> fileGroups = new List<FileGroup>();
			foreach(FileGroup fileGroup in database.FileGroups)
			{
				if(!String.Equals(fileGroup.Name, "PRIMARY"))
					fileGroups.Add(fileGroup);
			}
			if(fileGroups.Count > 0)
			{
				string relativeDir = "Storage";
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);

				string fileName = Path.Combine(relativeDir, "FileGroups.sql");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				
				ScriptingOptions options = new ScriptingOptions();
				options.Encoding = Encoding;
				options.AllowSystemObjects = false;
				options.TargetServerVersion = this.TargetServerVersion;

				Console.WriteLine(outputFileName);

				// Script out the file groups (but not the files because we don't want the file paths in source control).
				using(TextWriter writer = new StreamWriter(outputFileName, false, Encoding))
				{
					StringCollection script = new StringCollection();
					foreach(FileGroup fileGroup in fileGroups)
					{
						string stringLiteralName = fileGroup.Name.Replace("'", "''");
						writer.WriteLine("IF NOT EXISTS(SELECT * FROM sys.filegroups WHERE name = N'{0}')", stringLiteralName);
						writer.WriteLine("BEGIN");

						fileGroup.Initialize(true);
						script.Clear();
						ScriptCreate(fileGroup, script, options);
						// There should only be one batch in the script collection
						// and we don't want a GO statement afterwards (so don't use WriteBatches method).
						writer.WriteLine(script[0]);

						writer.WriteLine("PRINT 'Warning: File group {0} was created without any data files. A file must be added to this file group before data can be inserted into it.'", MakeSqlBracket(stringLiteralName));
						writer.WriteLine("END");
						writer.WriteLine("GO");
					}
				}

				AddScriptFile(fileName);
			}
		}

		private void ScriptFullTextCatalogs()
		{
			if(database.FullTextCatalogs.Count > 0)
			{
				string relativeDir = "Storage";
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);

				string fileName = Path.Combine(relativeDir, "FullTextCatalogs.sql");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				ScriptingOptions options = new ScriptingOptions();
				options.FileName = outputFileName;
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.AllowSystemObjects = false;
				options.TargetServerVersion = this.TargetServerVersion;

				Console.WriteLine(fileName);

				Scripter scripter = new Scripter(server);
				scripter.Options = options;
				FullTextCatalog[] fullTextCatalogs = new FullTextCatalog[database.FullTextCatalogs.Count];
				database.FullTextCatalogs.CopyTo(fullTextCatalogs, 0);
				scripter.Script(fullTextCatalogs);

				AddScriptFile(fileName);
			}
		}

		private void ScriptTables()
		{
			ScriptingOptions tableOptions = new ScriptingOptions();
			tableOptions.Encoding = this.Encoding;
			tableOptions.Permissions = true;
			tableOptions.TargetServerVersion = this.TargetServerVersion;
			tableOptions.Statistics = false;

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
			kciOptions.Statistics = true;
			kciOptions.Triggers = true;
			kciOptions.XmlIndexes = true;
			kciOptions.TargetServerVersion = this.TargetServerVersion;

			Scripter kciScripter = new Scripter(server);
			kciScripter.Options = kciOptions;
			kciScripter.PrefetchObjects = false;

			ScriptingOptions fkyOptions = new ScriptingOptions();
			fkyOptions.Encoding = this.Encoding;
			fkyOptions.DriForeignKeys = true;
			fkyOptions.DriIncludeSystemNames = true;
			fkyOptions.PrimaryObject = false;
			fkyOptions.SchemaQualifyForeignKeysReferences = true;
			fkyOptions.TargetServerVersion = this.TargetServerVersion;
			fkyOptions.Statistics = false;

			Scripter fkyScripter = new Scripter(server);
			fkyScripter.Options = fkyOptions;
			fkyScripter.PrefetchObjects = false;

			List<string> kciFileNames = new List<string>();
			List<string> fkyFileNames = new List<string>();

			SqlSmoObject[] objects = new SqlSmoObject[1];

			foreach (Table table in database.Tables)
			{
				if (!table.IsSystemObject)
				{
					objects[0] = table;

					string relativeDir = Path.Combine("Schemas", table.Schema, "Tables");
					string dir = Path.Combine(OutputDirectory, relativeDir);
					if(!Directory.Exists(dir))
						Directory.CreateDirectory(dir);

					string fileName = Path.Combine(relativeDir, table.Name + ".sql");
					AddScriptFile(fileName);
					string outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, tableScripter.ScriptWithList(objects));

					string kciFileName = Path.ChangeExtension(fileName, ".kci.sql");
					kciFileNames.Add(kciFileName);
					outputFileName = Path.Combine(OutputDirectory, kciFileName);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, kciScripter.ScriptWithList(objects));

					string fkyFileName = Path.ChangeExtension(fileName, ".fky.sql");
					fkyFileNames.Add(fkyFileName);
					outputFileName = Path.Combine(OutputDirectory, fkyFileName);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, fkyScripter.ScriptWithList(objects));

					if(table.RowCount > 0)
					{
						string relativeDataDir = Path.Combine("Schemas", table.Schema, "Data");
						string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
						if(!Directory.Exists(dataDir))
							Directory.CreateDirectory(dataDir);
					
						// If the table has more than 50,000 rows then we will use BCP.
						if(table.RowCount > 50000)
						{
							fileName = Path.Combine(relativeDataDir, table.Name + ".dat");
							AddDataFile(fileName, table.Schema, table.Name);
							outputFileName = Path.Combine(OutputDirectory, fileName);
							Console.WriteLine(outputFileName);
							BulkCopyTableData(table, outputFileName);
						}
						else
						{
							fileName = Path.Combine(relativeDataDir, table.Name + ".sql");
							AddScriptFile(fileName);
							outputFileName = Path.Combine(OutputDirectory, fileName);
							Console.WriteLine(outputFileName);
							ScriptTableData(table, outputFileName);
						}
					}
				}
			}
			AddScriptFileRange(kciFileNames);
			AddScriptFileRange(fkyFileNames);
		}

		private void ScriptTableData(Table table, string fileName)
		{
			int maxBatchSize = 1000;
			int divisor = 511;
			int remainder = 510;

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
			selectColumnListBuilder.AppendFormat("ABS(BINARY_CHECKSUM({0})),\r\n\t", checksumColumnList);
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
			// bcp [database].[schema].[table] out filename -S servername -T -N
			string bcpArguments = String.Format
			(
				"\"{0}.{1}.{2}\" out \"{3}\" -S {4} -T -N",
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

			foreach(Schema schema in GetSchemas())
			{
				ScriptUserDefinedFunctions(schema.Name, schemaBoundUrns, nonSchemaBoundFileNames);
				ScriptViews(schema.Name, schemaBoundUrns, nonSchemaBoundFileNames);
			}

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
								filename = String.Format(@"Schemas\{0}\Views\{1}.sql", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
								AddScriptFile(filename);
								break;
							case "UserDefinedFunction":
								filename = String.Format(@"Schemas\{0}\Functions\{1}.sql", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
								AddScriptFile(filename);
								break;
						}
					}
				}
			}

			// Add all non-schema bound functions and view file names after the schema bound ones
			AddScriptFileRange(nonSchemaBoundFileNames);
		}

		private void ScriptViewHeaders()
		{
			foreach(Schema schema in GetSchemas())
			{
				ScriptViewHeaders(schema.Name);
			}
		}

		private void ScriptViewHeaders(string schema)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			IList<View> views = new List<View>();
			foreach(View view in database.Views)
			{
				if(view.Schema == schema && !view.IsSystemObject)
					views.Add(view);
			}

			if(views.Count == 0)
				return;

			string relativeDir = Path.Combine("Schemas", schema, "Views");
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
			AddScriptFile(fileName);
		}

		private void ScriptViews(string schema, UrnCollection schemaBoundUrns, ICollection<string> nonSchemaBoundFileNames)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			IList<View> views = new List<View>();
			foreach(View view in database.Views)
			{
				if(view.Schema == schema && !view.IsSystemObject)
					views.Add(view);
			}

			if(views.Count == 0)
				return;

			string relativeDir = Path.Combine("Schemas", schema, "Views");
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;
			dropOptions.TargetServerVersion = targetServerVersion;

			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.Encoding = Encoding;
			viewOptions.FullTextIndexes = true;
			viewOptions.Indexes = true;
			viewOptions.Permissions = true;
			viewOptions.Statistics = true;
			viewOptions.TargetServerVersion = targetServerVersion;

			Scripter viewScripter = new Scripter(server);
			viewScripter.Options = viewOptions;
			viewScripter.PrefetchObjects = false;

			ScriptingOptions triggerOptions = new ScriptingOptions();
			triggerOptions.Encoding = Encoding;
			triggerOptions.PrimaryObject = false;
			triggerOptions.Triggers = true;
			triggerOptions.TargetServerVersion = targetServerVersion;

			Scripter triggerScripter = new Scripter(server);
			triggerScripter.Options = triggerOptions;
			triggerScripter.PrefetchObjects = false;

			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach(View view in views)
			{
				string fileName = Path.Combine(relativeDir, view.Name + ".sql");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				Console.WriteLine(outputFileName);
				
				objects[0] = view;
				StringCollection script = viewScripter.ScriptWithList(objects);
				// The 3rd bath in the script is the CREATE VIEW statement.
				// Replace it with an ALTER VIEW statement.
				script[2] = view.ScriptHeader(true) + view.TextBody;

				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					WriteBatches(writer, script);

					foreach(Trigger trigger in view.Triggers)
					{
						objects[0] = trigger;
						triggerScripter.Options = dropOptions;
						WriteBatches(writer, triggerScripter.ScriptWithList(objects));
						triggerScripter.Options = triggerOptions;
						WriteBatches(writer, triggerScripter.ScriptWithList(objects));
					}
				}
				if(view.IsSchemaBound)
					schemaBoundUrns.Add(view.Urn);
				else
					nonSchemaBoundFileNames.Add(fileName);

				
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
			options.TargetServerVersion = targetServerVersion;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			AddScriptFile(fileName);
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
			options.TargetServerVersion = targetServerVersion;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			AddScriptFile(fileName);
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
			options.TargetServerVersion = this.TargetServerVersion;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			AddScriptFile(fileName);
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
			options.TargetServerVersion = this.TargetServerVersion;
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.ScriptWithList(urns);
			scripter.PrefetchObjects = false;
			AddScriptFile(fileName);
		}

		private void ScriptStoredProcedureHeaders()
		{
			foreach(Schema schema in GetSchemas())
			{
				ScriptStoredProcedureHeaders(schema.Name);
			}
		}

		private void ScriptStoredProcedureHeaders(string schema)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			IList<StoredProcedure> sprocs = new List<StoredProcedure>();
			foreach(StoredProcedure sproc in database.StoredProcedures)
			{
				if(sproc.Schema == schema && !sproc.IsSystemObject && sproc.ImplementationType == ImplementationType.TransactSql)
					sprocs.Add(sproc);
			}

			if(sprocs.Count == 0)
				return;

			string relativeDir = Path.Combine("Schemas", schema, "Stored Procedures");
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
			AddScriptFile(fileName);
		}

		private void ScriptStoredProcedures()
		{
			foreach(Schema schema in GetSchemas())
			{
				ScriptStoredProcedures(schema.Name);
			}
		}

		private void ScriptStoredProcedures(string schema)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;
			dropOptions.TargetServerVersion = this.TargetServerVersion;
			
			ScriptingOptions options = new ScriptingOptions();
			options.Permissions = true;
			options.TargetServerVersion = this.TargetServerVersion;

			IList<StoredProcedure> sprocs = new List<StoredProcedure>();
			foreach(StoredProcedure sproc in database.StoredProcedures)
			{
				if(sproc.Schema == schema && !sproc.IsSystemObject && sproc.ImplementationType == ImplementationType.TransactSql)
					sprocs.Add(sproc);
			}

			if(sprocs.Count == 0)
				return;

			string relativeDir = Path.Combine("Schemas", schema, "Stored Procedures");
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
				string fileName = Path.Combine(relativeDir, sproc.Name + ".sql");
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
				AddScriptFile(fileName);
			}
		}

		private void ScriptUserDefinedFunctionHeaders()
		{
			foreach(Schema schema in GetSchemas())
			{
				ScriptUserDefinedFunctionHeaders(schema.Name);
			}
		}

		private void ScriptUserDefinedFunctionHeaders(string schema)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			IList<UserDefinedFunction> udfs = new List<UserDefinedFunction>();
			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(udf.Schema == schema && !udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
				{
					udfs.Add(udf);
				}
			}

			if(udfs.Count == 0)
				return;

			string relativeDir = Path.Combine("Schemas", schema, "Functions");
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
			AddScriptFile(fileName);
		}

		private void ScriptUserDefinedFunctions(string schema, UrnCollection schemaBoundUrns, ICollection<string> nonSchemaBoundFileNames)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			IList<UserDefinedFunction> udfs = new List<UserDefinedFunction>();
			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(udf.Schema == schema && !udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
				{
					udfs.Add(udf);
				}
			}

			if(udfs.Count == 0)
				return;

			string relativeDir = Path.Combine("Schemas", schema, "Functions");
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			ScriptingOptions options = new ScriptingOptions();
			options.Encoding = this.Encoding;
			options.Permissions = true;
			options.TargetServerVersion = this.TargetServerVersion;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach(UserDefinedFunction udf in udfs)
			{
				string fileName = Path.Combine(relativeDir, udf.Name + ".sql");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				Console.WriteLine(outputFileName);
				objects[0] = udf;
				StringCollection script = scripter.ScriptWithList(objects);
				// The 3rd bath in the script is the CREATE FUNCTION statement.
				// Replace it with an ALTER FUNCTION statement.
				script[2] = udf.ScriptHeader(true) + udf.TextBody;
				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					WriteBatches(writer, script);
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
				options.TargetServerVersion = this.TargetServerVersion;
				
				Console.WriteLine(options.FileName);

				Scripter scripter = new Scripter(server);
				scripter.Options = options;
				PartitionFunction[] partitionFunctions = new PartitionFunction[database.PartitionFunctions.Count];
				database.PartitionFunctions.CopyTo(partitionFunctions, 0);
				scripter.Script(partitionFunctions);

				AddScriptFile(fileName);
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
				options.TargetServerVersion = this.TargetServerVersion;

				Console.WriteLine(options.FileName);

				Transfer transfer = new Transfer(database);
				transfer.Options = options;
				transfer.CopyAllObjects = false;
				transfer.CopyAllPartitionSchemes = true;
				transfer.ScriptTransfer();
				AddScriptFile(fileName);
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
			options.TargetServerVersion = this.TargetServerVersion;

			Console.WriteLine(options.FileName);

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);

			// script out role membership (only members that are roles)
			using(TextWriter writer = new StreamWriter(Path.Combine(this.OutputDirectory, fileName), true, Encoding))
			{
				foreach(DatabaseRole role in database.Roles)
				{
					// Get the list of roles that the current role is a member of.
					// Sort them for consistency in source control.
					var memberOfRoles = role.EnumRoles().OfType<string>().OrderBy(r => r);
					foreach(string memberOfRole in memberOfRoles)
					{
						if(database.Roles.Contains(memberOfRole))
						{
							string addToRoleScript = ScriptAddToRole(role, memberOfRole, options);
							// In SQL 2008 R2 SMO the ScriptAddToRole method includes EXEC. But SQL 2008 before R2 did not.
							// This change will work whether or not SMO has been updated to R2 on the user's machine.
							if(!addToRoleScript.StartsWith("EXEC", StringComparison.InvariantCultureIgnoreCase))
								writer.Write("EXEC ");
							writer.WriteLine(addToRoleScript);
							writer.WriteLine("GO");
						}
					}
				}
				// Script out database permissions (e.g. GRANT CREATE TABLE TO ...)
				// I haven't found a way to script out just the permissions using public
				// methods so here I use reflection to call the method that scripts permissions.
				StringCollection permissionScript = new StringCollection();
				AddScriptPermission(database, permissionScript, options);
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
			AddScriptFile(fileName);
		}


		private void ScriptSchemas()
		{
			UrnCollection urns = new UrnCollection();
			urns.AddRange(GetSchemas(false).Select(s => s.Urn));

			if(urns.Count == 0)
				return;

			Directory.CreateDirectory(Path.Combine(OutputDirectory, "Schemas"));
			string fileName = @"Schemas\Schemas.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			options.ToFileOnly = true;
			options.Encoding = Encoding;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;
			options.ScriptOwner = true;
			options.TargetServerVersion = this.TargetServerVersion;

			Console.WriteLine(options.FileName);

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			scripter.ScriptWithList(urns);
			
			AddScriptFile(fileName);
		}

		private void ScriptSchemaObjects(ICollection collection, string fileName)
		{
			foreach(var group in collection.Cast<ScriptSchemaObjectBase>().GroupBy(s => s.Schema))
			{
				string schema = group.Key;
				string relativeDir = Path.Combine("Schemas", schema);
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);

				string relativeFilePath = Path.Combine(relativeDir, fileName);
				Console.WriteLine(relativeFilePath);

				ScriptingOptions options = new ScriptingOptions();
				options.FileName = Path.Combine(OutputDirectory, relativeFilePath);
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.AllowSystemObjects = false;
				options.IncludeIfNotExists = true;
				options.Permissions = true;
				options.TargetServerVersion = this.TargetServerVersion;

				Scripter scripter = new Scripter(server);
				scripter.Options = options;
				scripter.PrefetchObjects = false;
				ScriptSchemaObjectBase[] objects = group.ToArray();
				scripter.ScriptWithList(objects);

				AddScriptFile(relativeFilePath);
			}
		}
		
		private void ScriptSequences()
		{
			if(TargetServerVersion >= SqlServerVersion.Version110)
				ScriptSchemaObjects(database.Sequences, "Sequences.sql");
		}

		private void ScriptSynonyms()
		{
			ScriptSchemaObjects(database.Synonyms, "Synonyms.sql");
		}

		private void ScriptUserDefinedDataTypes()
		{
			ScriptSchemaObjects(database.UserDefinedDataTypes, "Types.sql");
		}

		private void ScriptUserDefinedTableTypes()
		{
			// We may want to consider scripting each table type as a separate file,
			// but for now that are all scripted into one file (all the types within the same schema).
			if(TargetServerVersion >= SqlServerVersion.Version100)
				ScriptSchemaObjects(database.UserDefinedTableTypes, "TableTypes.sql");
		}

		private void ScriptXmlSchemaCollections()
		{
			var groups = database.XmlSchemaCollections.Cast<XmlSchemaCollection>()
				.Where(x => x.ID >= 65536) // Only get user defined xml schema collections, not built in ones.
				.GroupBy(x => x.Schema);

			ScriptingOptions options = new ScriptingOptions();
			options.PrimaryObject = false;
			options.Permissions = true;
			options.TargetServerVersion = this.TargetServerVersion;
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
			
			foreach(var group in groups)
			{
				string schema = group.Key;
				string relativeDir = Path.Combine("Schemas", schema, "Xml Schema Collections");
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);

				StringBuilder sb = new StringBuilder();
				foreach(XmlSchemaCollection xmlSchemaCollection in group)
				{
					string fileName = Path.Combine(relativeDir, xmlSchemaCollection.Name + ".sql");
					string outputFileName = Path.Combine(OutputDirectory, fileName);
					Console.WriteLine(fileName);

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
					AddScriptFile(fileName);
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
					IndexedColumnCollection indexedColumns = bestIndex.IndexedColumns;
					foreach(Column column in table.Columns)
					{
						if(!column.Computed	&& (!indexedColumns.Contains(column.Name) || indexedColumns[column.Name].IsIncluded))
						{
							orderBy.Append(columnDelimiter);
							orderBy.Append(MakeSqlBracket(column.Name));
						}
					}
				}
			}
			return orderBy.ToString();
		}

		private IEnumerable<Schema> GetSchemas(bool includeDbo = true)
		{
			return database.Schemas
				.Cast<Schema>()
				.Where(s => !s.IsSystemObject || (includeDbo && s.Name == "dbo"));
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
				case SqlDataType.Date:
					return "'" + ((DateTime)sqlValue).ToString("yyyy-MM-dd", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.DateTime:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm:ss.fff", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.DateTime2:
					return "'" + ((DateTime)sqlValue).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.DateTimeOffset:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF K", DateTimeFormatInfo.InvariantInfo) + "'";
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
				case SqlDataType.Time:
					return "'" + ((TimeSpan)sqlValue).ToString("g", DateTimeFormatInfo.InvariantInfo) + "'";
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
				//case SqlDataType.Geography:
				//case SqlDataType.Geometry:
				//case SqlDataType.HierarchyId:	
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
