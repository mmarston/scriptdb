//   Copyright 2013 Mercent Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
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
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Broker;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Mercent.SqlServer.Management.IO;

namespace Mercent.SqlServer.Management
{
	public class FileScripter
	{
		private List<ScriptFile> scriptFiles = new List<ScriptFile>();
		private HashSet<string> fileSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		/// <summary>
		/// Set of unique extended property statements (EXEC sys.sp_addextendedproperty).
		/// </summary>
		/// <remarks>
		/// Due to the way we script out tables in multiple files, the extended properties on tables and columns
		/// would be included in all 3 table files (the primary .sql, .kci.sql and .fky.sql).
		/// To avoid these duplicates, we skip writing out EXEC sys.sp_addextendedproperty statement
		/// if it already exists in this set.
		/// </remarks>
		private HashSet<string> extendedPropertySet = new HashSet<string>();
		private bool ignoreFileSetModified = false;
		private SortedSet<string> ignoreFileSet = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
		private Server server;
		private Database database;
		private Char allExtraFilesResponseChar = '\0';
		private Char allEmptyDirectoriesResponseChar = '\0';
		private ScriptUtility utility;

		private static readonly string DBName = "$(DBNAME)";
		private static readonly HashSet<string> knownExtensions = new HashSet<string>(new [] { ".sql", ".cab", ".dat", ".fmt", ".udat", ".txt" }, StringComparer.OrdinalIgnoreCase);
		
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

		private int maxUncompressedFileSize = 100 * 1024 * 1024; // 100 MB;
		/// <summary>
		/// Gets or sets the max uncompressed file size (in bytes).
		/// </summary>
		/// <remarks>
		/// The default is 100 MB.
		/// </remarks>
		public int MaxUncompressedFileSize
		{
			get { return maxUncompressedFileSize; }
			set { maxUncompressedFileSize = value; }
		}

		/// <summary>
		/// Force the scripter to continue even when errors or data loss may occur.
		/// </summary>
		/// <remarks>
		/// Set this to <c>true</c> or <c>false</c> when running using automation tools
		/// that don't have an user interaction. This avoids prompting the user.
		/// This setting affects any errors where the user would normally be given the option
		/// to continue or abort (a "prompted" error). It also suppresses prompting the user for what to do with
		/// extra files. When set to <c>true</c> the scripter will continue on prompted
		/// errors and will delete extra files. When set to <c>false</c> the scripter
		/// will abort on prompted errors and keep extra files.
		/// </remarks>
		public bool? ForceContinue { get; set; }

		public bool TargetDataTools { get; set; }

		private SqlServerVersion targetServerVersion = SqlServerVersion.Version110;
		public SqlServerVersion TargetServerVersion
		{
			get { return targetServerVersion; }
			set { targetServerVersion = value; }
		}

		public event EventHandler<MessageReceivedEventArgs> ErrorMessageReceived;
		public event EventHandler<MessageReceivedEventArgs> ProgressMessageReceived;
		public event EventHandler<MessageReceivedEventArgs> OutputMessageReceived;

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
								// Get the path to the fileName relative to the OutputDirectory.
								string relativePath = fileName.Substring(OutputDirectory.Length).TrimStart('/', '\\');
								AddScriptFile(relativePath, null);
							}
						}
					}
					else
						AddScriptFile(ignoreLine, null);
				}
			}
			// Ignore the bin and obj directories of an SSDT project.
			// It doesn't hurt to always ignore these, so no need
			// to wrap this in a check for if(TargetDataTools)...
			AddScriptFile("bin", null);
			AddScriptFile("obj", null);
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

		private void AddUnicodeNativeDataFile(string dataFile, string schema, string table)
		{
			dataFile = CheckCompressFile(dataFile);
			string command = String.Format("!!bcp \"[{0}].[{1}].[{2}]\" in \"{3}\" -S $(SQLCMDSERVER) -T -N -k -E", FileScripter.DBName, schema, table, dataFile);
			AddScriptFile(dataFile, command);
		}

		private void AddUtf16DataFile(string dataFile, string schema, string table)
		{
			string formatFile = Path.ChangeExtension(dataFile, ".fmt");
			dataFile = CheckCompressFile(dataFile);
			string command = String.Format("!!bcp \"[{0}].[{1}].[{2}]\" in \"{3}\" -S $(SQLCMDSERVER) -T -k -E -f \"{4}\"", FileScripter.DBName, schema, table, dataFile, formatFile);
			AddScriptFile(dataFile, command);
			AddScriptFile(formatFile, null);
		}

		private void AddCodePageDataFile(string dataFile, string schema, string table, string codePage)
		{
			string formatFile = Path.ChangeExtension(dataFile, ".fmt");
			dataFile = CheckCompressFile(dataFile);
			string command = String.Format("!!bcp \"[{0}].[{1}].[{2}]\" in \"{3}\" -S $(SQLCMDSERVER) -T -C {4} -k -E -f \"{5}\"", FileScripter.DBName, schema, table, dataFile, codePage, formatFile);
			AddScriptFile(dataFile, command);
			AddScriptFile(formatFile, null);
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

		private string CheckCompressFile(string dataFile)
		{
			string fullDataFile = Path.Combine(this.outputDirectory, dataFile);
			FileInfo fileInfo = new FileInfo(fullDataFile);

			// If the file doesn't exist or the size is less than the max uncompressed size
			// then just return the original file.
			if(!fileInfo.Exists || fileInfo.Length < MaxUncompressedFileSize)
				return dataFile;

			// Compress the file.
			string compressedFile = Path.ChangeExtension(dataFile, ".cab");
			string fullCompressedFile = Path.Combine(this.outputDirectory, compressedFile);
			CompressFile(fullDataFile, fullCompressedFile);

			// Delete the original file (we don't want it to be included as part of the source control).
			fileInfo.Delete();

			// Similarly, when uncompressing the data file we want to use a different extension
			// that souce control can be configured to ignore.
			string tempDataFile = Path.ChangeExtension(dataFile, ".tmp");

			// Add a command to the SQL script to uncompress the file.
			string uncompressCommand = String.Format("!!expand \"{0}\" \"{1}\"", compressedFile, tempDataFile);
			AddScriptFile(compressedFile, uncompressCommand);
			return tempDataFile;
		}

		private void CompressFile(string source, string destination)
		{
			// Before compressing the file we set a bogus LastModified date.
			// This is an attempt to consistently generate the same .cab file (byte for byte)
			// as long as the uncompressed data file is the same.
			DateTime defaultTime = new DateTime(2000, 01, 01);
			File.SetCreationTime(source, defaultTime);
			File.SetLastWriteTime(source, defaultTime);

			string arguments = String.Format("\"{0}\" \"{1}\"", source, destination);
			Process process = new Process
			{
				StartInfo = new ProcessStartInfo
				{
					FileName = "makecab.exe",
					Arguments = arguments,
					UseShellExecute = false,
				}
			};
			process.Start();
			process.WaitForExit();
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

		public void Script()
		{
			VerifyProperties();

			if(this.OutputDirectory.Length > 0 && !Directory.Exists(this.OutputDirectory))
				Directory.CreateDirectory(this.OutputDirectory);

			scriptFiles.Clear();
			ignoreFileSet.Clear();
			extendedPropertySet.Clear();
			ignoreFileSetModified = false;
			if(!ForceContinue.HasValue)
			{
				allEmptyDirectoriesResponseChar = '\0';
				allExtraFilesResponseChar = '\0';
			}
			else if(ForceContinue.Value)
			{
				allEmptyDirectoriesResponseChar = 'd';
				allExtraFilesResponseChar = 'd';
			}
			else
			{
				allEmptyDirectoriesResponseChar = 'k';
				allExtraFilesResponseChar = 'k';
			}

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
			// we need the database name to have the correct case.
			database = server.Databases[databaseName];
			if(database == null)
				throw new KeyNotFoundException("The database '" + databaseName + "' was not found.");

			// Get the database ID.
			int databaseID = database.ID;
			// Create a new server connection because the old server connection has
			// cached the database object with the name we used to access it.
			server = new Server(connection);
			// Get the database object by ID.
			database = server.Databases.ItemById(databaseID);
			// The ScriptUtility instance methods need the context of the database.
			// Create a new ScriptUtility instance for this database.
			utility = new ScriptUtility(database);
			// Set the target server version based on the compatibility level.
			targetServerVersion = ScriptUtility.GetSqlServerVersion(database.CompatibilityLevel);

			PrefetchObjects();

			if(!TargetDataTools)
			{
				ScriptDatabase();
			}
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
			if(!TargetDataTools)
			{
				ScriptUserDefinedFunctionHeaders();
				ScriptViewHeaders();
				ScriptStoredProcedureHeaders();
			}
			ScriptTables();
			ScriptServiceBrokerQueues();
			ScriptServiceBrokerServices();
			ScriptUserDefinedFunctionsAndViews();
			ScriptStoredProcedures();

			// Here is a list of database objects that currently are not being scripted:
			//database.AsymmetricKeys;
			//database.Certificates;
			//database.ExtendedStoredProcedures;
			//database.Rules;
			//database.SymmetricKeys;
			//database.Triggers;
			//database.Users;

			if(!TargetDataTools)
			{
				using(StreamWriter writer = new StreamWriter(Path.Combine(OutputDirectory, "CreateDatabaseObjects.sql"), false, Encoding))
				{
					writer.WriteLine(":on error exit");
					foreach(ScriptFile file in this.scriptFiles.Where(f => f.Command != null))
					{
						writer.WriteLine("PRINT '{0}'", file.FileName);
						writer.WriteLine("GO");
						writer.WriteLine(file.Command);
					}
					// If the database is readonly then set it readonly at the very end.
					if(database.ReadOnly)
					{
						writer.WriteLine("PRINT 'Setting database to read-only mode.'");
						writer.WriteLine("GO");
						writer.WriteLine("ALTER DATABASE [{0}] SET READ_ONLY;", FileScripter.DBName);
					}
				}

				AddScriptFile("CreateDatabaseObjects.sql", null);
			}

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

		private void VerifyProperties()
		{
			if(String.IsNullOrWhiteSpace(this.ServerName))
				throw new InvalidOperationException("Set the ServerName property before calling the Script() method.");
			if(String.IsNullOrWhiteSpace(this.DatabaseName))
				throw new InvalidOperationException("Set the DatabaseName property before calling the Script() method.");
		}

		private void PrefetchObjects()
		{
			OnProgressMessageReceived("Prefetching objects.");
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
			OnProgressMessageReceived(null);

			PrefetchRoles();
			OnProgressMessageReceived(null);
			PrefetchFullTextCatalogs();
			OnProgressMessageReceived(null);
			PrefetchStoredProcedures(prefetchOptions);
			OnProgressMessageReceived(null);
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
			OnProgressMessageReceived(null);
			PrefetchUserDefinedFunctions(prefetchOptions);
			OnProgressMessageReceived(null);
			// Prefetching PartitionFunctions didn't help with SMO 2008.
			// Actually it wouldn't script out whether the range was LEFT or RIGHT (PartitionFunction.RangeType).
			database.PrefetchObjects(typeof(PartitionScheme), prefetchOptions);
			OnProgressMessageReceived(null);
			database.PrefetchObjects(typeof(UserDefinedAggregate), prefetchOptions);
			OnProgressMessageReceived(null);
			PrefetchTables(prefetchOptions);
			OnProgressMessageReceived(null);
			PrefetchSynonyms();
			OnProgressMessageReceived(null);
			PrefetchServiceBrokerMessageTypes();
			OnProgressMessageReceived(null);
			PrefetchServiceBrokerContracts();
			OnProgressMessageReceived(null);
			PrefetchServiceBrokerQueues();
			OnProgressMessageReceived(null);
			PrefetchServiceBrokerServices();
			OnProgressMessageReceived(null);
			PrefetchAssemblies(prefetchOptions);
			OnProgressMessageReceived(null);
			database.PrefetchObjects(typeof(XmlSchemaCollection), prefetchOptions);
			OnProgressMessageReceived(null);
			OnProgressMessageReceived(String.Empty);
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
				// Skip over the file if it isn't a known extension (.sql, .dat, .udat, .fmt).
				if(!knownExtensions.Contains(fileInfo.Extension))
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
				string relativeSubDir = Path.Combine(relativeDir, subDirInfo.Name);
				// Skip the directory if it is hidden or in the file set (because it was in the ignore list).
				if(subDirInfo.Attributes.HasFlag(FileAttributes.Hidden) || fileSet.Contains(relativeSubDir))
					continue;
				// If the directory is not empty then recursively call PromptExtraFiles...
				if(subDirInfo.EnumerateFileSystemInfos().Any())
					PromptExtraFiles(subDirInfo, relativeSubDir);
				else
				{
					// If the directory is empty, prompt about deleting it.
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
			options.ExtendedProperties = true;
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
					// Skip system objects.
					if(assembly.IsSystemObject)
						continue;

					string fileName = Path.Combine(relativeDir, assembly.Name + ".sql");
					string outputPath = Path.Combine(OutputDirectory, fileName);
					objects[0] = assembly;

					OnProgressMessageReceived(fileName);
					StringCollection script = script = scripter.ScriptWithList(objects);

					// SSDT projects should reference assemblies by using an assembly or project reference,
					// so we don't want to include the CREATE ASSEMBLY statement.
					// I tried setting ScriptingOptions.PrimaryObject = false, but that didn't prevent
					// the Scripter from scripting the CREATE ASSEMBLY statement.
					// So we remove the first batch in the script.
					// This may be the only batch in the script, but the script may also include permissions
					// on the assembly.
					if(TargetDataTools)
						script.RemoveAt(0);
					WriteBatches(outputPath, script);

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
							Urn[] children = new Urn[sortedChildren.Count];
							sortedChildren.Values.CopyTo(children, 0);
							script = scripter.ScriptWithList(children);
							WriteBatches(outputPath, true, script);
						}
					}
					else if(!TargetDataTools)
					{
						// The create script doesn't include VISIBILITY (this appears
						// to be a bug in SQL SMO) here we reset it and call Alter()
						// to generate an alter statement.

						// We don't include this for SSDT projects because visibility is set as
						// a property of the assembly reference.

						assembly.IsVisible = true;
						assembly.IsVisible = false;
						server.ConnectionContext.CapturedSql.Clear();
						assembly.Alter();
						StringCollection batches = server.ConnectionContext.CapturedSql.Text;
						// Remove the first string, which is a USE statement to set the database context
						batches.RemoveAt(0);
						WriteBatches(outputPath, true, batches);
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
			options.ExtendedProperties = true;
			options.TargetServerVersion = this.TargetServerVersion;
			
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;
			options.NoFileGroup = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			OnProgressMessageReceived(fileName);

			// Set the value of the internal ScriptName property used when scripting the database.
			// This the same property that the Transfer object sets to create the destination database.
			// The alternative (which I had previously used) was to go through the script and replace
			// the old database name with the new database name.
			typeof(Database).InvokeMember("ScriptName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.SetProperty, null, database, new string[] { FileScripter.DBName }, null);


			// Add our own check to see if the database already exists so we can optionally drop it.
			// The scripter will add its own check if the database does not exist so it will create it.
			// We then SET READ_WRITE in case the database was read-only
			// (this must be done before we can set to SINGLE_USER).
			// Then we SET SINGLE_USER.
			// Both of these are SET WITH ROLLBACK IMMEDIATE to close any existing connections.
			using(TextWriter writer = new StreamWriter(outputFileName, false, Encoding))
			{
				writer.WriteLine("IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}')", FileScripter.DBName);
				writer.WriteLine("BEGIN");
				writer.WriteLine("\tPRINT 'Note: the database ''{0}'' already exits. All open transactions will be rolled back and existing connections closed.';", FileScripter.DBName);
				writer.WriteLine("\tALTER DATABASE [{0}] SET READ_WRITE WITH ROLLBACK IMMEDIATE;", FileScripter.DBName);
				writer.WriteLine("\tALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", FileScripter.DBName);
				writer.WriteLine("\tIF '$(DROPDB)' IN ('true', '1')", FileScripter.DBName);
				writer.WriteLine("\tBEGIN");
				writer.WriteLine("\t\tPRINT 'Dropping database ''{0}''';", FileScripter.DBName);
				writer.WriteLine("\t\tDROP DATABASE [{0}];", FileScripter.DBName);
				writer.WriteLine("\tEND");
				writer.WriteLine("END");
				writer.WriteLine("GO");
			
				// Script out the database options.
				StringCollection script = scripter.ScriptWithList(new SqlSmoObject[] { database });

				// If the database is read-only then remove the SET READ_ONLY statement from the script.
				// We need to remove the SET READ_ONLY statement otherwise we couldn't create any of the database objects!
				if(database.ReadOnly)
				{
					Regex readOnlyRegex = new Regex(@"\bSET\s+READ_ONLY\b", RegexOptions.IgnoreCase);
					// Note that we loop through the statements from the end because we expect
					// the SET READ_ONLY statement to be the last one.
					for(int i = script.Count - 1; i >= 0; i--)
					{
						if(readOnlyRegex.IsMatch(script[i]))
						{
							script.RemoveAt(i);
							break;
						}
					}
				}

				// Add the database options to the file.
				WriteBatches(writer, script);
				
				// Now that the datase exists, add USE statement so that all the following scripts use the database.
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
				options.ExtendedProperties = true;
				options.TargetServerVersion = this.TargetServerVersion;

				OnProgressMessageReceived(fileName);

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
				options.ExtendedProperties = true;
				options.TargetServerVersion = this.TargetServerVersion;

				OnProgressMessageReceived(fileName);

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
			tableOptions.ExtendedProperties = true;
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
			kciOptions.ExtendedProperties = true;
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
			fkyOptions.ExtendedProperties = true;
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
				if (!table.IsSystemObject || table.IsSysDiagramsWithData())
				{
					objects[0] = table;

					string relativeDir = Path.Combine("Schemas", table.Schema, "Tables");
					string dir = Path.Combine(OutputDirectory, relativeDir);
					if(!Directory.Exists(dir))
						Directory.CreateDirectory(dir);

					if(TargetDataTools && table.HasClusteredIndex)
					{
						// SSDT doesn't like for the file group or partition scheme
						// to be specified on the table and on the clustured index.
						tableOptions.NoFileGroup = true;
						tableOptions.NoTablePartitioningSchemes = true;
						tableOptions.ScriptDataCompression = false;
					}
					else
					{
						tableOptions.NoFileGroup = false;
						tableOptions.NoTablePartitioningSchemes = false;
						tableOptions.ScriptDataCompression = true;
					}

					string fileName = Path.Combine(relativeDir, table.Name + ".sql");
					AddScriptFile(fileName);
					string outputFileName = Path.Combine(OutputDirectory, fileName);
					OnProgressMessageReceived(fileName);
					WriteBatches(outputFileName, tableScripter.ScriptWithList(objects));

					// When targeting SSDT, use a single file for each table.
					// Otherwise, create separate kci (key, constraint, index) and fky (foreign key) files.
					if(TargetDataTools)
					{
						WriteBatches(outputFileName, true, kciScripter.ScriptWithList(objects));
						WriteBatches(outputFileName, true, fkyScripter.ScriptWithList(objects));
					}
					else
					{
						string kciFileName = Path.ChangeExtension(fileName, ".kci.sql");
						kciFileNames.Add(kciFileName);
						outputFileName = Path.Combine(OutputDirectory, kciFileName);
						OnProgressMessageReceived(kciFileName);
						WriteBatches(outputFileName, kciScripter.ScriptWithList(objects));

						string fkyFileName = Path.ChangeExtension(fileName, ".fky.sql");
						fkyFileNames.Add(fkyFileName);
						outputFileName = Path.Combine(OutputDirectory, fkyFileName);
						OnProgressMessageReceived(fkyFileName);
						WriteBatches(outputFileName, fkyScripter.ScriptWithList(objects));
					}

					if(table.RowCount > 0)
					{
						// If the table does not have a unique index (a primary key is a unique index)
						// then output a warning.
						if(!table.HasUniqueIndex())
						{
							string warning = String.Format
							(
								"Warning: The table {0}.{1} has data but does not have a primary key or unique index.",
								MakeSqlBracket(table.Schema),
								MakeSqlBracket(table.Name)
							);
							OnErrorMessageReceived(warning);
						}
						// If the table has any variant columns then we can't use a BCP text format.
						if(table.HasAnyVariantColumns())
						{
							// If the table has more than 50,000 rows then we will use BCP with a Unicode native format.
							// This is a binary format that can handle the variant data type and a large number of rows.
							if(table.RowCount > 50000)
							{
								BulkCopyTableDataUnicodeNative(table);
							}
							else
							{
								// Otherwise use INSERT statements.
								// This format is can be viewed in a text editor, diff'd and merged and can handle the
								// variant data type but is more verbose (leading to larger files).
								ScriptTableData(table);
							}
						}
						else if(table.RowCount > 50000)
						{
							// If the table has more than 50,000 rows then we will use BCP with a compact text format.
							// We used to use Unicode native format (see BulkCopyTableDataUnicodeNative method) but
							// the Unicode native format is a binary file that doesn't diff or merge well.
							// The compact text format is larger than the binary format but is more compact than the
							// full JSON-like text format used for tables with fewer rows.
							BulkCopyTableDataCompactCodePage(table, "1252");
						}
						else if(table.HasAnyXmlColumns())
						{
							// If the table has any XML columns then we prefer INSERT statements over the custom JSON-
							// like BCP format. When we script out INSERT statements we can nicely format the XML
							// with newlines and indents rather than having it all on one line. This allows the XML
							// to be more easily viewed, edited, diff'd and merged.
							ScriptTableData(table);
						}
						else
						{
							// Otherwise use BCP with a custom JSON-like text format.
							// This format is designed to allow the file to be viewed in a text editor,
							// diff'd and merged.
							// For tables that have more than a few rows, this method runs faster than INSERT statements
							// when deploying the database (see the ScriptTableData method).
							BulkCopyTableDataCodePage(table, "1252");
						}
					}

					// Clear the set of unique extended properties.
					// This isn't strictly necessary, but it does avoid the memory build up
					// of collecting the properties from all tables into the set.
					extendedPropertySet.Clear();
				}
			}
			AddScriptFileRange(kciFileNames);
			AddScriptFileRange(fkyFileNames);
		}

		private void ScriptTableData(Table table)
		{
			string relativeDataDir = Path.Combine("Schemas", table.Schema, "Data");
			string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
			if(!Directory.Exists(dataDir))
				Directory.CreateDirectory(dataDir);

			string relativeFileName = Path.Combine(relativeDataDir, table.Name + ".sql");
			AddScriptFile(relativeFileName);

			string fileName = Path.Combine(OutputDirectory, relativeFileName);
			OnProgressMessageReceived(relativeFileName);

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

		/// <summary>
		/// Scripts out a table's data using bcp Unicode native format (-N).
		/// </summary>
		private void BulkCopyTableDataUnicodeNative(Table table)
		{
			string relativeDataDir = Path.Combine("Schemas", table.Schema, "Data");
			string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
			if(!Directory.Exists(dataDir))
				Directory.CreateDirectory(dataDir);

			string relativeDataFile = Path.Combine(relativeDataDir, table.Name + ".dat");

			string dataFile = Path.Combine(OutputDirectory, relativeDataFile);
			OnProgressMessageReceived(relativeDataFile);

			// Run bcp to create the data file.
			// bcp "SELECT * FROM [database].[schema].[table] ORDER BY ..." queryout "dataFile" -S servername -T -N
			string bcpArguments = String.Format
			(
				"\"SELECT * FROM {0}.{1}.{2} {3}\" out \"{4}\" -S {5} -T -N",
				MakeSqlBracket(this.DatabaseName),
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				GetOrderByClauseForTable(table),
				dataFile,
				this.ServerName
			);

			RunBcp(bcpArguments);

			// We have to wait to add the file until after the file is generated
			// because adding the file will check the file size.
			AddUnicodeNativeDataFile(relativeDataFile, table.Schema, table.Name);
		}

		/// <summary>
		/// Scripts out a table's data using a custom Unicode character bcp format.
		/// </summary>
		private void BulkCopyTableDataUtf16(Table table)
		{
			string relativeDataDir = Path.Combine("Schemas", table.Schema, "Data");
			string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
			if(!Directory.Exists(dataDir))
				Directory.CreateDirectory(dataDir);

			// We use the ".udat" extension to distinguish it from other .dat files so that
			// a text editor can be associated with the extension and so that source control
			// systems can be configured to handle it appropriately.
			string relativeDataFile = Path.Combine(relativeDataDir, table.Name + ".udat");

			string dataFile = Path.Combine(OutputDirectory, relativeDataFile);
			string tmpDataFile = dataFile + ".tmp";
			string formatFile = Path.ChangeExtension(dataFile, ".fmt");
			OnProgressMessageReceived(relativeDataFile);

			// Run bcp to create the format file.
			// bcp "[schema].[table]" format nul -S servername -d database -T -w -f formatFile -x
			string bcpArguments = String.Format
			(
				"\"{0}.{1}\" format nul -S \"{2}\" -d \"{3}\" -T -w -f \"{4}\" -x",
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				this.ServerName,
				this.DatabaseName,
				formatFile
			);

			RunBcp(bcpArguments);

			// Modify the format file so that the data file will be formatted how we want it to be.
			ModifyOutUtf16BcpFormatFile(formatFile, table);

			// Run bcp to create the data file.
			// bcp "SELECT CONVERT(nvarchar(2), null), <columns> FROM [schema].[table] <order by clause>" queryout tmpDataFile -S servername -d database -T -f formatFile
			bcpArguments = String.Format
			(
				"\"SELECT CONVERT(nvarchar(2), null), {0} FROM {1}.{2} {3}\" queryout \"{4}\" -S \"{5}\" -d \"{6}\" -T -f \"{7}\"",
				GetBulkCopySelectString(table),
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				GetOrderByClauseForTable(table),
				tmpDataFile,
				this.ServerName,
				this.DatabaseName,
				formatFile
			);

			RunBcp(bcpArguments);

			// In order to ensure that Visual Studio, Notepad, and other editors open the file correctly,
			// add UTF16 byte order mark to the start of the data file. Unfortunately, the only way to do
			// this is to copy the file (that is why we have bcp write to a tmp file).
			CopyUtf16BcpDataFile(tmpDataFile, dataFile);

			// Modify the format file so that it can be used by bcp to load data into the database.
			ModifyInUtf16BcpFormatFile(formatFile);

			// We have to wait to add the file until after the file is generated
			// because adding the file will check the file size.
			AddUtf16DataFile(relativeDataFile, table.Schema, table.Name);
		}

		/// <summary>
		/// Scripts out a table's data using a custom character bcp format using the specified code page.
		/// </summary>
		/// <remarks>
		/// The codePage parameter is a string because BCP also supports "ACP", "OEM", and "RAW" in addition
		/// to numeric code pages.
		/// </remarks>
		private void BulkCopyTableDataCodePage(Table table, string codePage = "1252")
		{
			string relativeDataDir = Path.Combine("Schemas", table.Schema, "Data");
			string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
			if(!Directory.Exists(dataDir))
				Directory.CreateDirectory(dataDir);

			// We use the ".txt" extension to treat it as a text file.
			string relativeDataFile = Path.Combine(relativeDataDir, table.Name + ".txt");

			string dataFile = Path.Combine(OutputDirectory, relativeDataFile);
			string formatFile = Path.ChangeExtension(dataFile, ".fmt");
			OnProgressMessageReceived(relativeDataFile);

			// Run bcp to create the format file.
			// bcp "[schema].[table]" format nul -S servername -d database -T -c -C codePage -f formatFile -x
			string bcpArguments = String.Format
			(
				"\"{0}.{1}\" format nul -S \"{2}\" -d \"{3}\" -T -c -C {4} -f \"{5}\" -x",
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				this.ServerName,
				this.DatabaseName,
				codePage,
				formatFile
			);

			RunBcp(bcpArguments);

			// Modify the format file so that the data file will be formatted how we want it to be.
			ModifyOutCodePageBcpFormatFile(formatFile, table);

			// Run bcp to create the data file.
			// bcp "SELECT CONVERT(nvarchar(2), null), <columns> FROM [schema].[table] <order by clause>" queryout dataFile -S servername -d database -T -C codePage -f formatFile
			bcpArguments = String.Format
			(
				"\"SELECT CONVERT(nvarchar(2), null), {0} FROM {1}.{2} {3}\" queryout \"{4}\" -S \"{5}\" -d \"{6}\" -T -C {7} -f \"{8}\"",
				GetBulkCopySelectString(table),
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				GetOrderByClauseForTable(table),
				dataFile,
				this.ServerName,
				this.DatabaseName,
				codePage,
				formatFile
			);

			RunBcp(bcpArguments);

			// Modify the format file so that it can be used by bcp to load data into the database.
			ModifyInCodePageBcpFormatFile(formatFile);

			// We have to wait to add the file until after the file is generated
			// because adding the file will check the file size.
			AddCodePageDataFile(relativeDataFile, table.Schema, table.Name, codePage);
		}

		/// <summary>
		/// Scripts out a table's data using a custom character bcp format using the specified code page.
		/// </summary>
		/// <remarks>
		/// The codePage parameter is a string because BCP also supports "ACP", "OEM", and "RAW" in addition
		/// to numeric code pages.
		/// </remarks>
		private void BulkCopyTableDataCompactCodePage(Table table, string codePage = "1252")
		{
			string relativeDataDir = Path.Combine("Schemas", table.Schema, "Data");
			string dataDir = Path.Combine(OutputDirectory, relativeDataDir);
			if(!Directory.Exists(dataDir))
				Directory.CreateDirectory(dataDir);

			// We use the ".txt" extension to treat it as a text file.
			string relativeDataFile = Path.Combine(relativeDataDir, table.Name + ".txt");

			string dataFile = Path.Combine(OutputDirectory, relativeDataFile);
			string formatFile = Path.ChangeExtension(dataFile, ".fmt");
			OnProgressMessageReceived(relativeDataFile);

			// Run bcp to create the format file.
			// bcp "[schema].[table]" format nul -S servername -d database -T -c -C codePage -f formatFile -x
			string bcpArguments = String.Format
			(
				"\"{0}.{1}\" format nul -S \"{2}\" -d \"{3}\" -T -c -C {4} -f \"{5}\" -x",
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				this.ServerName,
				this.DatabaseName,
				codePage,
				formatFile
			);

			RunBcp(bcpArguments);

			// Modify the format file so that the data file will be formatted how we want it to be.
			ModifyOutCompactCodePageBcpFormatFile(formatFile, table);

			// Run bcp to create the data file.
			// bcp "SELECT CONVERT(nvarchar(2), null), <columns> FROM [schema].[table] <order by clause>" queryout dataFile -S servername -d database -T -C codePage -f formatFile
			bcpArguments = String.Format
			(
				"\"SELECT CONVERT(nvarchar(2), null), {0} FROM {1}.{2} {3}\" queryout \"{4}\" -S \"{5}\" -d \"{6}\" -T -C {7} -f \"{8}\"",
				GetBulkCopySelectString(table),
				MakeSqlBracket(table.Schema),
				MakeSqlBracket(table.Name),
				GetOrderByClauseForTable(table),
				dataFile,
				this.ServerName,
				this.DatabaseName,
				codePage,
				formatFile
			);

			RunBcp(bcpArguments);

			// Modify the format file so that it can be used by bcp to load data into the database.
			ModifyInCodePageBcpFormatFile(formatFile);

			// We have to wait to add the file until after the file is generated
			// because adding the file will check the file size.
			AddCodePageDataFile(relativeDataFile, table.Schema, table.Name, codePage);
		}

		private bool SkipBulkCopyColumn(Column column)
		{
			return column.Computed || GetBaseSqlDataType(column.DataType) == SqlDataType.Timestamp;
		}

		private string GetBulkCopySelectString(Table table)
		{
			return String.Join(", ", table.Columns.Cast<Column>().Select(GetBulkCopySelectString));
		}

		private string GetBulkCopySelectString(Column column)
		{
			// For columns to be skipped (e.g. computed, timestamp) all we need is a null
			// placeholder column--the type doesn't really matter, so we make it a bit.
			if(SkipBulkCopyColumn(column))
				return "CONVERT(bit, NULL)";
			else
				return MakeSqlBracket(column.Name);
		}

		/// <summary>
		/// Copies data from a temporary bcp file to a destination data file, adding a byte order mark, and then deletes the temp file.
		/// </summary>
		private void CopyUtf16BcpDataFile(string tmpDataFile, string dataFile)
		{
			using(Stream source = new FileStream(tmpDataFile, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read))
			using(Stream destination = new FileStream(dataFile, FileMode.OpenOrCreate, FileAccess.ReadWrite))
			{
				// Write the byte order mark.
				destination.Write(new byte[]{ 0xff, 0xfe }, 0, 2);

				// Copy data from the tmp file.
				source.CopyTo(destination);
				
				// Since we are overwriting the destination, ensure to set the proper length (in case the new data is smaller).
				destination.SetLength(destination.Position);
			}
			File.Delete(tmpDataFile);
		}

		private string EscapeUtf16BcpTerminator(string value)
		{
			StringBuilder result = new StringBuilder(value.Length * 3);
			foreach(char ch in value)
			{
				result.Append(ch);
				result.Append(@"\0");
			}
			return result.ToString();
		}

		/// <summary>
		/// Modifies a bcp format file to output a data file with a format similar to JSON.
		/// </summary>
		/// <remarks>
		/// This format is designed to be source-control friendly for diff and merge operations.
		/// Note that the occurences of "\r", "\n", and "\0" in the terminator actually do contain
		/// backslashes that should show up in the XML format file as backslashes.
		/// This method adds an extra "padding" column so that the terminator of this column will look
		/// like the label for the first column.
		/// </remarks>
		private void ModifyOutUtf16BcpFormatFile(string formatFile, Table table)
		{
			ModifyOutBcpFormatFile
			(
				formatFile,
				table,
				name => EscapeUtf16BcpTerminator(@"{\r\n\t" + name + @": "),
				name => EscapeUtf16BcpTerminator(@",\r\n\t" + name + @": "),
				EscapeUtf16BcpTerminator("*"),
				EscapeUtf16BcpTerminator(@"\r\n}\r\n")
			);
		}

		/// <summary>
		/// Modifies a bcp format file to output a data file with a format similar to JSON.
		/// </summary>
		/// <remarks>
		/// This format is designed to be source-control friendly for diff and merge operations.
		/// Note that the occurences of "\r" and "\n" in the terminator actually do contain
		/// backslashes that should show up in the XML format file as backslashes.
		/// This method adds an extra "padding" column so that the terminator of this column will look
		/// like the label for the first column.
		/// </remarks>
		private void ModifyOutCodePageBcpFormatFile(string formatFile, Table table)
		{
			ModifyOutBcpFormatFile
			(
				formatFile,
				table,
				name => @"{\r\n\t" + name + @": ",
				name => @",\r\n\t" + name + @": ",
				"*",
				@"\r\n}\r\n"
			);
		}

		/// <summary>
		/// Modifies a bcp format file to output a data file with a format similar to JSON.
		/// </summary>
		/// <remarks>
		/// This format is designed to be source-control friendly for diff and merge operations.
		/// Note that the occurences of "\r" and "\n" in the terminator actually do contain
		/// backslashes that should show up in the XML format file as backslashes.
		/// This method adds an extra "padding" column so that the terminator of this column will look
		/// like the label for the first column.
		/// </remarks>
		private void ModifyOutCompactCodePageBcpFormatFile(string formatFile, Table table)
		{
			ModifyOutBcpFormatFile
			(
				formatFile,
				table,
				name => "«",
				name => "»,«",
				"*",
				@"»;\r\n"
			);
		}

		/// <summary>
		/// Modifies a bcp format file to output a data file with a format similar custom terminators.
		/// </summary>
		/// <param name="formatFile"></param>
		/// <param name="table"></param>
		/// <param name="firstTerminatorSelector">Function that returns the terminator for the first column from the column name.</param>
		/// <param name="innerTerminatorSelector">Function that returns the terminator for an inner column from the column name.</param>
		/// <param name="replacement">Text to use in place of a calculated or timestamp column.</param>
		/// <param name="rowTerminator">Terminator to use for the row (the last column).</param>
		private void ModifyOutBcpFormatFile
		(
			string formatFile,
			Table table,
			Func<string, string> firstTerminatorSelector,
			Func<string, string> innerTerminatorSelector,
			string replacement,
			string rowTerminator
		)
		{
			XElement formatElement = XElement.Load(formatFile);
			XNamespace ns = "http://schemas.microsoft.com/sqlserver/2004/bulkload/format";
			XNamespace xsi = "http://www.w3.org/2001/XMLSchema-instance";

			// The format file has a "RECORD" that contains "FIELD" elements that describe the
			// format of data field in the data file.
			XElement record = formatElement.Element(ns + "RECORD");

			// The format file also has a "ROW" that contains "COLUMN" elements that describe the
			// schema of the rows in SQL (name and SQL data type).
			XElement row = formatElement.Element(ns + "ROW");

			// Get an array of the fields.
			var fields = record.Elements(ns + "FIELD").ToList();

			// Get an array of the columns.
			var columns = row.Elements(ns + "COLUMN").ToList();

			// Use this terminator for the last included column
			// (the first one to be processed in reverse order).
			string terminator = rowTerminator;

			// Loop through all the fields/columns in reverse order.
			for(int index = fields.Count - 1; index >= 0; index--)
			{
				var field = fields[index];
				var column = columns[index];
				string columnName = (string)column.Attribute("NAME");

				// If the column should be skipped, then prepend the replacement text (e.g. *)
				// to the terminator.
				if(SkipBulkCopyColumn(table.Columns[index]))
					terminator = replacement + terminator;

				// Update the TERMINATOR of the field.
				field.SetAttributeValue("TERMINATOR", terminator);

				// Use this column's name in the terminator for the next column
				// ("next" in reverse, so really the preceeding column).
				terminator = innerTerminatorSelector(columnName);
			}

			string firstColumnName = (string)columns[0].Attribute("NAME");

			// Add a blank padding field to the start of the record.
			XElement paddingField = new XElement
			(
				ns + "FIELD",
				new XAttribute("ID", 0),
				new XAttribute(xsi + "type", "CharTerm"),
				new XAttribute("TERMINATOR", firstTerminatorSelector(firstColumnName)),
				new XAttribute("MAX_LENGTH", 2)
			);
			record.AddFirst(paddingField);

			// Add a blank padding column to the start of the row.
			XElement paddingColumn = new XElement
			(
				ns + "COLUMN",
				new XAttribute("SOURCE", 0),
				new XAttribute("NAME", "__Padding__"),
				new XAttribute(xsi + "type", "SQLVARYCHAR")
			);
			row.AddFirst(paddingColumn);

			// Overwrite the format file.
			formatElement.Save(formatFile);
		}

		/// <summary>
		/// Modifies a bcp format file to load a data file with a format similar to JSON.
		/// </summary>
		/// <remarks>
		/// This method expects a format file that has already been modified by <see cref="ModifyOutBcpFormatFile"/>.
		/// This method will remove the "padding" column from the "ROW" but leave the corresponding field in the "RECORD".
		/// </remarks>
		private void ModifyInUtf16BcpFormatFile(string formatFile)
		{
			// There is currently no difference in behavior for modifying the Utf16 vs CodePage format file.
			ModifyInCodePageBcpFormatFile(formatFile);
		}

		/// <summary>
		/// Modifies a bcp format file to load a data file with a format similar to JSON.
		/// </summary>
		/// <remarks>
		/// This method expects a format file that has already been modified by <see cref="ModifyOutBcpFormatFile"/>.
		/// This method will remove the "padding" column from the "ROW" but leave the corresponding field in the "RECORD".
		/// </remarks>
		private void ModifyInCodePageBcpFormatFile(string formatFile)
		{
			XElement formatElement = XElement.Load(formatFile);
			XNamespace ns = "http://schemas.microsoft.com/sqlserver/2004/bulkload/format";

			// Remove the first "COLUMN" element from the "ROW".
			XElement row = formatElement.Element(ns + "ROW");
			row.Element(ns + "COLUMN").Remove();

			// Overwrite the format file.
			formatElement.Save(formatFile);
		}

		private void RunBcp(string arguments)
		{
			ProcessStartInfo bcpStartInfo = new ProcessStartInfo("bcp.exe", arguments);
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

		private bool SkipBcpErrorMessage(string message)
		{
			// We don't need to see the empty string warning.
			return message == "SQLState = S1000, NativeError = 0" ||
			(
				message != null && message.Contains("Warning: BCP import with a format file will convert empty strings in delimited columns to NULL.")
			);
		}

		void bcpProcess_ErrorDataReceived(object sender, DataReceivedEventArgs e)
		{
			if(!SkipBcpErrorMessage(e.Data))
				OnErrorMessageReceived(e.Data);
		}

		void bcpProcess_OutputDataReceived(object sender, DataReceivedEventArgs e)
		{
			if(!SkipBcpErrorMessage(e.Data))
				OnOutputMessageReceived(e.Data);
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
						string fileName;
						switch(node.Urn.Type)
						{
							case "View":
								fileName = String.Format(@"Schemas\{0}\Views\{1}.sql", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
								AddScriptFile(fileName);
								break;
							case "UserDefinedFunction":
								fileName = String.Format(@"Schemas\{0}\Functions\{1}.sql", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
								AddScriptFile(fileName);
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
			OnProgressMessageReceived(fileName);
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
			viewOptions.ExtendedProperties = true;
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
			triggerOptions.ExtendedProperties = true;
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
				OnProgressMessageReceived(fileName);
				
				objects[0] = view;
				StringCollection script = viewScripter.ScriptWithList(objects);

				if(!TargetDataTools)
				{
					// The 3rd batch in the script is the CREATE VIEW statement.
					// Replace it with an ALTER VIEW statement.
					script[2] = view.ScriptHeader(true) + view.TextBody;
				}

				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					WriteBatches(writer, script);

					foreach(Trigger trigger in view.Triggers)
					{
						objects[0] = trigger;
						if(!TargetDataTools)
						{
							triggerScripter.Options = dropOptions;
							WriteBatches(writer, triggerScripter.ScriptWithList(objects));
						}
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
			OnProgressMessageReceived(fileName);
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			options.ExtendedProperties = true;
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
			OnProgressMessageReceived(fileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			options.ExtendedProperties = true;
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
			OnProgressMessageReceived(fileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			options.ExtendedProperties = true;
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
			OnProgressMessageReceived(fileName);
			options.ToFileOnly = true;
			options.Encoding = this.Encoding;
			options.ExtendedProperties = true;
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
			OnProgressMessageReceived(fileName);
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

			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;
			dropOptions.TargetServerVersion = this.TargetServerVersion;

			ScriptingOptions options = new ScriptingOptions
			{
				ExtendedProperties = true,
				Permissions = true,
				PrimaryObject = false,
				TargetServerVersion = this.TargetServerVersion
			};
			
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;
			
			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach (StoredProcedure sproc in sprocs)
			{
				string fileName = Path.Combine(relativeDir, sproc.Name + ".sql");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				OnProgressMessageReceived(fileName);
				using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					objects[0] = sproc;
					if(!TargetDataTools)
					{
						scripter.Options = dropOptions;
						WriteBatches(writer, scripter.ScriptWithList(objects));
					}
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
			OnProgressMessageReceived(fileName);
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
			options.ExtendedProperties = true;
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
				OnProgressMessageReceived(fileName);
				objects[0] = udf;
				StringCollection script = scripter.ScriptWithList(objects);
				if(!TargetDataTools)
				{
					// The 3rd batch in the script is the CREATE FUNCTION statement.
					// Replace it with an ALTER FUNCTION statement.
					script[2] = udf.ScriptHeader(true) + udf.TextBody;
				}
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
				options.ExtendedProperties = true;
				options.TargetServerVersion = this.TargetServerVersion;

				OnProgressMessageReceived(fileName);

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
				options.ExtendedProperties = true;
				options.TargetServerVersion = this.TargetServerVersion;

				OnProgressMessageReceived(fileName);

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
			ScriptingOptions options = new ScriptingOptions
			{
				AllowSystemObjects = false,
				Encoding = Encoding,
				ExtendedProperties = true,
				FileName = Path.Combine(this.OutputDirectory, fileName),
				Permissions = true,
				TargetServerVersion = this.TargetServerVersion,
				ToFileOnly = true
			};
			
			if(!TargetDataTools)
			{
				options.IncludeIfNotExists = true;
			}

			OnProgressMessageReceived(fileName);

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
			ScriptingOptions options = new ScriptingOptions
			{
				AllowSystemObjects = false,
				Encoding = Encoding,
				FileName = Path.Combine(OutputDirectory, fileName),
				Permissions = true,
				ScriptOwner = true,
				TargetServerVersion = this.TargetServerVersion,
				ToFileOnly = true
			};

			if(!TargetDataTools)
			{
				options.IncludeIfNotExists = true;
			}

			OnProgressMessageReceived(fileName);

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
				OnProgressMessageReceived(relativeFilePath);

				ScriptingOptions options = new ScriptingOptions();
				options.FileName = Path.Combine(OutputDirectory, relativeFilePath);
				options.ToFileOnly = true;
				options.Encoding = Encoding;
				options.AllowSystemObjects = false;
				options.ExtendedProperties = true;
				options.Permissions = true;
				options.TargetServerVersion = this.TargetServerVersion;

				if(!TargetDataTools)
				{
					options.IncludeIfNotExists = true;
				}

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
			options.ExtendedProperties = true;
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
					OnProgressMessageReceived(fileName);

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

		private static string MakeSqlBracket(string name)
		{
			return ScriptUtility.MakeSqlBracket(name);
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

		private SqlDataType GetBaseSqlDataType(DataType dataType)
		{
			return utility.GetBaseSqlDataType(dataType);
		}

		private string GetDataTypeAsString(DataType dataType)
		{
			return utility.GetDataTypeAsString(dataType);
		}
		
		private string GetOrderByClauseForTable(Table table)
		{
			string checksumColumnList;
			return GetOrderByClauseForTable(table, out checksumColumnList);
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
			return ScriptUtility.GetSqlLiteral(sqlValue, sqlDataType);
		}

		private string GetSqlVariantLiteral(object sqlValue, SqlString baseType, SqlInt32 precision, SqlInt32 scale, SqlString collation, SqlInt32 maxLength)
		{
			return utility.GetSqlVariantLiteral(sqlValue, baseType, precision, scale, collation, maxLength);
		}

		private bool IsAddExtendedPropertyStatement(string batch)
		{
			return batch.StartsWith("EXEC sys.sp_addextendedproperty");
		}

		private void OnErrorMessageReceived(string message)
		{
			if(ErrorMessageReceived == null)
				Console.Error.WriteLine(message);
			else
				ErrorMessageReceived(this, new MessageReceivedEventArgs(message));
		}

		private void OnOutputMessageReceived(string message)
		{
			if(OutputMessageReceived == null)
				Console.WriteLine(message);
			else
				OutputMessageReceived(this, new MessageReceivedEventArgs(message));
		}

		private void OnProgressMessageReceived(string message)
		{
			if(ProgressMessageReceived == null)
			{
				// For the console indicate progress with a period when the message is null.
				if(message == null)
					Console.Write('.');
				else
					Console.WriteLine(message);
			}
			else
				ProgressMessageReceived(this, new MessageReceivedEventArgs(message));
		}

		private bool SkipBatch(string batch)
		{
			// When targetting SSDT (data tools), skip SET statements.
			// When a script contains a SET statement, SSDT fails to build the project and returns this error:
			// "SQL70001: This statement is not recognized in this context."
			if(TargetDataTools && batch.StartsWith("SET", StringComparison.OrdinalIgnoreCase))
				return true;
			else if(IsAddExtendedPropertyStatement(batch))
			{
				// If this is an extended property, then add it to the set.
				// If it is already in the set, then it won't be added again,
				// and we want to skip it.
				bool added = this.extendedPropertySet.Add(batch);
				return !added;
			}
			else
				return false;
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
				string trimmedBatch = batch.Trim();
				if(!SkipBatch(batch))
				{
					writer.WriteLine(trimmedBatch);
					writer.WriteLine("GO");
				}
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
