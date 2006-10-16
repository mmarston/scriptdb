using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using System.Xml;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Broker;
using Microsoft.SqlServer.Management.Common;

namespace Mercent.SqlServer.Management
{
	public class FileScripter
	{
		private List<string> fileNames = new List<string>();
		private Dictionary<string, string> fileDictionary;
		private Server server;
		private Database database;
		
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

		private Encoding encoding = Encoding.ASCII;
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
			
			server = new Server(ServerName);
			// Set the execution mode to capture SQL (this is like saving the script
			// when editing sql objects in Management Studio).
			server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;

			database = server.Databases[databaseName];
			server.SetDefaultInitFields(typeof(Column), true);
			server.SetDefaultInitFields(typeof(View), new string[] {"ID", "Name", "Schema", "IsSystemObject"});

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
			ScriptUserDefinedFunctions();
			ScriptViews();
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
						writer.WriteLine("!!bcp \"[$(DBNAME)].[{0}].[{1}]\" in \"{2}\" -S $(SQLCMDSERVER) -T -n -k -E", schemaName, tableName, fileName);
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
			//database.FullTextCatalogs;
			//database.PartitionFunctions;
			//database.PartitionSchemes;
			//database.Rules;
			//database.SymmetricKeys;
			//database.Synonyms;
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
			database.PrefetchObjects(typeof(SqlAssembly), options);

			if(database.Assemblies.Count > 0)
			{
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
									object sqlSmoObject = null;
									switch(child.Urn.Type)
									{
										case "StoredProcedure":
											{
												StoredProcedure procedure = database.StoredProcedures[child.Urn.GetAttribute("Name"), child.Urn.GetAttribute("Schema")];
												string sqlCommand = String.Format("SELECT parameter_id, default_value FROM {0}.sys.parameters WHERE [object_id] = {1} AND has_default_value = 1",
												MakeSqlBracket(database.Name), procedure.ID);
												object[] defaults = new object[procedure.Parameters.Count];
												server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.ExecuteSql;
												using(SqlDataReader reader = server.ConnectionContext.ExecuteReader(sqlCommand))
												{
													while(reader.Read())
													{
														defaults[reader.GetInt32(0) - 1] = reader.GetSqlValue(1);
													}
												}
												server.ConnectionContext.ExecuteNonQuery("USE " + MakeSqlBracket(database.Name));
												for(int i = 0; i < defaults.Length; i++)
												{
													if(defaults[i] != null)
													{
														procedure.Parameters[i].DefaultValue = GetSqlLiteral(defaults[i]);
													}
												}
											}
											break;
										case "UserDefinedFunction":
											{
												UserDefinedFunction function = database.UserDefinedFunctions[child.Urn.GetAttribute("Name"), child.Urn.GetAttribute("Schema")];
												string sqlCommand = String.Format("SELECT parameter_id, default_value FROM {0}.sys.parameters WHERE [object_id] = {1} AND has_default_value = 1",
												MakeSqlBracket(database.Name), function.ID);
												object[] defaults = new object[function.Parameters.Count];
												server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.ExecuteSql;
												using(SqlDataReader reader = server.ConnectionContext.ExecuteReader(sqlCommand))
												{
													while(reader.Read())
													{
														defaults[reader.GetInt32(0) - 1] = reader.GetSqlValue(1);
													}
												}
												server.ConnectionContext.ExecuteNonQuery("USE " + MakeSqlBracket(database.Name));
												for(int i = 0; i < defaults.Length; i++)
												{
													if(defaults[i] != null)
													{
														function.Parameters[i].DefaultValue = GetSqlLiteral(defaults[i]);
													}
												}
											}
											break;
									}
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
		}

		private void ScriptDatabase()
		{
			string fileName = "Database.sql";
			ScriptingOptions options = new ScriptingOptions();
			//options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;
			options.NoFileGroup = true;


			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			Console.WriteLine(Path.Combine(this.OutputDirectory, fileName));

			StringCollection batches = scripter.ScriptWithList(new SqlSmoObject[] { database });
			string oldName = database.Name;
			string oldNameIdentifier = "[" + oldName + "]";
			string oldNameLiteral = "N'" + oldName + "'";
			string newName = "$(DBNAME)";
			string newNameIdentifier = "[" + newName + "]";
			string newNameLiteral = "N'" + newName + "'";
			
			using(TextWriter writer = new StreamWriter(Path.Combine(this.OutputDirectory, fileName), false, Encoding))
			{
				foreach(string batch in batches)
				{
					writer.WriteLine(batch.Replace(oldNameIdentifier, newNameIdentifier).Replace(oldNameLiteral, newNameLiteral));
					writer.WriteLine("GO");
				}
				writer.WriteLine("USE " + newNameIdentifier);
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

			ScriptingOptions prefetchOptions = new ScriptingOptions();
			prefetchOptions.ClusteredIndexes = true;
			prefetchOptions.DriChecks = true;
			prefetchOptions.DriClustered = true;
			prefetchOptions.DriDefaults = true;
			prefetchOptions.DriIndexes = true;
			prefetchOptions.DriNonClustered = true;
			prefetchOptions.DriPrimaryKey = true;
			prefetchOptions.DriUniqueKeys = true;
			prefetchOptions.Indexes = true;
			prefetchOptions.NonClusteredIndexes = true;
			prefetchOptions.Permissions = true;
			prefetchOptions.Statistics = true;
			prefetchOptions.Triggers = true;
			prefetchOptions.XmlIndexes = true;
			prefetchOptions.DriForeignKeys = true;

			database.PrefetchObjects(typeof(Table), prefetchOptions);
			SqlSmoObject[] objects = new SqlSmoObject[1];

			SQLDMO.SQLServer sqlDmoServer = new SQLDMO.SQLServerClass();
			sqlDmoServer.LoginSecure = true;
			sqlDmoServer.Connect(this.ServerName, null, null);
			SQLDMO._Database sqlDmoDatabase = sqlDmoServer.Databases.Item(this.DatabaseName, null);
			SQLDMO.Tables sqlDmoTables = sqlDmoDatabase.Tables;

			SQLDMO.BulkCopy bulkCopy = new SQLDMO.BulkCopyClass();
			bulkCopy.DataFileType = SQLDMO.SQLDMO_DATAFILE_TYPE.SQLDMODataFile_NativeFormat;

			foreach (Table table in database.Tables)
			{
				if (!table.IsSystemObject)
				{
					objects[0] = table;
					string filename = Path.Combine(relativeDir, table.Schema + "." + table.Name + ".tab");
					tabFileNames.Add(filename);
					string outputFileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, tableScripter.ScriptWithList(objects));

					filename = Path.ChangeExtension(filename, ".kci");
					kciFileNames.Add(filename);
					outputFileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, kciScripter.ScriptWithList(objects));

					filename = Path.ChangeExtension(filename, ".fky");
					fkyFileNames.Add(filename);
					outputFileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(outputFileName);
					WriteBatches(outputFileName, fkyScripter.ScriptWithList(objects));

					if(table.RowCount > 0)
					{
						filename = Path.Combine(relativeDataDir, table.Schema + "." + table.Name + ".dat");
						tabFileNames.Add(filename);
						bulkCopy.DataFilePath = Path.Combine(OutputDirectory, filename);
						Console.WriteLine(bulkCopy.DataFilePath);
						SQLDMO._Table sqlDmoTable = sqlDmoTables.ItemByID(table.ID);
						sqlDmoTable.ExportData(bulkCopy);
					}
				}
			}

			fileNames.AddRange(tabFileNames);
			fileNames.AddRange(kciFileNames);
			fileNames.AddRange(fkyFileNames);
		}

		private void ScriptUserDefinedFunctionsAndViews()
		{
			UrnCollection urns = new UrnCollection();

			string viewRelativeDir = "Views";
			List<string> triggerFileNames = new List<string>();
			ScriptViews(viewRelativeDir, urns, triggerFileNames);

			if (urns.Count <= 0)
				return;

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Parents);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			foreach(DependencyCollectionNode node in dependencies)
			{
				// Check that the dependency is a view that we have scripted out
				if(urns.Contains(node.Urn))
				{
					string filename;
					filename = node.Urn.GetAttribute("Schema") + "." + node.Urn.GetAttribute("Name") + ".viw";
					this.fileNames.Add(Path.Combine(viewRelativeDir, filename));
				}
			}

			this.fileNames.AddRange(triggerFileNames);
			ScriptUserDefinedFunctions();
		}

		private void ScriptViews(string relativeDir, UrnCollection urns, ICollection<string> triggerFileNames)
		{
			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true; 
			
			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.Encoding = Encoding;
			viewOptions.Indexes = true;
			viewOptions.Permissions = true;
			viewOptions.Statistics = true;

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

			string dir = Path.Combine(OutputDirectory, relativeDir);
			if (!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			ScriptingOptions prefetchOptions = new ScriptingOptions();
			prefetchOptions.Indexes = true;
			prefetchOptions.Permissions = true;
			prefetchOptions.Statistics = true;
			prefetchOptions.Triggers = true;

			database.PrefetchObjects(typeof(View), prefetchOptions);
			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach (View view in database.Views)
			{
				if (!view.IsSystemObject)
				{
					string filename = Path.Combine(relativeDir, view.Schema + "." + view.Name + ".viw");
					string outputFileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(outputFileName);
					using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
					{
						objects[0] = view;
						viewScripter.Options = dropOptions;
						WriteBatches(writer, viewScripter.ScriptWithList(objects));
						viewScripter.Options = viewOptions;
						WriteBatches(writer, viewScripter.ScriptWithList(objects));
					}
					urns.Add(view.Urn);

					foreach(Trigger trigger in view.Triggers)
					{
						filename = Path.Combine(relativeDir, view.Schema + "." + trigger.Name + ".trg"); // is the trigger schema the same as the view?
						outputFileName = Path.Combine(OutputDirectory, filename);
						Console.WriteLine(outputFileName);
						using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
						{
							objects[0] = trigger;
							triggerScripter.Options = dropOptions;
							WriteBatches(writer, triggerScripter.ScriptWithList(objects));
							triggerScripter.Options = triggerOptions;
							WriteBatches(writer, triggerScripter.ScriptWithList(objects));
						}
						triggerFileNames.Add(filename);
					}
				}
			}
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

		private void ScriptViews()
		{
			ScriptingOptions prefetchOptions = new ScriptingOptions();
			prefetchOptions.Indexes = true;
			prefetchOptions.Permissions = true;
			prefetchOptions.Statistics = true;
			prefetchOptions.Triggers = true;

			database.PrefetchObjects(typeof(View), prefetchOptions);
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

			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true; 

			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.Encoding = Encoding;
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
				this.fileNames.Add(fileName);
				
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
					this.fileNames.Add(fileName);
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
			scripter.ScriptWithList(urns);
			this.fileNames.Add(fileName);
		}

		private void ScriptServiceBrokerQueues()
		{
			// Get a list of IDs for Queues that are not system queues
			List<int> nonSystemQueueIds = new List<int>();
			server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.ExecuteSql;
			string sqlCommand = String.Format("select object_id from {0}.sys.service_queues WHERE is_ms_shipped = 0 ORDER BY object_id", MakeSqlBracket(database.Name));
			using(SqlDataReader reader = server.ConnectionContext.ExecuteReader(sqlCommand))
			{
				while(reader.Read())
				{
					nonSystemQueueIds.Add(reader.GetInt32(0));
				}
			}

			// After using the ConnectionContext to execute the reader above
			// we have to change the connection back to using the correct database.
			// This appears to be an issue/bug with SQL SMO.
			sqlCommand = String.Format("USE {0}", MakeSqlBracket(database.Name));
			server.ConnectionContext.ExecuteNonQuery(sqlCommand);

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

			database.PrefetchObjects(typeof(StoredProcedure), options);
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
			//database.PrefetchObjects(typeof(UserDefinedFunction));
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

		private void ScriptUserDefinedFunctions()
		{
			database.PrefetchObjects(typeof(UserDefinedFunction));
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
				this.fileNames.Add(fileName);
			}
		}
		
		private void ScriptUserDefinedFunctions(string relativeDir, UrnCollection urns)
		{
			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;

			ScriptingOptions options = new ScriptingOptions();
			options.Encoding = this.Encoding;
			options.Permissions = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

			string dir = Path.Combine(OutputDirectory, relativeDir);
			if (!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			database.PrefetchObjects(typeof(UserDefinedFunction), options);
			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach (UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if (!udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
				{
					string filename = Path.Combine(relativeDir, udf.Schema + "." + udf.Name + ".udf");
					string outputFileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(outputFileName);
					using(TextWriter writer = new StreamWriter(outputFileName, false, this.Encoding))
					{
						objects[0] = udf;
						scripter.Options = dropOptions;
						WriteBatches(writer, scripter.ScriptWithList(objects));
						scripter.Options = options;
						WriteBatches(writer, scripter.ScriptWithList(objects));
					}
					urns.Add(udf.Urn);
				}
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
			string fileName = "Roles.sql";
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
			transfer.CopyAllRoles = true;
			transfer.ScriptTransfer();

			// script out role membership (only members that are roles)
			using(TextWriter writer = new StreamWriter(Path.Combine(this.OutputDirectory, fileName), true, Encoding))
			{
				foreach(DatabaseRole role in database.Roles)
				{
					bool memberAdded = false;
					server.ConnectionContext.CapturedSql.Clear();
					foreach(string member in role.EnumMembers())
					{
						if(database.Roles.Contains(member))
						{
							role.AddMember(member); // call AddMember so that we can script it out
							memberAdded = true;
						}
					}
					if(memberAdded)
					{
						// Alter() will script out the the results of calling AddMember.
						role.Alter();
						StringCollection batchesWithUse = server.ConnectionContext.CapturedSql.Text;
						// Create a new collection without the USE statements that set the database context
						StringCollection batchesWithoutUse = new StringCollection();
						foreach(string batch in batchesWithUse)
						{
							if(!batch.StartsWith("USE "))
								batchesWithoutUse.Add(batch);
						}
						WriteBatches(writer, batchesWithoutUse);
					}
				}
			}
			this.fileNames.Add(fileName);
		}


		private void ScriptSchemas()
		{
			string fileName = "Schemas.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = Path.Combine(OutputDirectory, fileName);
			options.ToFileOnly = true;
			options.Encoding = Encoding;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;

			Console.WriteLine(options.FileName);

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.CopyAllObjects = false;
			transfer.CopyAllSchemas = true;
			transfer.ScriptTransfer();
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

				Transfer transfer = new Transfer(database);
				transfer.Options = options;
				transfer.CopyAllObjects = false;
				transfer.CopyAllSynonyms = true;
				transfer.ScriptTransfer();
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

			database.PrefetchObjects(typeof(XmlSchemaCollection));

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

		public string GetDataTypeAsString(DataType dataType)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(MakeSqlBracket(dataType.Name));
			switch(dataType.SqlDataType)
			{
				case SqlDataType.Binary:
				case SqlDataType.Char:
				case SqlDataType.NChar:
				case SqlDataType.NVarChar:
				case SqlDataType.VarBinary:
				case SqlDataType.VarChar:
					sb.Append('(');
					sb.Append(dataType.MaximumLength);
					sb.Append(')');
					break;
				case SqlDataType.NVarCharMax:
				case SqlDataType.VarBinaryMax:
				case SqlDataType.VarCharMax:
					sb.Append("(max)");
					break;
				case SqlDataType.Decimal:
				case SqlDataType.Numeric:
					sb.AppendFormat("({0},{1})", dataType.NumericPrecision, dataType.NumericScale);
					break;
			}
			return sb.ToString();
		}

		private string GetSqlLiteral(object val)
		{
			if(DBNull.Value == val || (val is INullable && ((INullable)val).IsNull))
				return "NULL";
			else if(val is System.Data.SqlTypes.SqlBinary)
			{
				return ByteArrayToHexLiteral(((SqlBinary)val).Value);
			}
			else if(val is System.Data.SqlTypes.SqlBoolean)
			{
				return ((SqlBoolean)val).Value ? "1" : "0";
			}
			else if(val is System.Data.SqlTypes.SqlBytes)
			{
				return ByteArrayToHexLiteral(((SqlBytes)val).Value);
			}
			else if(val is System.Data.SqlTypes.SqlChars)
			{
				return "'" + EscapeChar(new string(((SqlChars)val).Value), '\'') + "'";
			}
			else if(val is System.Data.SqlTypes.SqlDateTime)
			{
				return ((SqlDateTime)val).Value.ToString("'yyyy-MM-ddTHH:mm:ss.fff'");
			}
			else if(val is System.Data.SqlTypes.SqlDecimal
				|| val is System.Data.SqlTypes.SqlByte
				|| val is System.Data.SqlTypes.SqlInt16
				|| val is System.Data.SqlTypes.SqlInt32
				|| val is System.Data.SqlTypes.SqlInt64
				|| val is System.Data.SqlTypes.SqlMoney)
			{
				return val.ToString();
			}
			else if(val is System.Data.SqlTypes.SqlSingle)
			{
				return ((SqlSingle)val).Value.ToString("r");
			}
			else if(val is System.Data.SqlTypes.SqlDouble)
			{
				return ((SqlDouble)val).Value.ToString("r");
			}
			else if(val is System.Data.SqlTypes.SqlGuid
				|| val is System.Data.SqlTypes.SqlString)
			{
				return "'" + EscapeChar(val.ToString(), '\'') + "'";
			}
			else if(val is System.Data.SqlTypes.SqlXml)
			{
				return "'" + EscapeChar(((SqlXml)val).Value, '\'') + "'";
			}
			else
			{
				throw new ApplicationException("Unsupported type :" + val.GetType().ToString());
			}
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
