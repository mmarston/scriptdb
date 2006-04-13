using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Text;

using Microsoft.SqlServer.Management.Smo;
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

			database = server.Databases[databaseName];

			ScriptRoles();
			ScriptSchemas();
			
			ScriptTables();
			// TODO: still need to test/resolve dependencies between tables/udfs
			ScriptUserDefinedFunctions();
			ScriptViews();
			ScriptStoredProcedures();

			using(StreamWriter writer = new StreamWriter(Path.Combine(OutputDirectory, "CreateDatabaseObjects.sql"), false, Encoding))
			{
				foreach(string fileName in this.fileNames)
				{
					writer.WriteLine("PRINT '{0}'", fileName);
					writer.WriteLine("GO", fileName);
					if(Path.GetExtension(fileName) == ".dat")
					{
						writer.WriteLine("!!bcp \"$(SQLCMDDBNAME).{0}\" in \"{1}\" -S $(SQLCMDSERVER) -T -n -k", Path.GetFileNameWithoutExtension(fileName), fileName);
					}
					else
					{
						writer.WriteLine(":r \"{0}\"", fileName);
					}
				}
			}

			// Here is a list of database objects that currently are not being scripted:
			//database.Assemblies;
			//database.AsymmetricKeys;
			//database.Certificates;
			//database.ExtendedStoredProcedures;
			//database.FullTextCatalogs;
			//database.PartitionFunctions;
			//database.PartitionSchemes;
			//database.Rules;
			//database.ServiceBroker;
			//database.SymmetricKeys;
			//database.Triggers;
			//database.Users;
			//database.UserDefinedAggregates;
			//database.UserDefinedDataTypes;
			//database.UserDefinedTypes;
			//database.XmlSchemaCollections;

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

		private void ScriptTables()
		{
			ScriptingOptions tableOptions = new ScriptingOptions();
			tableOptions.ToFileOnly = true;
			tableOptions.Encoding = this.Encoding;

			Scripter tableScripter = new Scripter(server);
			tableScripter.Options = tableOptions;
			tableScripter.PrefetchObjects = false;
			
			// this list might be able to be trimmed down because
			// some of the options may overlap (e.g. DriIndexes and Indexes).
			ScriptingOptions kciOptions = new ScriptingOptions();
			kciOptions.ToFileOnly = true;
			kciOptions.Encoding = this.Encoding;
			kciOptions.PrimaryObject = false;
			kciOptions.ClusteredIndexes = true;
			kciOptions.DriChecks = true;
			kciOptions.DriClustered = true;
			kciOptions.DriDefaults = true;
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
			fkyOptions.ToFileOnly = true;
			fkyOptions.Encoding = this.Encoding;
			fkyOptions.DriForeignKeys = true;
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
					tableOptions.FileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(tableOptions.FileName);
					tableScripter.ScriptWithList(objects);

					filename = Path.ChangeExtension(filename, ".kci");
					kciFileNames.Add(filename);
					kciOptions.FileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(kciOptions.FileName);
					kciScripter.ScriptWithList(objects);

					filename = Path.ChangeExtension(filename, ".fky");
					fkyFileNames.Add(filename);
					fkyOptions.FileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(fkyOptions.FileName);
					fkyScripter.ScriptWithList(objects);

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

		private void ScriptViews()
		{
			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.ToFileOnly = true;
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true; 
			
			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.ToFileOnly = true;
			viewOptions.AppendToFile = true;
			viewOptions.Encoding = Encoding;
			viewOptions.Indexes = true;
			viewOptions.Permissions = true;
			viewOptions.Statistics = true;

			Scripter viewScripter = new Scripter(server);
			viewScripter.Options = viewOptions;
			viewScripter.PrefetchObjects = false;

			ScriptingOptions triggerOptions = new ScriptingOptions();
			triggerOptions.ToFileOnly = true;
			triggerOptions.AppendToFile = true;
			triggerOptions.Encoding = Encoding;
			triggerOptions.PrimaryObject = false;
			triggerOptions.Triggers = true;

			Scripter triggerScripter = new Scripter(server);
			triggerScripter.Options = triggerOptions;
			triggerScripter.PrefetchObjects = false;

			string relativeDir = "Views";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if (!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			UrnCollection urns = new UrnCollection();
			List<string> triggerFileNames = new List<string>();

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
					dropOptions.FileName = viewOptions.FileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(viewOptions.FileName);
						
					objects[0] = view;
					viewScripter.Options = dropOptions;
					viewScripter.ScriptWithList(objects);
					viewScripter.Options = viewOptions;
					viewScripter.ScriptWithList(objects);
					urns.Add(view.Urn);

					foreach(Trigger trigger in view.Triggers)
					{
						filename = Path.Combine(relativeDir, view.Schema + "." + trigger.Name + ".trg"); // is the trigger schema the same as the view?
						dropOptions.FileName = triggerOptions.FileName = Path.Combine(OutputDirectory, filename);
						Console.WriteLine(triggerOptions.FileName);

						objects[0] = trigger;
						triggerScripter.Options = dropOptions;
						triggerScripter.ScriptWithList(objects);
						triggerScripter.Options = triggerOptions;
						triggerScripter.ScriptWithList(objects);
						triggerFileNames.Add(filename);
					}
				}
			}

			if (urns.Count <= 0)
				return;

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Parents);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			foreach(DependencyCollectionNode node in dependencies)
			{
				// Check that the dependency is a view that we have scripted out
				if(urns.Contains(node.Urn) && node.Urn.Type == "View")
				{
					string filename = node.Urn.GetAttribute("Schema") + "." + node.Urn.GetAttribute("Name") + ".viw";
					this.fileNames.Add(Path.Combine(relativeDir, filename));
				}
			}
			this.fileNames.AddRange(triggerFileNames);
		}

		private void ScriptStoredProcedures()
		{
			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.ToFileOnly = true;
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;

			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.AppendToFile = true;
			options.Encoding = Encoding;
			options.Permissions = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

			string relativeDir = "Stored Procedures";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if (!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			database.PrefetchObjects(typeof(StoredProcedure), options);
			UrnCollection urns = new UrnCollection();
			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach (StoredProcedure sproc in database.StoredProcedures)
			{
				if (!sproc.IsSystemObject)
				{
					string filename = Path.Combine(relativeDir, sproc.Schema + "." + sproc.Name + ".prc");
					dropOptions.FileName = options.FileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(options.FileName);

					objects[0] = sproc;
					scripter.Options = dropOptions;
					scripter.ScriptWithList(objects);
					scripter.Options = options;
					scripter.ScriptWithList(objects);
					urns.Add(sproc.Urn);
				}
			}

			if (urns.Count <= 0)
				return;

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Parents);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			foreach(DependencyCollectionNode node in dependencies)
			{
				// Check that the dependency is a udf that we have scripted out
				if(urns.Contains(node.Urn) && node.Urn.Type == "StoredProcedure")
				{
					string filename = node.Urn.GetAttribute("Schema") + "." + node.Urn.GetAttribute("Name") + ".prc";
					this.fileNames.Add(Path.Combine(relativeDir, filename));
				}
			}
		}

		private void ScriptUserDefinedFunctions()
		{
			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.ToFileOnly = true;
			dropOptions.Encoding = Encoding;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;

			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.AppendToFile = true;
			options.Encoding = this.Encoding;
			options.Permissions = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.PrefetchObjects = false;

			string relativeDir = "Functions";
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if (!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			database.PrefetchObjects(typeof(UserDefinedFunction), options);
			UrnCollection urns = new UrnCollection();
			SqlSmoObject[] objects = new SqlSmoObject[1];
			foreach (UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if (!udf.IsSystemObject)
				{
					string filename = Path.Combine(relativeDir, udf.Schema + "." + udf.Name + ".udf");
					dropOptions.FileName = options.FileName = Path.Combine(OutputDirectory, filename);
					Console.WriteLine(options.FileName);

					objects[0] = udf;
					scripter.Options = dropOptions;
					scripter.ScriptWithList(objects);
					scripter.Options = options;
					scripter.ScriptWithList(objects);
					urns.Add(udf.Urn);
				}
			}

			if (urns.Count <= 0)
				return;

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Parents);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			foreach(DependencyCollectionNode node in dependencies)
			{
				// Check that the dependency is a udf that we have scripted out
				if(urns.Contains(node.Urn) && node.Urn.Type == "UserDefinedFunction")
				{
					string filename = node.Urn.GetAttribute("Schema") + "." + node.Urn.GetAttribute("Name") + ".udf";
					this.fileNames.Add(Path.Combine(relativeDir, filename));
				}
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
					foreach(string member in role.EnumMembers())
					{
						if(database.Roles.Contains(member))
						{
							writer.WriteLine("sp_addrolemember N'{0}', N'{1}'", role.Name.Replace("'", "''"), member.Replace("'", "''"));
							writer.WriteLine("GO");
						}
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
	}
}
