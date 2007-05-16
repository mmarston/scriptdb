using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
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
using NUnit.Framework;

namespace Mercent.SqlServer.Management.Tests
{
	[TestFixture]
	public class ScriptTest
	{
		private Server server;
		private Database database;

		[SetUp]
		public void SetUp()
		{
			SqlConnectionInfo connectionInfo = new SqlConnectionInfo();
			connectionInfo.ServerName = "Tank";
			connectionInfo.DatabaseName = "Product_Admin";
			ServerConnection connection = new ServerConnection(connectionInfo);
			server = new Server(connection);
			server.SetDefaultInitFields
			(
				typeof(FullTextCatalog),
				new string[]
				{
					"IsAccentSensitive",
					"IsDefault"
				}
			);
			StringCollection strings = server.GetPropertyNames(typeof(DatabaseRole));
			foreach(string s in strings)
			{
				Console.WriteLine(s);
			}
			database = server.Databases[connection.DatabaseName];
			//server = new Server(@"tank");
			//server.SetDefaultInitFields(typeof(StoredProcedure), "IsSystemObject");
			//server.SetDefaultInitFields(typeof(UserDefinedFunction), "IsSystemObject");
			//server.SetDefaultInitFields(typeof(View), true);
			//server.SetDefaultInitFields(typeof(Table), true);
		}

		[Test]
		public void Test1()
		{
			Scripter scripter = new Scripter(server);
			DependencyTree dependencyTree = scripter.DiscoverDependencies(new Urn[] { database.Urn }, DependencyType.Parents);
			DependencyCollection dependencyCollection = scripter.WalkDependencies(dependencyTree);
			foreach (DependencyCollectionNode node in dependencyCollection)
			{
				Console.WriteLine(node.Urn);
			}
		}

		[Test]
		public void Test2()
		{
			Scripter scripter = new Scripter(server);
			UrnCollection depencencies = Scripter.EnumDependencies(database.Tables["Table2"], DependencyType.Parents);
			foreach (Urn urn in depencencies)
			{
				Console.WriteLine(urn);
			}
		}

		[Test]
		public void Test3()
		{
			ScriptingOptions options = new ScriptingOptions();
			//options.WithDependencies = true;
			options.ToFileOnly = true;
			options.FileName = "test.sql";
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.Script(new SqlSmoObject[] { database.Views["vwTable1"], database.UserDefinedFunctions["Udf1"] });
		}

		[Test]
		public void TestDatabase()
		{
			
			//server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;
			//database.Rename("$(NEW_DB_NAME)");
			////server.ConnectionContext.CapturedSql.Clear();
			//database.Create();
			//database.Alter();
			////database.DatabaseOptions.
			//foreach(string line in server.ConnectionContext.CapturedSql.Text)
			//{
			//    Console.WriteLine(line);
			//}
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.NoFileGroup = true;
			options.FileName = "database.sql";
			options.IncludeIfNotExists = true;
			options.FullTextCatalogs = true;
			//options.Permissions = true;
			//options.PrimaryObject = false;
			DatabasePermissionInfo[] permissions = database.EnumDatabasePermissions("zirconium_service");


			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			string newDbName = "$(DBNAME)";
			typeof(Database).InvokeMember("ScriptName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.SetProperty, null, database, new string[] { newDbName }, null);

			scripter.Script(new SqlSmoObject[] { database });
		}

		[Test]
		public void TestAssemblies()
		{

			ScriptingOptions assemblyOptions = new ScriptingOptions();
			assemblyOptions.ToFileOnly = true;
			assemblyOptions.Encoding = System.Text.Encoding.UTF8;
			assemblyOptions.Permissions = true;

			Scripter assemblyScripter = new Scripter(server);
			assemblyScripter.Options = assemblyOptions;
			assemblyScripter.PrefetchObjects = false;

			string dir = "Assemblies";
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);
			

			//server.SetDefaultInitFields(typeof(SqlAssembly), true);
			//server.SetDefaultInitFields(true);
			//database.PrefetchObjects(typeof(StoredProcedure), assemblyOptions);
			
			database.PrefetchObjects(typeof(UserDefinedAggregate), assemblyOptions);
			database.PrefetchObjects(typeof(UserDefinedFunction), assemblyOptions);
			database.PrefetchObjects(typeof(UserDefinedType), assemblyOptions);
			database.PrefetchObjects(typeof(SqlAssembly), assemblyOptions);
			server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;
			foreach(SqlAssembly assembly in database.Assemblies)
			{
				AssemblySecurityLevel securityLevel = assembly.AssemblySecurityLevel;
				string filename = Path.Combine(dir, assembly.Name + ".sql");
				assemblyScripter.Options.AppendToFile = false;
				assemblyScripter.Options.FileName = filename;
				assemblyScripter.ScriptWithList(new SqlSmoObject[] { assembly });
				// Check if the assembly is visible.
				// If the assembly is visible then it can have CLR objects.
				// If the assembly is not visible then it is intended to be called from
				// other assemblies.
				if(assembly.IsVisible)
				{
					DependencyTree tree = assemblyScripter.DiscoverDependencies(new SqlSmoObject[] { assembly }, DependencyType.Children);
					if(tree.HasChildNodes && tree.FirstChild.HasChildNodes)
					{
						UrnCollection children = new UrnCollection();
						for(DependencyTreeNode child = tree.FirstChild.FirstChild; child != null; child = child.NextSibling)
						{
							if(child.Urn.Type != "SqlAssembly")
								children.Add(child.Urn);
							Console.WriteLine(child.Urn);
						}
						assemblyScripter.Options.AppendToFile = true;
						assemblyScripter.ScriptWithList(children);
					}
				}
				else
				{
					// The create script doesn't include the VISIBILITY (this appears
					// to be a bug in SQL SMO) so we change it and generate an alter
					// statement.
					assembly.IsVisible = true;
					assembly.IsVisible = false;
					server.ConnectionContext.CapturedSql.Clear();
					assembly.Alter();
					// grab the second string in the collection
					// (the first is a USE statement to set the database context)
					using(TextWriter writer = new StreamWriter(filename, true))
					{
						writer.WriteLine(server.ConnectionContext.CapturedSql.Text[1]);
						writer.WriteLine("GO");
					}

				}
			}

		}

		[Test]
		public void TransferDatabase()
		{
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.FileName = "TransferDatabase.sql";
			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.PrefetchObjects = false;
			transfer.ObjectList = new ArrayList();
			transfer.ObjectList.Add(database);
			transfer.CopyAllObjects = false;
			//transfer.CopySchema = true;
			transfer.DestinationServer = "marston";
			transfer.CreateTargetDatabase = true;
			transfer.DestinationDatabase = "NEW_DB_NAME";
			transfer.ScriptTransfer();
		}

		[Test]
		public void TransferTables()
		{
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.FileName = "TransferTables.sql";

			Transfer transfer = new Transfer(database);
			transfer.CopyAllObjects = false;
			transfer.CopyAllTables = true;
			transfer.Options = options;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TransferTypes()
		{
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.FileName = "TransferTypes.sql";

			Transfer transfer = new Transfer(database);
			transfer.CopyAllObjects = false;
			transfer.CopyAllUserDefinedDataTypes = true;
			transfer.CopyAllUserDefinedTypes = true;
			transfer.Options = options;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TransferAssemblies()
		{
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.FileName = "TransferAssemblies.sql";
			Transfer transfer = new Transfer(database);
			transfer.CopyAllObjects = false;
			transfer.CopyAllSqlAssemblies = true;
			transfer.Options = options;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestTables()
		{
			ScriptingOptions tableOptions = new ScriptingOptions();
			tableOptions.ToFileOnly = true;
			tableOptions.Encoding = System.Text.Encoding.UTF8;
			
			Scripter tableScripter = new Scripter(server);
			tableScripter.Options = tableOptions;
			tableScripter.PrefetchObjects = false;
			
			ScriptingOptions kciOptions = new ScriptingOptions();
			kciOptions.ToFileOnly = true;
			kciOptions.Encoding = System.Text.Encoding.UTF8;
			kciOptions.PrimaryObject = false;
			kciOptions.ClusteredIndexes = true;
			kciOptions.DriChecks = true;
			kciOptions.DriClustered = true;
			kciOptions.DriDefaults = true;
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
			fkyOptions.ToFileOnly = true;
			fkyOptions.Encoding = System.Text.Encoding.UTF8;
			fkyOptions.DriForeignKeys = true;
			fkyOptions.PrimaryObject = false;

			Scripter fkyScripter = new Scripter(server);
			fkyScripter.Options = fkyOptions;
			fkyScripter.PrefetchObjects = false;

			if (!Directory.Exists("Tables"))
				Directory.CreateDirectory("Tables");

			ScriptingOptions prefetchOptions = new ScriptingOptions(tableOptions);
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

			//server.SetDefaultInitFields(typeof(Table), true);
			//database.PrefetchObjects(typeof(Table));
			database.PrefetchObjects(typeof(Table), prefetchOptions);
			
			SqlSmoObject[] objects = new SqlSmoObject[1];
			
			foreach (Table table in database.Tables)
			{
				if (!table.IsSystemObject)
				{
					objects[0] = table;
					Console.WriteLine(table.Urn);
					string filename = Path.Combine("Tables", table.Schema + "." + table.Name + ".tab");
					tableScripter.Options.FileName = filename;
					tableScripter.ScriptWithList(objects);

					kciScripter.Options.FileName = Path.ChangeExtension(filename, ".kci");
					kciScripter.ScriptWithList(objects);

					fkyScripter.Options.FileName = Path.ChangeExtension(filename, ".fky");
					fkyScripter.ScriptWithList(objects);

					if(table.RowCount > 0)
					{
						ScriptDataAsInsert(table);
					}
				}
			}
		}

		[Test]
		public void TestViews()
		{

			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.ToFileOnly = true;
			viewOptions.Encoding = System.Text.Encoding.UTF8;
			viewOptions.Indexes = true;
			viewOptions.Permissions = true;
			viewOptions.Statistics = true;

			Scripter viewScripter = new Scripter(server);
			viewScripter.Options = viewOptions;
			viewScripter.PrefetchObjects = false;

			ScriptingOptions triggerOptions = new ScriptingOptions();
			triggerOptions.ToFileOnly = true;
			triggerOptions.Encoding = System.Text.Encoding.UTF8;
			triggerOptions.PrimaryObject = false;
			triggerOptions.Triggers = true;

			Scripter triggerScripter = new Scripter(server);
			triggerScripter.Options = triggerOptions;
			triggerScripter.PrefetchObjects = false;

			if (!Directory.Exists("Views"))
				Directory.CreateDirectory("Views");

			ScriptingOptions prefetchOptions = new ScriptingOptions();
			prefetchOptions.Indexes = true;
			prefetchOptions.Permissions = true;
			prefetchOptions.Statistics = true;
			prefetchOptions.Triggers = true;

			database.PrefetchObjects(typeof(View), prefetchOptions);

			UrnCollection urns = new UrnCollection();

			foreach (View view in database.Views)
			{
				if (!view.IsSystemObject)
				{
					string filename = Path.Combine("Views", view.Schema + "." + view.Name + ".viw");
					viewScripter.Options.FileName = filename;
					viewScripter.ScriptWithList(new SqlSmoObject[] { view });
					//urns.Add(view.Urn);
					foreach (Trigger trigger in view.Triggers)
					{
						triggerScripter.Options.FileName = Path.Combine("Views", view.Schema + "." + trigger.Name + ".trg");
						triggerScripter.ScriptWithList(new SqlSmoObject[] { trigger });
						urns.Add(trigger.Urn);
					}
				}
			}

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Parents);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			using(TextWriter writer = new StreamWriter(@"Views\Dependencies.txt"))
			{
				foreach(DependencyCollectionNode node in dependencies)
				{
					writer.WriteLine(node.Urn);
					//writer.WriteLine("r: {0}.{1}.tab", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
				}
			}
		}

		[Test]
		public void TestViewsWithSchemaBinding()
		{

			ScriptingOptions viewOptions = new ScriptingOptions();
			viewOptions.ToFileOnly = true;
			viewOptions.Encoding = System.Text.Encoding.UTF8;
			viewOptions.Indexes = true;
			viewOptions.Permissions = true;
			viewOptions.Statistics = true;

			Scripter viewScripter = new Scripter(server);
			viewScripter.Options = viewOptions;
			viewScripter.PrefetchObjects = false;

			ScriptingOptions triggerOptions = new ScriptingOptions();
			triggerOptions.ToFileOnly = true;
			triggerOptions.Encoding = System.Text.Encoding.UTF8;
			triggerOptions.PrimaryObject = false;
			triggerOptions.Triggers = true;

			Scripter triggerScripter = new Scripter(server);
			triggerScripter.Options = triggerOptions;
			triggerScripter.PrefetchObjects = false;

			if(!Directory.Exists("Views"))
				Directory.CreateDirectory("Views");

			ScriptingOptions prefetchOptions = new ScriptingOptions();
			prefetchOptions.Indexes = true;
			prefetchOptions.Permissions = true;
			prefetchOptions.Statistics = true;
			prefetchOptions.Triggers = true;

			database.PrefetchObjects(typeof(View), prefetchOptions);

			UrnCollection schemaBoundViews = new UrnCollection();
			UrnCollection nonSchemaBoundViews = new UrnCollection();
			UrnCollection triggerUrns = new UrnCollection();

			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
				{
					string filename = Path.Combine("Views", view.Schema + "." + view.Name + ".viw");
					viewScripter.Options.FileName = filename;
					viewScripter.ScriptWithList(new SqlSmoObject[] { view });
					if(view.IsSchemaBound)
						schemaBoundViews.Add(view.Urn);
					else
						nonSchemaBoundViews.Add(view.Urn);
					foreach(Trigger trigger in view.Triggers)
					{
						triggerScripter.Options.FileName = Path.Combine("Views", view.Schema + "." + trigger.Name + ".trg");
						triggerScripter.ScriptWithList(new SqlSmoObject[] { trigger });
						triggerUrns.Add(trigger.Urn);
					}
				}
			}

			if(schemaBoundViews.Count > 0)
			{
				DependencyWalker walker = new DependencyWalker(server);
				DependencyTree tree = walker.DiscoverDependencies(schemaBoundViews, DependencyType.Parents);
				DependencyCollection dependencies = walker.WalkDependencies(tree);
				using(TextWriter writer = new StreamWriter(@"Views\Dependencies.txt"))
				{
					foreach(DependencyCollectionNode node in dependencies)
					{
						if(schemaBoundViews.Contains(node.Urn))
							writer.WriteLine(node.Urn);
						//writer.WriteLine("r: {0}.{1}.tab", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
					}
				}
			}
		}

		[Test]
		public void TestTransferDependencies()
		{
			Transfer transfer = new Transfer(database);
			transfer.Options.WithDependencies = true;
			transfer.CopyAllObjects = false;
			transfer.CopyAllViews = true;
			transfer.CopyAllUserDefinedFunctions = true;
			UrnCollection urns = transfer.EnumObjects();
			foreach(Urn urn in urns)
			{
				if(urn.Type == "UserDefinedFunction" || urn.Type == "View")
					Console.WriteLine("{0} ({1})", urn.GetNameForType(urn.Type), urn.Type);
			}
		}

		[Test]
		public void TestTransferDependencies2()
		{

			ArrayList objects = new ArrayList();
			database.PrefetchObjects(typeof(UserDefinedFunction));
			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(!udf.IsSystemObject)
				{
					objects.Add(udf);
				}
			}
			database.PrefetchObjects(typeof(View));
			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
				{
					objects.Add(view);
				}
			}

			Transfer transfer = new Transfer(database);
			transfer.Options.WithDependencies = true;
			transfer.CopyAllObjects = false;
			transfer.ObjectList = objects;

			UrnCollection urns = transfer.EnumObjects();
			foreach(Urn urn in urns)
			{
				if(urn.Type == "UserDefinedFunction" || urn.Type == "View")
					Console.WriteLine("{0} ({1})", urn.GetNameForType(urn.Type), urn.Type);
			}
		}

		[Test]
		public void TestDiscoverDependencies()
		{

			UrnCollection urns = new UrnCollection();
			database.PrefetchObjects(typeof(UserDefinedFunction));
			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(!udf.IsSystemObject)
				{
					urns.Insert(0, udf.Urn);
				}
			}
			database.PrefetchObjects(typeof(View));
			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
				{
					urns.Insert(0, view.Urn);
				}
			}
			

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Children);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			for(int i = dependencies.Count - 1; i >= 0; i--)
			{
				Urn urn = dependencies[i].Urn;
				if(urn.Type == "UserDefinedFunction" || urn.Type == "View")
					Console.WriteLine("{0} ({1})", urn.GetNameForType(urn.Type), urn.Type);
			}
		}

		[Test]
		public void TestXmlSchemaCollections()
		{

			if(!Directory.Exists("XmlSchemaCollections"))
				Directory.CreateDirectory("XmlSchemaCollections");
			
			database.PrefetchObjects(typeof(XmlSchemaCollection));

			StringBuilder sb = new StringBuilder();

			XmlWriterSettings writerSettings = new XmlWriterSettings();
			writerSettings.ConformanceLevel = ConformanceLevel.Fragment;
			writerSettings.NewLineOnAttributes = true;
			writerSettings.Encoding = Encoding.UTF8;
			writerSettings.Indent = true;
			writerSettings.IndentChars = "\t";

			XmlReaderSettings readerSettings = new XmlReaderSettings();
			readerSettings.ConformanceLevel = ConformanceLevel.Fragment;
						
			foreach(XmlSchemaCollection xmlSchemaCollection in database.XmlSchemaCollections)
			{
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
				string sqlFilename = Path.Combine("XmlSchemaCollections", xmlSchemaCollection.Schema + "." + xmlSchemaCollection.Name + ".sql");
				using(TextWriter writer = new StreamWriter(sqlFilename, false, Encoding.UTF8))
				{
					writer.WriteLine("CREATE XML SCHEMA COLLECTION {0}.{1} AS N'", MakeSqlBracket(xmlSchemaCollection.Schema), MakeSqlBracket(xmlSchemaCollection.Name));
					writer.WriteLine(sb.ToString());
					writer.WriteLine("'");
					writer.WriteLine("GO");
				}
			}
		}

		[Test]
		public void TestServiceBrokerMessageTypes()
		{
			
			if(!Directory.Exists("ServiceBroker"))
				Directory.CreateDirectory("ServiceBroker");

			UrnCollection urns = new UrnCollection();
			foreach(MessageType messageType in database.ServiceBroker.MessageTypes)
			{
				// this is a hack to only get user defined message types, not built in ones
				if(messageType.ID >= 65536)
				{
					urns.Add(messageType.Urn);
				}
			}
			
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.FileName = @"ServiceBroker\MessageTypes.sql";
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			
			scripter.ScriptWithList(urns);
		}

		[Test]
		public void TestServiceBrokerContracts()
		{

			if(!Directory.Exists("ServiceBroker"))
				Directory.CreateDirectory("ServiceBroker");
			
			UrnCollection urns = new UrnCollection();
			foreach(ServiceContract contract in database.ServiceBroker.ServiceContracts)
			{
				// this is a hack to only get user defined contracts, not built in ones
				if(contract.ID >= 65536)
				{
					urns.Add(contract.Urn);
				}
			}

			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.FileName = @"ServiceBroker\Contracts.sql";
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			
			scripter.ScriptWithList(urns);
		}

		[Test]
		public void TestServiceBrokerQueues()
		{

			if(!Directory.Exists("ServiceBroker"))
				Directory.CreateDirectory("ServiceBroker");

			List<int> systemQueueIds = new List<int>();
			string sqlCommand = String.Format("select object_id from {0}.sys.service_queues WHERE is_ms_shipped = 1 ORDER BY object_id", MakeSqlBracket(database.Name));
			using(SqlDataReader reader = server.ConnectionContext.ExecuteReader(sqlCommand))
			{
				while(reader.Read())
				{
					systemQueueIds.Add(reader.GetInt32(0));
				}
			}
			sqlCommand = String.Format("USE {0}", MakeSqlBracket(database.Name));
			server.ConnectionContext.ExecuteNonQuery(sqlCommand);
			UrnCollection urns = new UrnCollection();
			foreach(ServiceQueue queue in database.ServiceBroker.Queues)
			{
				if(systemQueueIds.BinarySearch(queue.ID) < 0)
				{
					urns.Add(queue.Urn);
				}
			}

			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.FileName = @"ServiceBroker\Queues.sql";
			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			scripter.ScriptWithList(urns);
		}

		[Test]
		public void TestServiceBrokerServices()
		{

			if(!Directory.Exists("ServiceBroker"))
				Directory.CreateDirectory("ServiceBroker");

			UrnCollection urns = new UrnCollection();
			foreach(BrokerService service in database.ServiceBroker.Services)
			{
				if(service.ID >= 65536)
				{
					urns.Add(service.Urn);
				}
			}

			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.FileName = @"ServiceBroker\Services.sql";
			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			scripter.ScriptWithList(urns);
		}

		[Test]
		public void TestStoredProcedures()
		{

			ScriptingOptions dropOptions = new ScriptingOptions();
			//dropOptions.ToFileOnly = true;
			dropOptions.Encoding = System.Text.Encoding.UTF8;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;
			
			ScriptingOptions sprocOptions = new ScriptingOptions();
			//sprocOptions.ToFileOnly = true;
			//sprocOptions.AppendToFile = true;
			sprocOptions.Encoding = System.Text.Encoding.UTF8;
			sprocOptions.Permissions = true;

			Scripter sprocScripter = new Scripter(server);
			sprocScripter.Options = sprocOptions;
			sprocScripter.PrefetchObjects = false;
			sprocOptions.PrimaryObject = false;

			if (!Directory.Exists("Stored Procedures"))
				Directory.CreateDirectory("Stored Procedures");

			database.PrefetchObjects(typeof(StoredProcedure), sprocOptions);

			foreach (StoredProcedure sproc in database.StoredProcedures)
			{
				if (!sproc.IsSystemObject && sproc.ImplementationType == ImplementationType.TransactSql)
				{
					string filename = Path.Combine("Stored Procedures", sproc.Schema + "." + sproc.Name + ".prc");
					//dropOptions.FileName = filename;
					//sprocOptions.FileName = filename;
					sprocScripter.Options = dropOptions;
					using(TextWriter writer = new StreamWriter(filename, false))
					{
						StringCollection batches = sprocScripter.ScriptWithList(new SqlSmoObject[] { sproc });
						foreach(string batch in batches)
						{
							writer.WriteLine(batch.Trim());
							writer.WriteLine("GO");
						}
						sprocScripter.Options = sprocOptions;

						//batches = sprocScripter.ScriptWithList(new SqlSmoObject[] { sproc });
						batches = sproc.Script(sprocOptions);
						foreach(string batch in batches)
						{
							writer.WriteLine(batch.Trim());
							writer.WriteLine("GO");
						}
					}

				}
			}
		}

		[Test]
		public void TestParameterDefaultValue()
		{
			database.PrefetchObjects(typeof(StoredProcedure));
			StoredProcedure procedure = database.StoredProcedures["BulkEditProduct2"];

			string sqlCommand = String.Format("SELECT parameter_id, default_value FROM {0}.sys.parameters WHERE [object_id] = {1} AND has_default_value = 1",
				MakeSqlBracket(database.Name), procedure.ID);
			object[] defaults = new object[procedure.Parameters.Count];
			using(SqlDataReader reader = server.ConnectionContext.ExecuteReader(sqlCommand))
			{
				while(reader.Read())
				{
					defaults[reader.GetInt32(0) - 1] = reader.GetSqlValue(1);
				}
			}
			for(int i = 0; i < defaults.Length; i++)
			{
				if(defaults[i] != null)
				{
					procedure.Parameters[i].DefaultValue = GetSqlLiteral(defaults[i]);
				}
			}

			server.ConnectionContext.ExecuteNonQuery("Use " + database.Name);
			//server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;
			//procedure.Alter();
			//StringCollection batches = server.ConnectionContext.CapturedSql.Text;
			Scripter scripter = new Scripter(server);
			StringCollection batches = scripter.ScriptWithList(new Urn[] { procedure.Urn });
			
			//StringCollection batches  = procedure.Script();
			foreach(string batch in batches)
			{
				Console.WriteLine(batch);
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
		            return "N'" + EscapeChar(((SqlXml)sqlValue).Value, '\'') + "'";
				default:
					throw new ApplicationException("Unsupported type :" + sqlDataType.ToString());
		    }
		}

		public string GetSqlLiteral(object val)
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
				return ((SqlDateTime)val).Value.ToString("'yyyy-MM-dd HH:mm:ss.fff'");
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

		[Test]
		public void TestUdfs()
		{

			ScriptingOptions udfOptions = new ScriptingOptions();
			udfOptions.ToFileOnly = true;
			udfOptions.Encoding = System.Text.Encoding.UTF8;
			udfOptions.Permissions = true;

			Scripter udfScripter = new Scripter(server);
			udfScripter.Options = udfOptions;
			udfScripter.PrefetchObjects = false;

			string dir = "Functions";
			if (!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			database.PrefetchObjects(typeof(UserDefinedFunction), udfOptions);

			foreach (UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if (!udf.IsSystemObject)
				{
					string filename = Path.Combine(dir, udf.Schema + "." + udf.Name + ".udf");
					udfScripter.Options.FileName = filename;
					udfScripter.ScriptWithList(new SqlSmoObject[] { udf });
				}
			}
		}

		[Test]
		public void TestAlterUdfs()
		{

			ScriptingOptions udfOptions = new ScriptingOptions();
			udfOptions.Encoding = System.Text.Encoding.UTF8;
			udfOptions.Permissions = true;

			Scripter udfScripter = new Scripter(server);
			udfScripter.Options = udfOptions;
			udfScripter.PrefetchObjects = false;
			
			string dir = "Functions";
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			database.PrefetchObjects(typeof(UserDefinedFunction), udfOptions);
			udfOptions.PrimaryObject = false;

			foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
			{
				if(!udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
				{
					string filename = Path.Combine(dir, udf.Schema + "." + udf.Name + ".udf");
					StringCollection script = udfScripter.ScriptWithList(new SqlSmoObject[] { udf });
					
					//server.ConnectionContext.CapturedSql.Clear();
					//server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;
					//udf.ScriptHeader(true);
					//StringCollection script = server.ConnectionContext.CapturedSql.Text;
					using(TextWriter writer = new StreamWriter(filename, false, Encoding.UTF8))
					{
						writer.Write("SET ANSI_NULLS ");
						writer.WriteLine(udf.AnsiNullsStatus ? "ON" : "OFF");
						writer.WriteLine("GO");
						writer.Write("SET QUOTED_IDENTIFIER ");
						writer.WriteLine("GO");
						writer.WriteLine(udf.QuotedIdentifierStatus ? "ON" : "OFF");
						writer.WriteLine("GO");
						writer.WriteLine(udf.ScriptHeader(true).Trim());
						writer.WriteLine(udf.TextBody.Trim());
						writer.WriteLine("GO");
						foreach(string batch in script)
						{
							string trimmedBatch = batch.Trim();
							writer.WriteLine(trimmedBatch);
							writer.WriteLine("GO");
						}
						
					}
				}
			}
		}

		[Test]
		public void TestUdfHeaders()
		{

			string dir = "Functions";
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			database.PrefetchObjects(typeof(UserDefinedFunction));
			string filename = Path.Combine(dir, "Functions.sql");
					
			using(TextWriter writer = new StreamWriter(filename, false, Encoding.UTF8))
			{
				foreach(UserDefinedFunction udf in database.UserDefinedFunctions)
				{
					if(!udf.IsSystemObject && udf.ImplementationType == ImplementationType.TransactSql)
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
										writer.Write("CAST(NULL AS {0}) AS {1}", dataTypeAsString, column.Name);
									else
										writer.Write("CAST(NULL AS {0}) COLLATE {1} AS {2}", dataTypeAsString, column.Collation, column.Name);
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
			}
		}

		[Test]
		public void TestViewHeaders()
		{

			string dir = "Views";
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(dir, "Views.sql");
			Console.WriteLine(fileName);




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

			server.SetDefaultInitFields(typeof(View),
				"IsSystemObject");

			foreach(View view in database.Views)
			{
				if(!view.IsSystemObject)
				{
					int count = view.Columns.Count;
				}
			}

			database.PrefetchObjects(typeof(View));

			using(TextWriter writer = new StreamWriter(fileName, false, Encoding.UTF8))
			{
				//View view = database.Views["vwAlltype"];
				foreach(View view in database.Views)
				{
					if(!view.IsSystemObject)
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
			}
		}

		[Test]
		public void TestGetColumnPropertyNames()
		{
			foreach(string propertyName in server.GetPropertyNames(typeof(Column)))
				Console.WriteLine(propertyName);
		}

		[Test]
		public void TestGetStoredProcedureParameterPropertyNames()
		{
			foreach(string propertyName in server.GetPropertyNames(typeof(StoredProcedureParameter)))
				Console.WriteLine(propertyName);
		}
		[Test]
		public void TestGetStoredProcedurePropertyNames()
		{
			foreach(string propertyName in server.GetPropertyNames(typeof(StoredProcedure)))
				Console.WriteLine(propertyName);
		}


		[Test]
		public void TestGetViewPropertyNames()
		{
			foreach(string propertyName in server.GetPropertyNames(typeof(View)))
				Console.WriteLine(propertyName);
		}

		[Test]
		public void TestPrefetchClrStoredProcedureParameters()
		{
			server.SetDefaultInitFields(typeof(StoredProcedureParameter),
				"DataType",
				"DataTypeSchema",
				"ID",
				"IsOutputParameter",
				"Length",
				"NumericPrecision",
				"NumericScale",
				"SystemType",
				"XmlDocumentConstraint",
				"XmlSchemaNamespace",
				"XmlSchemaNamespaceSchema");
			
			server.SetDefaultInitFields(typeof(StoredProcedure),
				"ImplementationType",
				"IsSystemObject");
			
			string bracketedDatabaseName = MakeSqlBracket(database.Name);

			foreach(StoredProcedure procedure in database.StoredProcedures)
			{
				if(!procedure.IsSystemObject && procedure.ImplementationType == ImplementationType.SqlClr)
				{
					procedure.Parameters.Refresh();
				}
			}

			database.PrefetchObjects(typeof(StoredProcedure));

			string sqlCommand = "SELECT o.[object_id], parameter_id, default_value\r\n"
				+ "FROM " + bracketedDatabaseName + ".sys.objects AS o\r\n"
				+ "\tJOIN " + bracketedDatabaseName + ".sys.parameters AS p ON p.[object_id] = o.[object_id]\r\n"
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


		[Test]
		public void TestPrefetchViewColumns()
		{

			server.SetDefaultInitFields(typeof(Column), true);
		//	database.PrefetchObjects(typeof(View));
			//server.SetDefaultInitFields(typeof(View), new string[] { "Columns" });
			//database.PrefetchObjects(typeof(Column));
			foreach(View view in database.Views)
			{
				//DataTable dataTable = view.EnumColumns();
				//foreach(DataColumn column in dataTable.Columns)
				//{
				//    Console.WriteLine(column.ColumnName);
				//}
				//foreach(

				//view.ReturnsViewMetadata = true;
				//view.Refresh(true);
				foreach(Column column in view.Columns)
				{
					Console.WriteLine("{0} {1}", column.Name, column.DataType);
				}
			}
		}

		[Test]
		public void TestUdts()
		{

			ScriptingOptions udtOptions = new ScriptingOptions();
			udtOptions.ToFileOnly = true;
			udtOptions.Encoding = System.Text.Encoding.UTF8;
			udtOptions.Permissions = true;

			Scripter udtScripter = new Scripter(server);
			udtScripter.Options = udtOptions;
			udtScripter.PrefetchObjects = false;

			//string dir = "Types";
			//if(!Directory.Exists(dir))
			//	Directory.CreateDirectory(dir);

			//database.PrefetchObjects(typeof(UserDefinedDataType), udtOptions);
			database.PrefetchObjects(typeof(UserDefinedType), udtOptions);
			SqlSmoObject[] udts = new SqlSmoObject[database.UserDefinedDataTypes.Count + database.UserDefinedTypes.Count];
			int i = 0;
			for(; i < database.UserDefinedDataTypes.Count; i++)
			{
				udts[i] = database.UserDefinedDataTypes[i];
			}
			for(int i2 = 0; i2 < database.UserDefinedTypes.Count; i2++, i++)
			{
				udts[i] = database.UserDefinedTypes[i2];
			}
			udtScripter.Options.FileName = "Types.sql";
			udtScripter.ScriptWithList(udts);

			DependencyWalker walker = new DependencyWalker(server);
			DependencyTree tree = walker.DiscoverDependencies(udts, DependencyType.Parents);
			DependencyCollection dependencies = walker.WalkDependencies(tree);
			foreach(DependencyCollectionNode node in dependencies)
			{
				Console.WriteLine(node.Urn);
			}
		}

		[Test]
		public void TestUsersAndRoles()
		{


			string fileName = "Roles.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			//options.Permissions = true;
			//options.AllowSystemObjects = true;
			options.IncludeIfNotExists = true;
			options.IncludeDatabaseRoleMemberships = true;
			
			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			//server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;
			//database.PrefetchObjects(typeof(DatabaseRole));
			//server.SetDefaultInitFields(typeof(DatabaseRole), "Members");

			
			List<SqlSmoObject> roleList = new List<SqlSmoObject>();
			foreach(DatabaseRole role in database.Roles)
			{
				if(!role.IsFixedRole)
				{
					roleList.Add(role);
					//foreach(string member in role.EnumMembers())
					//{

					//    //if(database.Roles.Contains(member))
					//    //{
					//    //    role.AddMember(member);
					//    //    foreach(string line in role.Script())
					//    //    {
					//    //        Console.WriteLine(line);
					//    //    }
					//    //    role.Alter();
					//    Console.WriteLine("sp_addrolemember N'{0}', N'{1}'", role.Name.Replace("'", "''"), member.Replace("'", "''"));
					//    //    Console.WriteLine("GO");
					//    //}
					//}
				}
			}
			
			//Console.WriteLine(server.ConnectionContext.CapturedSql.ToString());

			//foreach (ApplicationRole role in database.ApplicationRoles)
			//{
			//    roleList.Add(role);
			//}

			//foreach (User user in database.Users)
			//{
			//    if (!user.IsSystemObject)
			//        roleList.Add(user);
			//}

			SqlSmoObject[] roles = new SqlSmoObject[roleList.Count];
			roleList.CopyTo(roles);
			
			
			scripter.Script(roles);

			//options.IncludeDatabaseRoleMemberships = true;
			//options.PrimaryObject = false;
			//options.IncludeIfNotExists = false;
			//options.AppendToFile = true;
			//scripter.Script(roles);
			StringCollection permissionScript = new StringCollection();
			Type databaseType = typeof(Database);
			options.Permissions = true;

			databaseType.InvokeMember("AddScriptPermission", BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.NonPublic, null, database, new object[] { permissionScript, options });
			using(TextWriter writer = new StreamWriter("Roles.sql", true))
			{
				foreach(string line in permissionScript)
				{
					writer.WriteLine(line);
				}
			}
			
		}

		[Test]
		public void TestTransferRoles()
		{

			string fileName = "TransferRoles.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;
			options.IncludeDatabaseRoleMemberships = true;
			options.WithDependencies = true;

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.CopyAllObjects = false;
			//transfer.CopyAllUsers = true;
			transfer.CopyAllRoles = true;
			//transfer.CopyAllSchemas = true;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestSchemas()
		{

			string fileName = "Schemas.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;

			Schema[] schemas = new Schema[database.Schemas.Count];
			database.Schemas.CopyTo(schemas, 0);

			scripter.Script(schemas);
		}

		[Test]
		public void TestTransferSchemas()
		{

			string fileName = "TransferSchemas.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			//options.WithDependencies = true;
			//options.IncludeIfNotExists = true;

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			//transfer.CopyAllObjects = false;
			//transfer.CopyAllUsers = true;
			//transfer.CopyAllRoles = true;
			//transfer.CopyAllSchemas = true;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestTransferSynonyms()
		{

			string fileName = "TransferSynonyms.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			//options.WithDependencies = true;
			//options.IncludeIfNotExists = true;

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.CopyAllObjects = false;
			transfer.CopyAllSynonyms = true;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestTransferData()
		{

			string fileName = "TransferData.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = false;

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.CopyAllObjects = true;
			transfer.CopyData = true;
			//transfer.CopyAllUsers = true;
			//transfer.CopyAllRoles = true;
			//transfer.CopyAllSchemas = true;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestTransferPartitionFunctions()
		{

			string fileName = "TransferPartitionFunctions.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.AllowSystemObjects = false;

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.CopyAllObjects = false;
			transfer.CopyAllPartitionFunctions = true;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestTransferPartitionSchemes()
		{

			string fileName = "TransferPartitionSchemes.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.AllowSystemObjects = false;

			Transfer transfer = new Transfer(database);
			transfer.Options = options;
			transfer.CopyAllObjects = false;
			transfer.CopyAllPartitionSchemes = true;
			transfer.ScriptTransfer();
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

		private string GetOrderByClauseForTable(Table table)
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
						if(columnDelimiter!= null)
							orderBy.Append(columnDelimiter);
						else
							columnDelimiter = ", ";
						orderBy.Append(MakeSqlBracket(column.Name));
					}
				}
			}
			else
			{
				string columnDelimiter = null;
				foreach(IndexedColumn indexColumn in bestIndex.IndexedColumns)
				{
					if(!indexColumn.IsIncluded)
					{
						if(columnDelimiter != null)
							orderBy.Append(columnDelimiter);
						else
							columnDelimiter = ", ";
						orderBy.Append(MakeSqlBracket(indexColumn.Name));
						if(indexColumn.Descending)
							orderBy.Append(" DESC");
					}
				}
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

		public void ScriptDataAsInsert(Table table)
		{
			int batchSize = 4;
			bool hasIdentityColumn = false;
			StringBuilder selectColumnListBuilder = new StringBuilder();
			StringBuilder insertColumnListBuilder = new StringBuilder();
			string columnDelimiter = null;
			IDictionary<int, SqlDataType> readerColumnsSqlDataType = new SortedList<int, SqlDataType>(table.Columns.Count);
			int columnCount = 0;
			int columnOrdinal;
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
					columnOrdinal = columnCount++;
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
			string orderByClause = GetOrderByClauseForTable(table);
			string selectCommand = String.Format("{0}\r\n{1}\r\n{2}", selectClause, fromClause, orderByClause);

			using(SqlConnection connection = new SqlConnection(server.ConnectionContext.ConnectionString))
			{
				SqlCommand command = new SqlCommand(selectCommand, connection);
				connection.Open();
				using(SqlDataReader reader = command.ExecuteReader())
				{
					object[] values = new object[reader.FieldCount];
					using(TextWriter writer = new StreamWriter(table.Name + "_insert.sql"))
					{
						if(hasIdentityColumn)
							writer.WriteLine("SET IDENTITY_INSERT {0} ON;\r\nGO", tableNameWithSchema);
						int rowCount = 0;
						while(reader.Read())
						{
							if(rowCount % batchSize == 0)
							{
								if(rowCount != 0)
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
		}

		[Test]
		public void TestScriptDataAsInsert()
		{
			Table table = database.Tables["AllType"];
			ScriptDataAsInsert(table);
		}

		[Test]
		public void TestCopyTable()
		{

			database.PrefetchObjects(typeof(Table)); 
			Table productTable = database.Tables["Product"];
			Table editedProductTable = database.Tables["EditedProduct"];
			if(editedProductTable == null)
				editedProductTable = new Table(database, "EditedProduct");

			
			int columnIndex = 0;
			string previousColumnName = null;
			foreach(Column column in productTable.Columns)
			{
				Column editedColumn = editedProductTable.Columns[column.Name];
				if(editedColumn == null)
				{
					editedProductTable.Columns.Add(new Column(editedProductTable, column.Name, column.DataType), previousColumnName);
				}
				else if(!(editedColumn.DataType.Name == column.DataType.Name))
				{
					editedColumn.DataType = column.DataType;
				}
				columnIndex++;
				previousColumnName = column.Name;
			}
			
			

			// remove extra columns
			while(columnIndex < editedProductTable.Columns.Count)
			{
				editedProductTable.Columns.Remove(editedProductTable.Columns[columnIndex]);
			}

			switch(editedProductTable.State)
			{
				case SqlSmoState.Creating:
					editedProductTable.Create();
					break;
				case SqlSmoState.Pending:
				case SqlSmoState.Existing:
					editedProductTable.Alter();
					break;
			}
		}

		[Test]
		public void TestSqlTypeFormats()
		{
			Assert.AreEqual("9999.00", new SqlMoney(9999m).ToString());
			Assert.AreEqual("9999.90", new SqlMoney(9999.9m).ToString());
			Assert.AreEqual("9999.90", new SqlMoney(9999.90m).ToString());
			Assert.AreEqual("9999.99", new SqlMoney(9999.99m).ToString());
			Assert.AreEqual("9999.9999", new SqlMoney(9999.9999m).ToString());
			Assert.AreEqual("10000.00", new SqlMoney(9999.99999m).ToString());

			Assert.AreEqual("9999", new SqlDecimal(9999m).ToString());
			Assert.AreEqual("9999.9", new SqlDecimal(9999.9m).ToString());
			Assert.AreEqual("9999.90", new SqlDecimal(9999.90m).ToString());
			Assert.AreEqual("9999.99", new SqlDecimal(9999.99m).ToString());
			Assert.AreEqual("3333.3333333333333333333333333", new SqlDecimal(10000m / 3m).ToString());

			Assert.AreEqual("True", new SqlBoolean(true).ToString());
			Assert.AreEqual("False", new SqlBoolean(false).ToString());

			Assert.AreEqual("9999", new SqlInt32(9999).ToString());

			Assert.AreEqual("9/11/2006 12:00:00 AM", new SqlDateTime(2006, 9, 11).ToString());
			Assert.AreEqual("12/1/2006 12:00:00 AM", new SqlDateTime(2006, 12, 1).ToString());

			Assert.AreEqual("99", new SqlByte(99).ToString());

			Assert.AreEqual("9999", new SqlDouble(9999d).ToString());
			Assert.AreEqual("9999.9", new SqlDouble(9999.9d).ToString());
			Assert.AreEqual("9999.99", new SqlDouble(9999.99d).ToString());
			Assert.AreEqual("9999.999", new SqlDouble(9999.999d).ToString());
			Assert.AreEqual("9999.9999", new SqlDouble(9999.9999d).ToString());

			Assert.AreEqual("9999", new SqlDouble(9999f).ToString());
			Assert.AreEqual("9999.900390625", new SqlDouble(9999.9f).ToString());
			Assert.AreEqual("9999.990234375", new SqlDouble(9999.99f).ToString());
			Assert.AreEqual("9999.9990234375", new SqlDouble(9999.999f).ToString());
			Assert.AreEqual("10000", new SqlDouble(9999.9999f).ToString());
		}

		[Test]
		public void TestClrTypeFormats()
		{
			Assert.AreEqual("True", true.ToString());
			Assert.AreEqual("False", false.ToString());

			Assert.AreEqual("99", ((byte)99).ToString());

			Assert.AreEqual("8/9/2007", new DateTime(2007, 8, 9).ToString("M/d/yyyy", DateTimeFormatInfo.InvariantInfo));

			Assert.AreEqual("8/9/2007 1:02 PM", new DateTime(2007, 8, 9, 13, 2, 3).ToString("M/d/yyyy h:mm tt", DateTimeFormatInfo.InvariantInfo));

			string customCurrencyFormat = "#,###.00";
			Assert.AreEqual("9,999.00", 9999M.ToString(customCurrencyFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual(".99", .99M.ToString(customCurrencyFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("99.99", 99.99M.ToString(customCurrencyFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("-9,999.99", (-9999.99M).ToString(customCurrencyFormat, NumberFormatInfo.InvariantInfo));


			string customDecimalFormat = "#,###.####";
			Assert.AreEqual("9,999", 9999M.ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual(".99", .99M.ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("99.99", 99.9900M.ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("99.999", 99.9990M.ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("99.9999", 99.9999M.ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("100", 99.99999M.ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("-9,999.99", (-9999.99M).ToString(customDecimalFormat, NumberFormatInfo.InvariantInfo));


			Assert.AreEqual("9,999", ((Int16)9999).ToString("#,###", NumberFormatInfo.InvariantInfo));
			Assert.AreEqual("-9,999", (-(Int16)9999).ToString("#,###", NumberFormatInfo.InvariantInfo));

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
	}
}

