using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
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

		[SetUp]
		public void SetUp()
		{
			server = new Server(@"mmarston");
			//server = new Server(@"tank");
			//server.SetDefaultInitFields(typeof(StoredProcedure), "IsSystemObject");
			//server.SetDefaultInitFields(typeof(UserDefinedFunction), "IsSystemObject");
			//server.SetDefaultInitFields(typeof(View), true);
			//server.SetDefaultInitFields(typeof(Table), true);
		}

		[Test]
		public void Test1()
		{
			Database database = server.Databases["SEM_Admin"];
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
			Database database = server.Databases["Test"];
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
			Database database = server.Databases["Test"];
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
			Database database = server.Databases["SEM_Merchant"];
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
			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			scripter.Script(new SqlSmoObject[] { database });
		}

		[Test]
		public void TestAssemblies()
		{
			Database database = server.Databases["Product_Merchant"];

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

			// do not prefetch assemblies--it doesn't script out the AssemblySecurityLevel!
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
			Database database = server.Databases["SEM_Merchant"];
			Transfer transfer = new Transfer(database);
			transfer.CreateTargetDatabase = true;
			transfer.DestinationDatabase = "$(NEW_DB_NAME)";
			transfer.Options = options;
			transfer.PrefetchObjects = false;
			transfer.ObjectList = new ArrayList();
			transfer.ObjectList.Add(database);
			transfer.CopyAllObjects = false;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TransferTables()
		{
			ScriptingOptions options = new ScriptingOptions();
			options.ToFileOnly = true;
			options.FileName = "TransferTables.sql";

			Database database = server.Databases["SEM_Merchant"];
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

			Database database = server.Databases["SEM_Merchant"];
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
			Database database = server.Databases["SEM_Merchant"];
			Transfer transfer = new Transfer(database);
			transfer.CopyAllObjects = false;
			transfer.CopyAllSqlAssemblies = true;
			transfer.Options = options;
			transfer.ScriptTransfer();
		}

		[Test]
		public void TestTables()
		{
			Database database = server.Databases["SEM_Merchant"];
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

			UrnCollection urns = new UrnCollection();

			ScriptingOptions prefetchOptions = new ScriptingOptions(tableOptions);
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

					urns.Add(table.Urn);
				}
			}

			//DependencyWalker walker = new DependencyWalker(server);
			//DependencyTree tree = walker.DiscoverDependencies(urns, DependencyType.Parents);
			//DependencyCollection dependencies = walker.WalkDependencies(tree);
			//using (TextWriter writer = new StreamWriter(@"Tables\Dependencies.txt"))
			//{
			//    foreach (DependencyCollectionNode node in dependencies)
			//    {
			//        //writer.WriteLine(node.Urn);
			//        writer.WriteLine("r: {0}.{1}.tab", node.Urn.GetAttribute("Schema"), node.Urn.GetAttribute("Name"));
			//    }
			//}

		}

		[Test]
		public void TestViews()
		{
			Database database = server.Databases["SEM_Merchant"];

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
		public void TestTransferDependencies()
		{
			//Database database = server.Databases["Merchant_Prod_Limoges"];
			Database database = server.Databases["Product_Merchant"];
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
			Database database = server.Databases["Product_Merchant"];

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

			//Database database = server.Databases["Merchant_Prod_Limoges"];
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
			Database database = server.Databases["Product_Merchant"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];
			
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
			Database database = server.Databases["Merchant_Prod_Limoges"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];
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
				return "'" + EscapeChar(((SqlXml)val).Value, '\'') +  "'";
			}
			else
			{
				throw new ApplicationException("Unsupported type :" + val.GetType().ToString());
			}
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

		[Test]
		public void TestUdfs()
		{
			Database database = server.Databases["SEM_Merchant"];

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
			Database database = server.Databases["SEM_Merchant"];

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
			Database database = server.Databases["Merchant_Prod_Limoges"];

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
		public void TestPrefetchViewColumns()
		{
			Database database = server.Databases["Merchant_Prod_Limoges"];

			server.SetDefaultInitFields(typeof(Column), true);
			database.PrefetchObjects(typeof(View));
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
			Database database = server.Databases["SEM_Merchant"];

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
			//Database database = server.Databases["SEM_Merchant"];
			//Database database = server.Databases["Design_Datawarehouse"];
			Database database = server.Databases["Dev_Datawarehouse"];


			string fileName = "Roles.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = true;
			options.IncludeIfNotExists = true;

			Scripter scripter = new Scripter(server);
			scripter.Options = options;
			server.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;
			

			List<SqlSmoObject> roleList = new List<SqlSmoObject>();
			foreach (DatabaseRole role in database.Roles)
			{
				//if (!role.IsFixedRole)
					roleList.Add(role);
					foreach(string member in role.EnumMembers())
					{
						if(database.Roles.Contains(member))
						{
							role.AddMember(member);
							foreach(string line in role.Script())
							{
								Console.WriteLine(line);
							}
							role.Alter();
							Console.WriteLine("sp_addrolemember N'{0}', N'{1}'", role.Name.Replace("'", "''"), member.Replace("'", "''"));
							Console.WriteLine("GO");
						}
					}
			}

			Console.WriteLine(server.ConnectionContext.CapturedSql.ToString());

			foreach (ApplicationRole role in database.ApplicationRoles)
			{
				roleList.Add(role);
			}

			foreach (User user in database.Users)
			{
				if (!user.IsSystemObject)
					roleList.Add(user);
			}

			SqlSmoObject[] roles = new SqlSmoObject[roleList.Count];
			roleList.CopyTo(roles);
			
			//DatabaseRole[] roles = new DatabaseRole[database.Roles.Count];
			//database.Roles.CopyTo(roles, 0);

			scripter.Script(roles);
		}

		[Test]
		public void TestTransferRoles()
		{
			Database database = server.Databases["Dev_Datawarehouse"];

			string fileName = "TransferRoles.sql";
			ScriptingOptions options = new ScriptingOptions();
			options.FileName = fileName;
			options.ToFileOnly = true;
			options.Encoding = System.Text.Encoding.UTF8;
			options.Permissions = true;
			options.AllowSystemObjects = false;
			options.IncludeIfNotExists = true;

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
			Database database = server.Databases["SEM_Merchant"];

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
			Database database = server.Databases["SEM_Merchant"];

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
			Database database = server.Databases["Dev_Merchant"];

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
			Database database = server.Databases["SEM_Merchant"];

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
			Database database = server.Databases["Dev_Merchant"];

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
			Database database = server.Databases["Dev_Merchant"];

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
				bool isFirstColumn = true;
				foreach(Column column in table.Columns)
				{
					if(!column.Computed)
					{
						if(!isFirstColumn)
							orderBy.Append(", ");
						else
							isFirstColumn = false;
						orderBy.Append(MakeSqlBracket(column.Name));
					}
				}
			}
			else
			{
				bool isFirstIndexColumn = true;
				foreach(IndexedColumn indexColumn in bestIndex.IndexedColumns)
				{
					if(!indexColumn.IsIncluded)
					{
						if(!isFirstIndexColumn)
							orderBy.Append(", ");
						else
							isFirstIndexColumn = false;
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
							orderBy.Append(MakeSqlBracket(column.Name));
						}
					}
				}
			}
			return orderBy.ToString();
		}


		[Test]
		public void TestScriptDataAsInsert()
		{
			Database database = server.Databases["Dev_Merchant"];
			Table table = database.Tables["AllType"];

			bool hasIdentityColumn = false;
			StringBuilder columnListBuilder = new StringBuilder();
			bool isFirstColumn = true;
			int columnCount = 1;
			foreach(Column column in table.Columns)
			{
				if(!column.Computed && column.DataType.SqlDataType != SqlDataType.Timestamp)
				{
					if(isFirstColumn)
						isFirstColumn = false;
					else
						columnListBuilder.AppendLine(",");
					columnListBuilder.Append('\t', columnCount++);
					columnListBuilder.Append(MakeSqlBracket(column.Name));
					if(column.Identity)
						hasIdentityColumn = true;
				}
			}

			string tableNameWithSchema = String.Format("{0}.{1}", MakeSqlBracket(table.Schema), MakeSqlBracket(table.Name));
			string tableNameWithDatabase = String.Format("{0}.{1}", MakeSqlBracket(database.Name), tableNameWithSchema);
			string columnList = columnListBuilder.ToString();
			string selectClause = String.Format("SELECT\r\n{0}", columnList);
			string fromClause = String.Format("FROM {0}", tableNameWithDatabase);
			string orderByClause = GetOrderByClauseForTable(table);
			string selectCommand = String.Format("{0}\r\n{1}\r\n{2}", selectClause, fromClause, orderByClause);
			
			SqlDataReader reader = server.ConnectionContext.ExecuteReader(selectCommand);
			object[] values = new object[reader.FieldCount];
			using(TextWriter writer = new StreamWriter(table.Name + "_insert.sql"))
			{
				if(hasIdentityColumn)
					writer.WriteLine("SET IDENTITY_INSERT {0} ON", tableNameWithSchema);
				int rowCount = 0;
				int batchSize = 4;
				while(reader.Read())
				{
					if(rowCount % batchSize == 0)
					{
						writer.WriteLine("INSERT {0}\r\n(\r\n{1}\r\n)\r\nSELECT", tableNameWithDatabase, columnList);
					}
					else
					{
						writer.WriteLine("UNION ALL SELECT");
					}
					reader.GetSqlValues(values);
					bool isFirstValue = true;
					columnCount = 1;
					foreach(object val in values)
					{
						if(!isFirstValue)
							writer.WriteLine(",");
						else
							isFirstValue = false;
						writer.Write(new string('\t', columnCount++));

						if(DBNull.Value == val || (val is INullable && ((INullable)val).IsNull))
							writer.Write("NULL");
						else if(val is System.Data.SqlTypes.SqlBinary)
						{
							writer.Write(ByteArrayToHexLiteral(((SqlBinary)val).Value));
						}
						else if(val is System.Data.SqlTypes.SqlBoolean)
						{
							writer.Write(((SqlBoolean)val).Value ? '1' : '0');
						}
						else if(val is System.Data.SqlTypes.SqlBytes)
						{
							writer.Write(ByteArrayToHexLiteral(((SqlBytes)val).Value));
						}
						else if(val is System.Data.SqlTypes.SqlChars)
						{
							writer.Write("'{0}'", new string(((SqlChars)val).Value));
						}
						else if(val is System.Data.SqlTypes.SqlDateTime)
						{
							writer.Write("'{0}'", ((SqlDateTime)val).Value.ToString("yyyy-MM-ddTHH:mm:ss.fff"));
						}
						else if(val is System.Data.SqlTypes.SqlDecimal
							|| val is System.Data.SqlTypes.SqlByte
							|| val is System.Data.SqlTypes.SqlInt16
							|| val is System.Data.SqlTypes.SqlInt32
							|| val is System.Data.SqlTypes.SqlInt64
							|| val is System.Data.SqlTypes.SqlMoney)
						{
							writer.Write(val.ToString());
						}
						else if(val is System.Data.SqlTypes.SqlSingle)
						{
							writer.Write(((SqlSingle)val).Value.ToString("r"));
						}
						else if(val is System.Data.SqlTypes.SqlDouble)
						{
							writer.Write(((SqlDouble)val).Value.ToString("r"));
						}
						else if(val is System.Data.SqlTypes.SqlGuid
							|| val is System.Data.SqlTypes.SqlString)
						{
							writer.Write("'{0}'", val.ToString());
						}
						else if(val is System.Data.SqlTypes.SqlXml)
						{
							writer.Write("'{0}'", ((SqlXml)val).Value);
						}
						else
						{
							throw new ApplicationException("A column was returned as an unsupported type (" + val.GetType().ToString());
						}
					}
					writer.WriteLine();
					rowCount++;
				}
				if(hasIdentityColumn)
					writer.WriteLine("SET IDENTITY_INSERT {0} OFF", tableNameWithSchema);
			}
		}
		[Test]
		public void TestCopyTable()
		{
			Database database = server.Databases["Dev_Merchant"];

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

		
	}
}
