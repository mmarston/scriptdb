using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Text;

using Microsoft.SqlServer.Management.Smo;
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
			server = new Server(@"mmarston\sql2005");
			//server = new Server(@"tank\sql2005rtm");
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
		public void TestStoredProcedures()
		{
			Database database = server.Databases["SEM_Merchant"];

			ScriptingOptions dropOptions = new ScriptingOptions();
			dropOptions.ToFileOnly = true;
			dropOptions.Encoding = System.Text.Encoding.UTF8;
			dropOptions.IncludeIfNotExists = true;
			dropOptions.ScriptDrops = true;
			
			ScriptingOptions sprocOptions = new ScriptingOptions();
			sprocOptions.ToFileOnly = true;
			sprocOptions.AppendToFile = true;
			sprocOptions.Encoding = System.Text.Encoding.UTF8;
			sprocOptions.Permissions = true;

			Scripter sprocScripter = new Scripter(server);
			sprocScripter.Options = sprocOptions;
			sprocScripter.PrefetchObjects = false;

			if (!Directory.Exists("Stored Procedures"))
				Directory.CreateDirectory("Stored Procedures");

			database.PrefetchObjects(typeof(StoredProcedure), sprocOptions);

			foreach (StoredProcedure sproc in database.StoredProcedures)
			{
				if (!sproc.IsSystemObject)
				{
					string filename = Path.Combine("Stored Procedures", sproc.Schema + "." + sproc.Name + ".prc");
					dropOptions.FileName = filename;
					sprocOptions.FileName = filename;
					sprocScripter.Options = dropOptions;
					sprocScripter.ScriptWithList(new SqlSmoObject[] { sproc });
					sprocScripter.Options = sprocOptions;
					sprocScripter.ScriptWithList(new SqlSmoObject[] { sproc });

				}
			}
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
							Console.WriteLine("sp_addrolemember N'{0}', N'{1}'", role.Name.Replace("'", "''"), member.Replace("'", "''"));
							Console.WriteLine("GO");
						}
					}
				
			}

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
	}
}
