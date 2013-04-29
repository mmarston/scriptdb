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
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Mercent.SqlServer.Management.Upgrade.Schema.Tests
{
	[TestClass]
	public class SchemaUpgradeScripterTest
	{
		static TestContext Context;
		static string SourceDatabaseName;
		static string TargetDatabaseName;
		static string UnitTestServerName;

		private SqlConnection connection;

		#region Initialization and Cleanup methods

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			SchemaUpgradeScripterTest.Context = context;
			UnitTestServerName = Environment.GetEnvironmentVariable("UnitTestSqlServer")
				?? Environment.MachineName;
			string suffix = DateTime.Now.ToString("s");

			SourceDatabaseName = "Source-" + suffix;
			TargetDatabaseName = "Target-" + suffix;
		}

		[TestCleanup]
		public void TestCleanup()
		{
			if(connection != null && connection.State != ConnectionState.Closed)
			{
				DropDatabase(SourceDatabaseName);
				DropDatabase(TargetDatabaseName);
				connection.Close();
			}
		}

		[TestInitialize]
		public void TestInitialize()
		{
			SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder
			{
				DataSource = UnitTestServerName,
				IntegratedSecurity = true,
				ApplicationName = "Mercent.SqlServer.Management.Upgrade.Data.Tests.DataUpgraderTest"
			};
			connection = new SqlConnection(connectionBuilder.ConnectionString);
			connection.Open();
		}

		#endregion Initialization and Cleanup methods

		[TestMethod]
		public void NoChangeTest()
		{
			string[] script1 = { CreateTestTable1 };
			string[] script2 = { CreateTestTable1 };
			ExecuteTest(script1, script2, expectedScript: String.Empty, expectedResult: false);
		}

		[TestMethod]
		public void CreateTableTest()
		{
			string[] script1 = { CreateTestTable1 };
			string[] script2 = { };
			string expectedScript = @"PRINT N'Creating [dbo].[TestTable1]...';


GO
CREATE TABLE [dbo].[TestTable1] (
    [ID]    INT          NOT NULL,
    [Value] VARCHAR (50) NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC),
    CONSTRAINT [AK_TestTable1_Value] UNIQUE NONCLUSTERED ([Value] ASC)
);


GO
";
			ExecuteTest(script1, script2, expectedScript);
		}

		#region Private Methods

		private static SchemaUpgradeScripter GetSchemaUpgradeScripter()
		{
			SchemaUpgradeScripter target = new SchemaUpgradeScripter
			{
				SourceDatabaseName = SourceDatabaseName,
				SourceServerName = UnitTestServerName,
				TargetDatabaseName = TargetDatabaseName,
				TargetServerName = UnitTestServerName
			};
			return target;
		}

		private string ExecuteTest
		(
			IEnumerable<String> sourceScript,
			IEnumerable<String> targetScript,
			string expectedScript = null,
			bool expectedResult = true
		)
		{
			try
			{
				// Create the source database.
				DropAndCreateDatabase(SourceDatabaseName, sourceScript);
			}
			catch(Exception ex)
			{
				throw new Exception("Source database initialization failed.", ex);
			}

			try
			{
				// Create the target database.
				DropAndCreateDatabase(TargetDatabaseName, targetScript);
			}
			catch(Exception ex)
			{
				throw new Exception("Target database initialization failed.", ex);
			}

			var target = GetSchemaUpgradeScripter();
			bool actualResult;
			string actualScript;
			using(StringWriter writer = new StringWriter())
			{
				// Call GenerateScript and verify the result and writer output are as expected.
				actualResult = target.GenerateScript(writer);
				Assert.AreEqual(expectedResult, actualResult, "GenerateScript() return value not as expected (it should be false when there are no schema changes; otherwise, true).");
				actualScript = writer.ToString();
				// If expectedScript is null, then the test did not require us to
				// verify the expected script is an exact string match.
				// Write it out to the output so it can be viewed.
				if(expectedScript == null)
				{
					Console.WriteLine("\r\n\r\n--------------\r\nActual Script:\r\n--------------");
					Console.WriteLine(actualScript);
				}
				else
				{
					// We don't require the test to include the SET ANSI_NULLS... boilerplate in the expected script.
					// Remove it from the actual.
					if
					(
						!expectedScript.StartsWith("SET ANSI_NULLS")
						&& actualScript.StartsWith("SET ANSI_NULLS")
						&& actualScript.Contains("GO\r\n")
					)
						actualScript = actualScript.Substring(actualScript.IndexOf("GO\r\n") + 4);
					Assert.AreEqual(expectedScript, actualScript, "GenerateScript() output script not as expected.");
				}
			}

			// If there is script to run, then run it to verify works properly.
			if(actualResult && actualScript != null)
			{
				string tempFile = Path.GetTempFileName();
				int exitCode;
				try
				{
					// Execute the script to ensure there are no errors in it.
					exitCode = ScriptUtility.RunSqlCmd(UnitTestServerName, TargetDatabaseName, new FileInfo(tempFile));
				}
				catch(Exception ex)
				{
					throw new Exception("The generated script failed to execute properly.", ex);
				}
				finally
				{
					File.Delete(tempFile);
				}
				Assert.AreEqual(0, exitCode, "SqlCmd.exe returned a non-zero exit code indicating that the generated script failed to execute properly.");
			}

			return actualScript;
		}

		#endregion Private Methods

		#region SQL Statements

		readonly string CreateTestTable1 =
@"CREATE TABLE TestTable1
(
	ID int not null PRIMARY KEY,
	Value varchar(50) null CONSTRAINT AK_TestTable1_Value UNIQUE
);";

		#endregion SQL Statements

		#region Private helper methods

		private void DropAndCreateDatabase(string databaseName)
		{
			string command =
@"IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}')
BEGIN
	ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [{0}];
END;

CREATE DATABASE [{0}];";
			command = String.Format(command, databaseName);
			Execute(command);
		}

		private void DropAndCreateDatabase(string databaseName, string batch)
		{
			DropAndCreateDatabase(databaseName);
			connection.ChangeDatabase(databaseName);
			Execute(batch);
		}

		private void DropAndCreateDatabase(string databaseName, IEnumerable<string> script)
		{
			DropAndCreateDatabase(databaseName);
			connection.ChangeDatabase(databaseName);
			Execute(script);
		}

		private void DropDatabase(string databaseName)
		{
			connection.ChangeDatabase("master");
			string command =
@"IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}')
BEGIN
	ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [{0}];
END;";
			command = String.Format(command, databaseName);
			Execute(command);
		}

		private void Execute(string commandText)
		{
			using(SqlCommand command = new SqlCommand(commandText, connection))
			{
				command.ExecuteNonQuery();
			}
		}

		private void Execute(IEnumerable<string> script)
		{
			using(SqlCommand command = new SqlCommand())
			{
				command.Connection = this.connection;
				foreach(string batch in script)
				{
					command.CommandText = batch;
					command.ExecuteNonQuery();
				}
			}
		}

		#endregion Private helper methods
	}
}
