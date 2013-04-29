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
using System.Text.RegularExpressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Mercent.SqlServer.Management.Upgrade.Data.Tests
{
	[TestClass]
	public class DataUpgraderTest
	{
		static string SourceDatabaseName;
		static string TargetDatabaseName;
		static string UnitTestServerName;

		private SqlConnection connection;

		/// Tests to add:
		
		#region Initialization and Cleanup methods

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
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
		public void CompositeKeyDeleteTest()
		{
			string[] script1 = { CreateCompositeKeyTable };
			string[] script2 = { CreateCompositeKeyTable, InsertCompositeKeyTable_1A_one };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[CompositeKey].';
GO
DELETE FROM [dbo].[CompositeKey] WHERE [Key1] = 1
	AND [Key2] = 'A';
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void CompositeKeyInsertTest()
		{
			string[] script1 = { CreateCompositeKeyTable, InsertCompositeKeyTable_1A_one };
			string[] script2 = { CreateCompositeKeyTable };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[CompositeKey].';
GO
INSERT INTO [dbo].[CompositeKey] ([Key1], [Key2], [Value])
VALUES (1, 'A', 'one');
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void CompositeKeyUpdateTest()
		{
			string[] script1 = { CreateCompositeKeyTable, InsertCompositeKeyTable_1A_uno };
			string[] script2 = { CreateCompositeKeyTable, InsertCompositeKeyTable_1A_one };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[CompositeKey].';
GO
UPDATE [dbo].[CompositeKey]
SET [Value] = 'uno'
WHERE [Key1] = 1
	AND [Key2] = 'A';
GO
";
			ExecuteTest(script1, script2, expected);
		}

		/// <summary>
		/// Test to ensure the rows are deleted, update, then inserted, in that order.
		/// </summary>
		[TestMethod]
		public void DeleteUpdateInsertTest()
		{
			// There is a UNIQUE constraint on the Value column.
			// Row ID 1 must be deleted before row ID 2 can have the value set to 'A'.
			// Similarly, the value of row ID 2 must be updated before row ID 3
			// can be inserted with value 'B'.
			string[] script1 =
			{
				CreateTestTable1,
				@"INSERT TestTable1(ID, Value)
				VALUES
				(2, 'A'),
				(3, 'B')"
			};
			string[] script2 =
			{
				CreateTestTable1,
				@"INSERT TestTable1(ID, Value)
				VALUES
				(1, 'A'),
				(2, 'B')"
			};
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [ID] = 1;
GO
PRINT 'Updating 1 row(s) in [dbo].[TestTable1].';
GO
UPDATE [dbo].[TestTable1]
SET [Value] = 'A'
WHERE [ID] = 2;
GO
PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (3, 'B');
GO
";
			ExecuteTest(script1, script2, expected);
		}

		/// <summary>
		/// When there is a dependency cylce, disable a foreign key.
		/// </summary>
		[TestMethod]
		public void DependencyCycleDeleteTest()
		{
			// In this example, Table A has a foreign key to C,
			// and C has a foreign key to B, and B has a foreign key to A,
			// creating a cycle. One of the foreign keys needs to be disabled.
			string[] script1 = { CreateTablesABC, CreateForeignKeyBToA };
			string[] script2 = { CreateTablesABC, InsertTablesABC, CreateForeignKeyBToA };
			string actualScript = ExecuteTest(script1, script2);

			// In this case the DataUpgradeScripter should only need to disable one foreign key.
			// Look for the number of occurances of NOCHECK.
			int noCheckCount = Regex.Matches(actualScript, @"\bNOCHECK\b").Count;
			Assert.AreEqual(1, noCheckCount, "The script should only need to disable one foreign key in this test scenario.");
		}

		/// <summary>
		/// When there is a dependency cylce, disable a foreign key.
		/// </summary>
		[TestMethod]
		public void DependencyCycleInsertTest()
		{
			// In this example, Table A has a foreign key to C,
			// and C has a foreign key to B, and B has a foreign key to A,
			// creating a cycle. One of the foreign keys needs to be disabled.
			string[] script1 = { CreateTablesABC, InsertTablesABC, CreateForeignKeyBToA };
			string[] script2 = { CreateTablesABC, CreateForeignKeyBToA };
			string actualScript = ExecuteTest(script1, script2);

			// In this case the DataUpgradeScripter should only need to disable one foreign key.
			// Look for the number of occurances of NOCHECK.
			int noCheckCount = Regex.Matches(actualScript, @"\bNOCHECK\b").Count;
			Assert.AreEqual(1, noCheckCount, "The script should only need to disable one foreign key in this test scenario.");
		}
		/// <summary>
		/// Ensure rows are deleted in dependency order.
		/// </summary>
		[TestMethod]
		public void DependencyOrderDeleteTest()
		{
			// In this example, Table A has a foreign key to C and C has a foreign key to B.
			// So the rows need to be deleted from A, then C, then B.
			// This test intentionally has the dependency order non-alphabetic (not A, B, C)
			// to ensure that the DataUpgradeScripter worked correctly and didn't just
			// happen to get it in the correct order because the tables were ordered alphabetically.
			string[] script1 = { CreateTablesABC };
			string[] script2 = { CreateTablesABC, InsertTablesABC };
			string actualScript = ExecuteTest(script1, script2);

			// In this case the DataUpgradeScripter should have been able to delete
			// in the correct order without having to disable any foreign keys.
			// Make sure the script doesn't contain "FK_"
			if(actualScript.Contains("FK_"))
				Assert.Fail("The script should not need to disable any foreign keys in this test scenario.");
		}

		/// <summary>
		/// Ensure rows are inserted in dependency order.
		/// </summary>
		[TestMethod]
		public void DependencyOrderInsertTest()
		{
			// In this example, Table A has a foreign key to C and C has a foreign key to B.
			// So the rows need to be inserted into B, then C, then A.
			// This test intentionally has the dependency order non-alphabetic (not A, B, C)
			// to ensure that the DataUpgradeScripter worked correctly and didn't just
			// happen to get it in the correct order because the tables were ordered alphabetically.
			string[] script1 = { CreateTablesABC, InsertTablesABC };
			string[] script2 = { CreateTablesABC };
			string actualScript = ExecuteTest(script1, script2);

			// In this case the DataUpgradeScripter should have been able to insert
			// in the correct order without having to disable any foreign keys.
			// Make sure the script doesn't contain "FK_"
			if(actualScript.Contains("FK_"))
				Assert.Fail("The script should not need to disable any foreign keys in this test scenario.");
		}
		[TestMethod]
		public void IdentityInsertTest()
		{
			string[] script1 = { CreateIdentityTable1, InsertIdentityTable1_one };
			string[] script2 = { CreateIdentityTable1 };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[IdentityTable1].';
GO
SET IDENTITY_INSERT [dbo].[IdentityTable1] ON;
GO
INSERT INTO [dbo].[IdentityTable1] ([ID], [Value])
VALUES (1, 'one');
GO
SET IDENTITY_INSERT [dbo].[IdentityTable1] OFF;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		/// <summary>
		/// Verifies that an identity column will be used if a table has no unique indexes.
		/// </summary>
		[TestMethod]
		public void IdentityOnlyCompareTest()
		{
			string[] script1 = { CreateTestTable1_IdentityOnly, InsertTestTable1_identity_uno };
			string[] script2 = { CreateTestTable1_IdentityOnly, InsertTestTable1_identity_one };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[TestTable1].';
GO
UPDATE [dbo].[TestTable1]
SET [Value] = 'uno'
WHERE [ID] = 1;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void NoChangesTest()
		{
			string[] script = { CreateTestTable1, InsertTestTable1_one };
			ExecuteTest(script, script, String.Empty, false);
		}

		/// <summary>
		/// Verifies that no comparison is performed on a table that has no unique index or primary key.
		/// </summary>
		/// <remarks>Currently we just ignore the table. We should probably have an event or error message (but not exception).</remarks>
		[TestMethod]
		public void NoIndexNoCompareTest()
		{
			string[] script1 = { CreateTestTable1_NoIndex, InsertTestTable1_uno };
			string[] script2 = { CreateTestTable1_NoIndex, InsertTestTable1_one };
			string expected = "";
			ExecuteTest(script1, script2, expected, false);
		}

		/// <summary>
		/// Verifies that a nonclustered unique key will be used if a table doesn't have a primary key.
		/// </summary>
		[TestMethod]
		public void NonClusteredUniqueKeyCompareTest()
		{
			string[] script1 = { CreateTestTable1_NonClusteredUniqueKey, InsertTestTable1_identity_uno };
			string[] script2 = { CreateTestTable1_NonClusteredUniqueKey, InsertTestTable1_identity_one };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [Value] = 'one';
GO
PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
SET IDENTITY_INSERT [dbo].[TestTable1] ON;
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, 'uno');
GO
SET IDENTITY_INSERT [dbo].[TestTable1] OFF;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void NullInsertTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_NULL };
			string[] script2 = { CreateTestTable1 };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, NULL);
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void NullKeyDeleteTest()
		{
			string[] script1 = { CreateNullKeyTable };
			string[] script2 = { CreateNullKeyTable, InsertNullKeyTable };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TableN].';
GO
DELETE FROM [dbo].[TableN] WHERE [NKey] IS NULL;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void NullKeyInsertTest()
		{
			string[] script1 = { CreateNullKeyTable, InsertNullKeyTable };
			string[] script2 = { CreateNullKeyTable };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[TableN].';
GO
INSERT INTO [dbo].[TableN] ([NKey], [Value])
VALUES (NULL, 'NULL');
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void NullKeyUpdateTest()
		{
			string[] script1 = { CreateNullKeyTable, InsertNullKeyTable };
			string[] script2 = { CreateNullKeyTable, InsertNullKeyTable_Empty };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[TableN].';
GO
UPDATE [dbo].[TableN]
SET [Value] = 'NULL'
WHERE [NKey] IS NULL;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void NullUpdateTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_NULL };
			string[] script2 = { CreateTestTable1, InsertTestTable1_one };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[TestTable1].';
GO
UPDATE [dbo].[TestTable1]
SET [Value] = NULL
WHERE [ID] = 1;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void SelfReferenceInsertTest()
		{
			string[] script1 =
			{
				CreateSelfReferenceTable,
				@"INSERT INTO SelfReference(ID, ParentID)
				VALUES
				(1, NULL),
				(2, 1),
				(3, 4),
				(4, 4)"
			};
			string[] script2 =
			{
				CreateSelfReferenceTable
			};
			string actualScript = ExecuteTest(script1, script2);
			// Check that the script disabled the foreign key constraint.
			if(!actualScript.Contains("ALTER TABLE [dbo].[SelfReference] NOCHECK CONSTRAINT [FK_SelfReference];"))
				Assert.Fail("The script did not disable the foreign key constraint.");
			// Check that the script re-enabled, WITH CHECK, the foreign key constraint.
			if(!actualScript.Contains("ALTER TABLE [dbo].[SelfReference] WITH CHECK CHECK CONSTRAINT [FK_SelfReference];"))
				Assert.Fail("The script did not re-enable the the foreign key constraint (at least not WITH CHECK).");
		}

		[TestMethod]
		public void SimpleDeleteTest()
		{
			string[] script1 = { CreateTestTable1 };
			string[] script2 = { CreateTestTable1, InsertTestTable1_one };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [ID] = 1;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void SimpleInsertTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_one };
			string[] script2 = { CreateTestTable1 };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, 'one');
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void SimpleUpdateTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_uno };
			string[] script2 = { CreateTestTable1, InsertTestTable1_one };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[TestTable1].';
GO
UPDATE [dbo].[TestTable1]
SET [Value] = 'uno'
WHERE [ID] = 1;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void SourceOnlyTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_one };
			string[] script2 = { };
			ExecuteTest(script1, script2, expectedScript: String.Empty, expectedResult: false);
		}

		[TestMethod]
		public void TargetOnlyTest()
		{
			string[] script1 = { };
			string[] script2 = { CreateTestTable1, InsertTestTable1_one };
			ExecuteTest(script1, script2, expectedScript: String.Empty, expectedResult: false);
		}

		[TestMethod]
		public void TriggerDeleteDisableTest()
		{
			string[] script1 = { CreateTestTable1, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string[] script2 = { CreateTestTable1, InsertTestTable1_one, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string expected =
@"PRINT 'Disabling triggers.';
GO
DISABLE TRIGGER [dbo].[TR_Delete_TestTable1] ON [dbo].[TestTable1];
GO
PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [ID] = 1;
GO
PRINT 'Enabling triggers.';
GO
ENABLE TRIGGER [dbo].[TR_Delete_TestTable1] ON [dbo].[TestTable1];
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void TriggerDeleteDontDisableTest()
		{
			string[] script1 = { CreateTestTable1, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string[] script2 = { CreateTestTable1, InsertTestTable1_one, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [ID] = 1;
GO
";
			var options = new DataUpgradeOptions { DisableTriggers = false };
			ExecuteTest(script1, script2, expected, options: options);
		}

		[TestMethod]
		public void TriggerInsertDisableTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_one, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string[] script2 = { CreateTestTable1, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string expected =
@"PRINT 'Disabling triggers.';
GO
DISABLE TRIGGER [dbo].[TR_Insert_TestTable1] ON [dbo].[TestTable1];
GO
PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, 'one');
GO
PRINT 'Enabling triggers.';
GO
ENABLE TRIGGER [dbo].[TR_Insert_TestTable1] ON [dbo].[TestTable1];
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void TriggerInsertDontDisableTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_one, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string[] script2 = { CreateTestTable1, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, 'one');
GO
";
			var options = new DataUpgradeOptions { DisableTriggers = false };
			ExecuteTest(script1, script2, expected, options: options);
		}

		[TestMethod]
		public void TriggerUpdateDisableTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_one, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string[] script2 = { CreateTestTable1, InsertTestTable1_uno, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string expected =
@"PRINT 'Disabling triggers.';
GO
DISABLE TRIGGER [dbo].[TR_Update_TestTable1] ON [dbo].[TestTable1];
GO
PRINT 'Updating 1 row(s) in [dbo].[TestTable1].';
GO
UPDATE [dbo].[TestTable1]
SET [Value] = 'one'
WHERE [ID] = 1;
GO
PRINT 'Enabling triggers.';
GO
ENABLE TRIGGER [dbo].[TR_Update_TestTable1] ON [dbo].[TestTable1];
GO
";
			ExecuteTest(script1, script2, expected);
		}
		[TestMethod]
		public void TriggerUpdateDontDisableTest()
		{
			string[] script1 = { CreateTestTable1, InsertTestTable1_one, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string[] script2 = { CreateTestTable1, InsertTestTable1_uno, CreateDeleteTriggerOnTable1, CreateInsertTriggerOnTable1, CreateUpdateTriggerOnTable1 };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[TestTable1].';
GO
UPDATE [dbo].[TestTable1]
SET [Value] = 'one'
WHERE [ID] = 1;
GO
";
			var options = new DataUpgradeOptions { DisableTriggers = false };
			ExecuteTest(script1, script2, expected, options: options);
		}
		/// <summary>
		/// Verifies that a unique clustered index will be used if a table doesn't have a primary key.
		/// </summary>
		[TestMethod]
		public void UniqueClusteredIndexCompareTest()
		{
			string[] script1 = { CreateTestTable1_UniqueClusteredIndex, InsertTestTable1_identity_uno };
			string[] script2 = { CreateTestTable1_UniqueClusteredIndex, InsertTestTable1_identity_one };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [Value] = 'one';
GO
PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
SET IDENTITY_INSERT [dbo].[TestTable1] ON;
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, 'uno');
GO
SET IDENTITY_INSERT [dbo].[TestTable1] OFF;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		/// <summary>
		/// Test that a unique key can be updated in a way that avoids violating the unique constraint.
		/// </summary>
		[TestMethod]
		public void UniqueConstraintUpdateDisableForeignKeyTest()
		{
			// There is a UNIQUE constraint on the Value column.
			// In this test, rows 1, 2, and 3 start out with values A, B, and C, respectively.
			// The same values are reassigned to different row IDs. In the end result
			// there are no duplicate values, but no matter which row we update first
			// it would result in a violation of the unique constraint.
			// For example, if we try to set row 1 value to B first then it would fail
			// because there would be two rows with value B (rows 1 and 2).
			string[] script1 =
			{
				CreateTestTable1,
				CreateTestTable2,
				@"INSERT TestTable1(ID, Value)
				VALUES
				(1, 'B'),
				(2, 'C'),
				(3, 'A');",
				@"INSERT TestTable2(Value)
				VALUES('A'),('B'),('C');"
			};
			string[] script2 =
			{
				CreateTestTable1,
				CreateTestTable2,
				@"INSERT TestTable1(ID, Value)
				VALUES
				(1, 'A'),
				(2, 'B'),
				(3, 'C');",
				@"INSERT TestTable2(Value)
				VALUES('A'),('B'),('C');"
			};
			string actualScript = ExecuteTest(script1, script2);
			// Check that the script disabled the foreign key constraint.
			if(!actualScript.Contains("ALTER TABLE [dbo].[TestTable2] NOCHECK CONSTRAINT [FK_TestTable2_TestTable1];"))
				Assert.Fail("The script did not disable the foreign key constraint.");
			// Check that the script re-enabled, WITH CHECK, the foreign key constraint.
			if(!actualScript.Contains("ALTER TABLE [dbo].[TestTable2] WITH CHECK CHECK CONSTRAINT [FK_TestTable2_TestTable1];"))
				Assert.Fail("The script did not re-enable the the foreign key constraint (at least not WITH CHECK).");
		}

		/// <summary>
		/// Test that a unique key can be updated in a way that avoids violating the unique constraint.
		/// </summary>
		[TestMethod]
		public void UniqueConstraintUpdateTest()
		{
			// There is a UNIQUE constraint on the Value column.
			// In this test, rows 1, 2, and 3 start out with values A, B, and C, respectively.
			// The same values are reassigned to different row IDs. In the end result
			// there are no duplicate values, but no matter which row we update first
			// it would result in a violation of the unique constraint.
			// For example, if we try to set row 1 value to B first then it would fail
			// because there would be two rows with value B (rows 1 and 2).
			string[] script1 =
			{
				CreateTestTable1,
				@"INSERT TestTable1(ID, Value)
				VALUES
				(1, 'B'),
				(2, 'C'),
				(3, 'A');"
			};
			string[] script2 =
			{
				CreateTestTable1,
				@"INSERT TestTable1(ID, Value)
				VALUES
				(1, 'A'),
				(2, 'B'),
				(3, 'C');"
			};
			string expected = null;
			ExecuteTest(script1, script2, expected);
		}

		/// <summary>
		/// Verifies that a unique nonclustered index will be used if a table doesn't have a primary key.
		/// </summary>
		[TestMethod]
		public void UniqueNonClusteredIndexCompareTest()
		{
			string[] script1 = { CreateTestTable1_UniqueNonClusteredIndex, InsertTestTable1_identity_uno };
			string[] script2 = { CreateTestTable1_UniqueNonClusteredIndex, InsertTestTable1_identity_one };
			string expected =
@"PRINT 'Deleting 1 row(s) from [dbo].[TestTable1].';
GO
DELETE FROM [dbo].[TestTable1] WHERE [Value] = 'one';
GO
PRINT 'Inserting 1 row(s) into [dbo].[TestTable1].';
GO
SET IDENTITY_INSERT [dbo].[TestTable1] ON;
GO
INSERT INTO [dbo].[TestTable1] ([ID], [Value])
VALUES (1, 'uno');
GO
SET IDENTITY_INSERT [dbo].[TestTable1] OFF;
GO
";
			ExecuteTest(script1, script2, expected);
		}
		[TestMethod]
		public void VarcharKeyUpdateCaseTest()
		{
			// Verify that even a key column will be compared case-sensitively
			// and updated appropriately.
			// In this test, the target was loaded with the key 'ONE' (all caps).
			// The source has 'One'.
			// The script should update the target with the new capitalization.
			string[] script1 =
			{
				CreateVarcharKeyTable,
				@"INSERT INTO VarcharKey(VarcharKey, Value)
				VALUES('One', 'Uno');"
			};
			string[] script2 =
			{
				CreateVarcharKeyTable,
				@"INSERT INTO VarcharKey(VarcharKey, Value)
				VALUES('ONE', 'Uno');"
			};
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[VarcharKey].';
GO
UPDATE [dbo].[VarcharKey]
SET [VarcharKey] = 'One'
WHERE [VarcharKey] = 'ONE';
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void VarcharKeyUpdateWhitespaceTest()
		{
			// Verify that even a key column will be compared for trailing whitespace
			// and updated appropriately.
			// In this test, the target was loaded with the key 'One  ' (with 2 trailing spaces).
			// The source doesn't have trailing spaces.
			// The script should update the target to remove trailing spaces.
			string[] script1 =
			{
				CreateVarcharKeyTable,
				@"INSERT INTO VarcharKey(VarcharKey, Value)
				VALUES('One', 'Uno');"
			};
			string[] script2 =
			{
				CreateVarcharKeyTable,
				@"INSERT INTO VarcharKey(VarcharKey, Value)
				VALUES('One   ', 'Uno');"
			};
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[VarcharKey].';
GO
UPDATE [dbo].[VarcharKey]
SET [VarcharKey] = 'One'
WHERE [VarcharKey] = 'One   ';
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void VariantInsertTest()
		{
			string[] script1 = { CreateVariantTable, InsertVariantTable_one };
			string[] script2 = { CreateVariantTable };
			string expected =
@"PRINT 'Inserting 1 row(s) into [dbo].[VariantTable].';
GO
INSERT INTO [dbo].[VariantTable] ([ID], [Value])
VALUES (1, CAST(CAST('one' AS [varchar](10)) COLLATE Latin1_General_CS_AS AS [sql_variant]));
GO
";
			ExecuteTest(script1, script2, expected);
		}

		[TestMethod]
		public void VariantUpdateTest()
		{
			string[] script1 = { CreateVariantTable, InsertVariantTable_1_00 };
			string[] script2 = { CreateVariantTable, InsertVariantTable_one };
			string expected =
@"PRINT 'Updating 1 row(s) in [dbo].[VariantTable].';
GO
UPDATE [dbo].[VariantTable]
SET [Value] = CAST(CAST(1.00 AS [decimal](10,2)) AS [sql_variant])
WHERE [ID] = 1;
GO
";
			ExecuteTest(script1, script2, expected);
		}

		#region SQL Statements

		readonly string CreateCompositeKeyTable =
@"CREATE TABLE CompositeKey
(
	Key1 int not null,
	Key2 char(1) not null,
	Value varchar(50),
	PRIMARY KEY (Key1, Key2)
);";

		readonly string CreateDeleteTriggerOnTable1 =
@"CREATE TRIGGER TR_Delete_TestTable1 ON TestTable1 AFTER Delete
AS PRINT 'Deleted from TestTable1.';
";

		readonly string CreateForeignKeyBToA = "ALTER TABLE TableB ADD CONSTRAINT FK_TableB_TableA FOREIGN KEY (AKey) REFERENCES TableA(AKey);";

		readonly string CreateIdentityTable1 =
@"CREATE TABLE IdentityTable1
(
	ID int not null identity PRIMARY KEY,
	Value varchar(50) null
);";

		readonly string CreateInsertTriggerOnTable1 =
@"CREATE TRIGGER TR_Insert_TestTable1 ON TestTable1 AFTER INSERT
AS PRINT 'Inserted into TestTable1.';
";

		readonly string CreateNullKeyTable =
@"
CREATE TABLE TableN
(
	NKey char(2) null UNIQUE,
	Value varchar(50) not null
);";

		readonly string CreateSelfReferenceTable =
@"CREATE TABLE SelfReference
(
	ID int not null PRIMARY KEY,
	ParentID int null CONSTRAINT FK_SelfReference FOREIGN KEY REFERENCES SelfReference(ID)
);";

		readonly string CreateTablesABC =
@"CREATE TABLE TableA
(
	AKey char(2) not null PRIMARY KEY,
	CKey char(2)
);

CREATE TABLE TableB
(
	BKey char(2) not null PRIMARY KEY,
	AKey char(2) null 
);

CREATE TABLE TableC
(
	CKey char(2) not null PRIMARY KEY,
	BKey char(2) null CONSTRAINT FK_TableC_TableB FOREIGN KEY REFERENCES TableB(BKey)
);

ALTER TABLE TableA ADD CONSTRAINT FK_TableA_TableC FOREIGN KEY (CKey) REFERENCES TableC(CKey);";

		readonly string CreateTestTable1 =
@"CREATE TABLE TestTable1
(
	ID int not null PRIMARY KEY,
	Value varchar(50) null CONSTRAINT AK_TestTable1_Value UNIQUE
);";

		readonly string CreateTestTable1_IdentityOnly =
@"CREATE TABLE TestTable1
(
	ID int NOT NULL IDENTITY(1,1),
	Value varchar(50) null
);";

		readonly string CreateTestTable1_NoIndex =
@"CREATE TABLE TestTable1
(
	ID int NOT NULL,
	Value varchar(50) null
);";

		readonly string CreateTestTable1_NonClusteredUniqueKey =
@"CREATE TABLE TestTable1
(
	ID int NOT NULL IDENTITY(1,1),
	Value varchar(50) null
);
CREATE UNIQUE NONCLUSTERED INDEX IX_TestTable1_Value ON TestTable1(Value)";

		readonly string CreateTestTable1_UniqueClusteredIndex =
@"CREATE TABLE TestTable1
(
	ID int NOT NULL IDENTITY(1,1),
	Value varchar(50) null
);
CREATE UNIQUE CLUSTERED INDEX IX_TestTable1_Value ON TestTable1(Value)";

		readonly string CreateTestTable1_UniqueNonClusteredIndex =
@"CREATE TABLE TestTable1
(
	ID int NOT NULL IDENTITY(1,1),
	Value varchar(50) null
);
CREATE UNIQUE NONCLUSTERED INDEX IX_TestTable1_Value ON TestTable1(Value)";
		readonly string CreateTestTable2 =
@"CREATE TABLE TestTable2
(
	Value varchar(50) PRIMARY KEY,
	CONSTRAINT FK_TestTable2_TestTable1 FOREIGN KEY (Value) REFERENCES TestTable1(Value)
);";

		readonly string CreateUpdateTriggerOnTable1 =
@"CREATE TRIGGER TR_Update_TestTable1 ON TestTable1 AFTER UPDATE
AS PRINT 'Updated TestTable1.';
";

		readonly string CreateVarcharKeyTable =
@"CREATE TABLE VarcharKey
(
	VarcharKey varchar(50) COLLATE Latin1_General_CI_AS NOT NULL PRIMARY KEY,
	Value varchar(50) null
);";
		readonly string CreateVariantTable =
@"CREATE TABLE VariantTable
(
	ID int not null PRIMARY KEY,
	Value sql_variant null
);";

		readonly string InsertCompositeKeyTable_1A_one =
@"INSERT INTO CompositeKey(Key1, Key2, Value)
VALUES(1, 'A', 'one');";

		readonly string InsertCompositeKeyTable_1A_uno =
@"INSERT INTO CompositeKey(Key1, Key2, Value)
VALUES(1, 'A', 'uno');";

		readonly string InsertIdentityTable1_one =
@"INSERT INTO IdentityTable1(Value)
VALUES('one');";

		readonly string InsertNullKeyTable =
@"INSERT TableN(NKey, Value)
VALUES(NULL, 'NULL')";

		readonly string InsertNullKeyTable_Empty =
@"INSERT TableN(NKey, Value)
VALUES(NULL, '')";

		readonly string InsertTablesABC =
@"
INSERT INTO TableB(BKey)
VALUES('B1');

INSERT INTO TableC(CKey, BKey)
VALUES('C1', 'B1');

INSERT INTO TableA(AKey, CKey)
VALUES('A1', 'C1');

UPDATE TableB
SET AKey = 'A1'
WHERE BKey = 'B1';";
		readonly string InsertTestTable1_identity_one =
@"INSERT INTO TestTable1(Value)
VALUES('one');";

		readonly string InsertTestTable1_identity_uno =
@"INSERT INTO TestTable1(Value)
VALUES('uno');";

		readonly string InsertTestTable1_NULL =
@"INSERT INTO TestTable1(ID, Value)
VALUES(1, NULL);";

		readonly string InsertTestTable1_one =
@"INSERT INTO TestTable1(ID, Value)
VALUES(1, 'one');";

		readonly string InsertTestTable1_uno =
@"INSERT INTO TestTable1(ID, Value)
VALUES(1, 'uno');";
		readonly string InsertVariantTable_1_00 =
@"INSERT INTO VariantTable(ID, Value)
VALUES(1, CONVERT(decimal(10, 2), 1.00));";

		readonly string InsertVariantTable_one =
@"INSERT INTO VariantTable(ID, Value)
VALUES(1, CONVERT(varchar(10), 'one') COLLATE Latin1_General_CS_AS);";

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

		private string ExecuteTest
		(
			IEnumerable<String> sourceScript,
			IEnumerable<String> targetScript,
			string expectedScript = null,
			bool expectedResult = true,
			DataUpgradeOptions options = null
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

			var target = GetDataUpgradeScripter();
			bool actualResult;
			string actualScript;
			using(StringWriter writer = new StringWriter())
			{
				// Call GenerateScript and verify the result and writer output are as expected.
				actualResult = target.GenerateScript(writer, options);
				Assert.AreEqual(expectedResult, actualResult, "GenerateScript() return value not as expected (it should be false when there are no data changes; otherwise, true).");
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
				try
				{
					// Execute the script to ensure there are no errors in it.
					Execute(actualScript.Split(new[] { "\r\nGO\r\n" }, StringSplitOptions.RemoveEmptyEntries));
				}
				catch(Exception ex)
				{
					throw new Exception("The generated script failed to execute properly.", ex);
				}
				using(StringWriter writer = new StringWriter())
				{
					// Call GenerateScript and verify that the upgrade script succeeded in synchronizing all the changes.
					bool syncResult = target.GenerateScript(writer);
					string unsynchronizedScript = writer.ToString();
					Assert.AreEqual(String.Empty, unsynchronizedScript, "The generated script failed to synchronize all changes. The 'actual' value shows the script of what is not in sync.");
					Assert.IsFalse(syncResult, "The generated script failed to synchronize all changes.");
				}
			}

			return actualScript;
		}

		private DataUpgradeScripter GetDataUpgradeScripter()
		{
			return new DataUpgradeScripter
			{
				SourceServerName = UnitTestServerName,
				SourceDatabaseName = SourceDatabaseName,
				TargetServerName = UnitTestServerName,
				TargetDatabaseName = TargetDatabaseName
			};
		}

		#endregion Private helper methods
	}
}
