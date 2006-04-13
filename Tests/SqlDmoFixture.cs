using System;
using System.Collections.Generic;
using System.Text;

using NUnit.Framework;

using SQLDMO;

namespace Mercent.SqlServer.Management.Tests
{
	[TestFixture]
	public class SqlDmoFixture
	{
		[Test]
		public void ExportDataText()
		{
			string serverName = @"mmarston\sql2005";
			string databaseName = "SEM_Merchant";
			string tableName = "MerchantAccount";

			SQLServer server = new SQLServerClass();
			server.LoginSecure = true;
			server.Connect(serverName, null, null);
			_Database database = server.Databases.Item(databaseName, null);
			Assert.IsNotNull(database, "The database could not be found.");
			_Table table = database.Tables.Item(tableName, null);
			Assert.IsNotNull(table, "The table could not be found.");

			BulkCopy bulkCopy = new BulkCopyClass();
			bulkCopy.DataFilePath = tableName + ".dat";
			bulkCopy.DataFileType = SQLDMO_DATAFILE_TYPE.SQLDMODataFile_NativeFormat;

			table.ExportData(bulkCopy);
		}
	}
}
