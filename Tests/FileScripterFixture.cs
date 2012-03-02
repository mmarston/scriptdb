using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Mercent.SqlServer.Management;

namespace Mercent.SqlServer.Management.Tests
{
	[TestClass]
	public class FileScripterFixture
	{
		[TestMethod]
		public void ScriptTest()
		{
			FileScripter scripter = new FileScripter();
			scripter.OutputDirectory = "Product_Merchant";
			scripter.ServerName = @"tank";
			scripter.DatabaseName = "Product_Merchant";
			scripter.Script();
		}
	}
}
