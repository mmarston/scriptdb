using System;
using System.Collections.Generic;
using System.Text;

using NUnit.Framework;
using Mercent.SqlServer.Management;

namespace Mercent.SqlServer.Management.Tests
{
	[TestFixture]
	public class FileScripterFixture
	{
		[Test]
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
