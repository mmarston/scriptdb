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
			scripter.OutputDirectory = "Merchant";
			scripter.ServerName = @"splat";
			scripter.DatabaseName = "MercentCustomer41_Design";
			scripter.Encoding = Encoding.ASCII;
			scripter.Script();
		}
	}
}
