using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace Mercent.SqlServer.Management
{
	class Program
	{
		static void Main(string[] args)
		{
			if(args.Length < 2)
			{
				Console.WriteLine("Usage: {0} ServerName DatabaseName [OutDirectory]", Path.GetFileName(Assembly.GetExecutingAssembly().Location));
				return;
			}
			FileScripter scripter = new FileScripter();
			scripter.ServerName = args[0];
			scripter.DatabaseName = args[1];
			if (args.Length > 2)
				scripter.OutputDirectory = args[2];
			scripter.Script();
		}
	}
}
