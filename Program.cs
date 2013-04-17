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
using System.IO;
using System.Reflection;
using System.Text;
using Mercent.SqlServer.Management.Upgrade;

namespace Mercent.SqlServer.Management
{
	class Program
	{
		static void Main(string[] args)
		{
			if(args.Length < 2)
			{
				ShowUsage();
				return;
			}
			if(String.Equals(args[0], "-Upgrade", StringComparison.OrdinalIgnoreCase))
			{
				Upgrade(args);
			}
			else
			{
				Create(args);
			}
		}

		private static void Create(string[] args)
		{
			FileScripter scripter = new FileScripter();
			scripter.ServerName = args[0];
			scripter.DatabaseName = args[1];
			for(int i = 2; i < args.Length; i++)
			{
				string arg = args[i];
				// The -SSDT argument causes ScriptDB to generate scripts for use with
				// Visual Studio SQL Server Data Tools (SSDT) projects.
				if(String.Equals(arg, "-SSDT", StringComparison.OrdinalIgnoreCase))
				{
					scripter.TargetDataTools = true;
					// SSDT wants the files to use UTF8 encoding.
					scripter.Encoding = Encoding.UTF8;
				}
				else if(String.IsNullOrEmpty(scripter.OutputDirectory))
				{
					scripter.OutputDirectory = arg;
				}
				else
				{
					Console.WriteLine("Unexpected argument: {0}", arg);
					ShowUsage();
					return;
				}
			}
			scripter.Script();
		}

		private static void ShowUsage()
		{
			string program = Path.GetFileName(Assembly.GetExecutingAssembly().Location);
			Console.WriteLine("Usage: {0} ServerName DatabaseName [OutDirectory] [-SSDT]\r\n\t{0} -Upgrade ServerName SourceDatabase TargetDatabase [OutDirectory]", program);
		}

		private static void Upgrade(string[] args)
		{
			if(args.Length < 4)
			{
				ShowUsage();
				return;
			}

			UpgradeScripter scripter = new UpgradeScripter();
			scripter.SourceServerName = scripter.TargetServerName = args[1];
			scripter.SourceDatabaseName = args[2];
			scripter.TargetDatabaseName = args[3];
			if(args.Length > 4)
				scripter.OutputDirectory = args[4];
			if(args.Length > 5)
			{
				Console.WriteLine("Unexpected argument: {0}", args[5]);
				ShowUsage();
				return;
			}

			try
			{
				scripter.GenerateScripts();
			}
			catch(AbortException)
			{
				// Catch and swallow an abort exception.
				// The error was already output and the user was
				// prompted whether to continue. The user chose
				// not to continue (so abort).
			}
		}
	}
}
