using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mercent.SqlServer.Management
{
	internal class ScriptFile
	{
		public ScriptFile(string fileName)
		{
			if(fileName == null)
				throw new ArgumentNullException("fileName");
			this.FileName = fileName;
			this.Command = ":r \"" + fileName + '"';
		}

		public ScriptFile(string fileName, string command)
		{
			this.FileName = fileName;
			this.Command = command;
		}

		public string FileName { get; private set; }
		public string Command { get; private set; }
	}
}
