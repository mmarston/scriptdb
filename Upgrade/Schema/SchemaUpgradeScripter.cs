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
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Dac;

namespace Mercent.SqlServer.Management.Upgrade.Schema
{
	public class SchemaUpgradeScripter
	{
		public string SourceDatabaseName { get; set; }
		public DacPackage SourcePackage { get; set; }
		public string SourceServerName { get; set; }
		public string TargetDatabaseName { get; set; }
		public string TargetServerName { get; set; }

		/// <summary>
		/// Extracts a <see cref="DacPackage"/> from the source database and returns it.
		/// </summary>
		public DacPackage ExtractSource(FileInfo packageFile)
		{
			if(packageFile == null)
				throw new ArgumentNullException("packageFile");

			// Verify properties.
			if(String.IsNullOrWhiteSpace(this.SourceServerName))
				throw new InvalidOperationException("Set the SourceServerName property before calling the ExtractSource() method.");
			if(String.IsNullOrWhiteSpace(this.SourceDatabaseName))
				throw new InvalidOperationException("Set the SourceDatabaseName property before calling the ExtractSource() method.");

			SqlConnectionStringBuilder sourceBuilder = new SqlConnectionStringBuilder
			{
				DataSource = SourceServerName,
				InitialCatalog = SourceDatabaseName,
				IntegratedSecurity = true
			};
			DacServices sourceServices = new DacServices(sourceBuilder.ConnectionString);

			DacExtractOptions extractOptions = new DacExtractOptions
			{
				IgnorePermissions = false,
				IgnoreUserLoginMappings = true
			};

			// Ensure the package file directory exists.
			packageFile.Directory.Create();

			sourceServices.Extract(packageFile.FullName, SourceDatabaseName, SourceDatabaseName, new Version(1, 0), extractOptions: extractOptions);
			return DacPackage.Load(packageFile.FullName);
		}

		public bool GenerateScript(TextWriter writer, bool dropObjectsNotInSource = true)
		{
			if(writer == null)
				throw new ArgumentNullException("writer");

			VerifyProperties();

			SqlConnectionStringBuilder targetBuilder = new SqlConnectionStringBuilder
			{
				DataSource = TargetServerName,
				InitialCatalog = TargetDatabaseName,
				IntegratedSecurity = true
			};
			DacServices targetServices = new DacServices(targetBuilder.ConnectionString);

			DacDeployOptions deployOptions = new DacDeployOptions
			{
				AllowDropBlockingAssemblies = true,
				BlockOnPossibleDataLoss = false,
				CommentOutSetVarDeclarations = true,
				DropObjectsNotInSource = true,
				DropPermissionsNotInSource = true,
				DropRoleMembersNotInSource = true,
				GenerateSmartDefaults = true,
				IgnoreAnsiNulls = false,
				IgnoreKeywordCasing = false,
				IgnoreQuotedIdentifiers = false,
				IgnoreSemicolonBetweenStatements = false,
				IgnoreWhitespace = false,
				ScriptDatabaseCollation = true,
				ScriptDatabaseCompatibility = true
			};

			// Use the package from the SourcePackage property.
			// But if that is null, then create a temporary package.
			// Use try/finally to ensure that the temporary file gets deleted.
			DacPackage tempSourcePackage = SourcePackage;
			FileInfo tempSourcePackageFile = null;
			string deployScript;
			try
			{
				if(tempSourcePackage == null)
				{
					tempSourcePackageFile = new FileInfo(Path.GetTempFileName());
					tempSourcePackage = ExtractSource(tempSourcePackageFile);
				}

				// Generate the deploy script (schema upgrade).
				deployScript = targetServices.GenerateDeployScript(SourcePackage, TargetDatabaseName, options: deployOptions); ;
			}
			finally
			{
				if(tempSourcePackageFile != null)
					tempSourcePackageFile.Delete();
			}

			// Try to remove extraneous header, setvar, SQLCMD mode detection, final PRINT statement, etc.
			deployScript = TrimDeployScript(deployScript);

			// If the deploy script is empty after trimming (no schema changes)
			// then return false.
			// Otherwise, write the script and return true.
			if(deployScript.Length == 0)
				return false;
			else
			{
				writer.Write(deployScript);
				return true;
			}
		}

		/// <summary>
		/// Try to remove extraneous header, setvar, SQLCMD mode detection, final PRINT statement, etc.
		/// </summary>
		/// <remarks>
		/// This method is not about trimming whitespace.
		/// If there are no schema changes then the return value should be an empty string.
		/// This method also changes the script variable $(DatabaseName) to $(SQLCMDDBNAME).
		/// </remarks>
		private static string TrimDeployScript(string deployScript)
		{
			// Try to remove extraneous header, setvar, and SQLCMD mode detection.
			int firstUseIndex = deployScript.IndexOf("USE [$(DatabaseName)]");
			int startIndex = 0;
			int endIndex = deployScript.Length;
			if(firstUseIndex >= 0)
			{
				string go = "\r\nGO\r\n";
				int nextGoIndex = deployScript.IndexOf(go, firstUseIndex);
				if(nextGoIndex > 0)
					startIndex = nextGoIndex + go.Length;
			}

			string updateComplete = "PRINT N'Update complete.';\r\n\r\n\r\nGO\r\n";
			if(deployScript.EndsWith(updateComplete))
				endIndex = deployScript.Length - updateComplete.Length;

			// If we found the boundaries to start from to skip the extra stuff at the begining
			// or end then use the substring.
			if(startIndex > 0 || endIndex != deployScript.Length)
				deployScript = deployScript.Substring(startIndex, endIndex - startIndex);

			// Use the SQLCMDDBNAME variable name instead of $(DatabaseName).
			deployScript = deployScript.Replace("$(DatabaseName)", "$(SQLCMDDBNAME)");
			return deployScript;
		}

		private void VerifyProperties()
		{
			if(this.SourcePackage == null)
			{
				if(String.IsNullOrWhiteSpace(this.SourceServerName))
					throw new InvalidOperationException("Set the SourceServerName or SourcePackage property before calling the GenerateScript() method.");
				if(String.IsNullOrWhiteSpace(this.SourceDatabaseName))
					throw new InvalidOperationException("Set the SourceDatabaseName or SourcePackage property before calling the GenerateScript() method.");
			}
			if(String.IsNullOrWhiteSpace(this.TargetServerName))
				throw new InvalidOperationException("Set the TargetServerName property before calling the GenerateScript() method.");
			if(String.IsNullOrWhiteSpace(this.TargetDatabaseName))
				throw new InvalidOperationException("Set the TargetDatabaseName property before calling the GenerateScript() method.");
		}
	}
}
