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
		public DacPackage TargetPackage { get; set; }
		public string TargetServerName { get; set; }

		public bool SyncMode { get; set; }

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

			return Extract(SourceServerName, SourceDatabaseName, packageFile, "source");
		}

		/// <summary>
		/// Extracts a <see cref="DacPackage"/> from the target database and returns it.
		/// </summary>
		public DacPackage ExtractTarget(FileInfo packageFile)
		{
			if(packageFile == null)
				throw new ArgumentNullException("packageFile");

			// Verify properties.
			if(String.IsNullOrWhiteSpace(this.TargetServerName))
				throw new InvalidOperationException("Set the SourceServerName property before calling the ExtractTarget() method.");
			if(String.IsNullOrWhiteSpace(this.TargetDatabaseName))
				throw new InvalidOperationException("Set the SourceDatabaseName property before calling the ExtractTarget() method.");

			return Extract(TargetServerName, TargetDatabaseName, packageFile, "target");
		}

		public string GenerateReport()
		{
			VerifyProperties("GenerateReport");

			// Get the database deployment options.
			DacDeployOptions deployOptions = GetDeployOptions();

			// Use the package from the SourcePackage property.
			// But if that is null, then create a temporary package.
			// Use try/finally to ensure that the temporary file gets deleted.
			DacPackage tempSourcePackage = SourcePackage;
			FileInfo tempSourcePackageFile = null;
			string deployReport;
			try
			{
				if(tempSourcePackage == null)
				{
					tempSourcePackageFile = new FileInfo(Path.GetTempFileName());
					tempSourcePackage = ExtractSource(tempSourcePackageFile);
				}

				// Generate the deploy report.
				if(this.TargetPackage == null)
				{
					// If the target package is not specified, then create a DacServices instance
					// and use the GenerateDeployReport instance method that takes a target database name.
					DacServices targetServices = GetTargetDacServices();
					deployReport = targetServices.GenerateDeployReport(tempSourcePackage, TargetDatabaseName, deployOptions);
				}
				else
				{
					// Otherwise, since the target package is specified, use the DacServices.GenerateDeployReport
					// static method that takes a target package.
					deployReport = DacServices.GenerateDeployReport(tempSourcePackage, TargetPackage, TargetDatabaseName ?? TargetPackage.Name, deployOptions);
				}
			}
			finally
			{
				if(tempSourcePackageFile != null)
					tempSourcePackageFile.Delete();
			}

			return deployReport;
		}

		public bool GenerateScript(TextWriter writer, bool dropObjectsNotInSource = true)
		{
			if(writer == null)
				throw new ArgumentNullException("writer");

			VerifyProperties("GenerateScript");

			// Get the database deployment options.
			DacDeployOptions deployOptions = GetDeployOptions();
			deployOptions.DropObjectsNotInSource = dropObjectsNotInSource;

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
				if(this.TargetPackage == null)
				{
					// If the target package is not specified, then create a DacServices instance
					// and use the GenerateDeployScript instance method that takes a target database name.
					DacServices targetServices = GetTargetDacServices();
					deployScript = targetServices.GenerateDeployScript(tempSourcePackage, TargetDatabaseName, deployOptions);
				}
				else
				{
					// Otherwise, since the target package is specified, use the DacServices.GenerateDeployScript
					// static method that takes a target package.
					deployScript = DacServices.GenerateDeployScript(tempSourcePackage, TargetPackage, TargetDatabaseName ?? TargetPackage.Name, deployOptions);
				}
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
				SetOptions(writer);
				writer.Write(deployScript);
				return true;
			}
		}

		private static DacPackage Extract(string serverName, string databaseName, FileInfo packageFile, string label)
		{
			SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder
			{
				DataSource = serverName,
				InitialCatalog = databaseName,
				IntegratedSecurity = true
			};
			DacServices dacServices = new DacServices(connectionStringBuilder.ConnectionString);
			dacServices.ProgressChanged += (s, e) =>
			{
				if(e.Status == DacOperationStatus.Running)
					Console.WriteLine("{0} ({1})", e.Message, label);
			};
			DacExtractOptions extractOptions = new DacExtractOptions
			{
				IgnorePermissions = false,
				IgnoreUserLoginMappings = true,
				Storage = DacSchemaModelStorageType.Memory
			};

			// Ensure the package file directory exists.
			packageFile.Directory.Create();

			dacServices.Extract(packageFile.FullName, databaseName, databaseName, new Version(1, 0), extractOptions: extractOptions);
			return DacPackage.Load(packageFile.FullName, DacSchemaModelStorageType.Memory);
		}

		/// <summary>
		/// Get the database deployment options.
		/// </summary>
		/// <remarks>
		/// Note that even though options such as keyword casing, simicolons, and whitespace
		/// don't affect the runtime behavior, we want to compare these so that we can
		/// achieve source-code level equivalence between the source and target
		/// (script files generate from the databases should be identical).
		/// </remarks>
		private DacDeployOptions GetDeployOptions()
		{
			return new DacDeployOptions
			{
				AllowDropBlockingAssemblies = true,
				BlockOnPossibleDataLoss = false,
				CommentOutSetVarDeclarations = true,
				//ExcludeObjectTypes = new [] { ObjectType.Users },
				//DoNotDropObjectTypes = new [] { ObjectType.Users },
				DropObjectsNotInSource = true,
				DropPermissionsNotInSource = true,
				DropRoleMembersNotInSource = true,
				GenerateSmartDefaults = true,
				IgnoreAnsiNulls = false,
				IgnoreKeywordCasing = false,
				IgnorePartitionSchemes = this.SyncMode,
				IgnoreQuotedIdentifiers = false,
				IgnoreSemicolonBetweenStatements = false,
				IgnoreWhitespace = false,
				ScriptDatabaseCollation = true,
				ScriptDatabaseCompatibility = true
			};
		}

		/// <summary>
		/// Apply the appropriate SET options (ANSI_NULLS, ANSI_PADDING, etc).
		/// </summary>
		/// <remarks>
		/// These are the SET options required for updates to tables that affect filtered indexes, indexes on views,
		/// or indexes computed columns.
		/// See http://msdn.microsoft.com/en-us/library/ms188783.aspx
		/// (the "Required SET Options for Filtered Indexes" section)
		/// and http://msdn.microsoft.com/en-us/library/ms190356.aspx
		/// (the "When you are creating and manipulating indexes on computed columns or indexed views..." paragraph).
		/// </remarks>
		private static void SetOptions(TextWriter writer)
		{
			writer.WriteLine("SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON;");
			writer.WriteLine("SET NUMERIC_ROUNDABORT OFF;");
			writer.WriteLine("GO");
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

		private DacServices GetTargetDacServices()
		{
			SqlConnectionStringBuilder targetBuilder = new SqlConnectionStringBuilder
			{
				DataSource = TargetServerName,
				InitialCatalog = TargetDatabaseName,
				IntegratedSecurity = true
			};
			DacServices targetServices = new DacServices(targetBuilder.ConnectionString);

			return targetServices;
		}

		private void VerifyProperties(string method)
		{
			if(this.SourcePackage == null)
			{
				if(String.IsNullOrWhiteSpace(this.SourceServerName))
					throw new InvalidOperationException("Set the SourceServerName or SourcePackage property before calling the " + method + "() method.");
				if(String.IsNullOrWhiteSpace(this.SourceDatabaseName))
					throw new InvalidOperationException("Set the SourceDatabaseName or SourcePackage property before calling the " + method + "() method.");
			}
			if(this.TargetPackage == null)
			{
				if(String.IsNullOrWhiteSpace(this.TargetServerName))
					throw new InvalidOperationException("Set the TargetServerName or TargetPackage property before calling the " + method + "() method.");
				if(String.IsNullOrWhiteSpace(this.TargetDatabaseName))
					throw new InvalidOperationException("Set the TargetDatabaseName or TargetPackage property before calling the " + method + "() method.");
			}
		}
	}
}
