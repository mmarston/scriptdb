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
using System.Linq;
using System.Text;
using Mercent.SqlServer.Management.IO;
using Mercent.SqlServer.Management.Upgrade.Data;
using Mercent.SqlServer.Management.Upgrade.Schema;
using Microsoft.SqlServer.Dac;

namespace Mercent.SqlServer.Management.Upgrade
{
	public class UpgradeScripter
	{
		public UpgradeScripter()
		{
			// Default to empty string, which uses current directory.
			OutputDirectory = String.Empty;
		}

		public string OutputDirectory { get; set; }
		public string SourceDatabaseName { get; set; }

		/// <summary>
		/// Directory for source database scripts (optional).
		/// </summary>
		/// <remarks>
		/// This is the directory containing the output from <see cref="FileScripter.Script"/>.
		/// </remarks>
		public string SourceDirectory { get; set; }
		public string SourceServerName { get; set; }
		public string TargetDatabaseName { get; set; }

		/// <summary>
		/// Directory for target database scripts (optional).
		/// </summary>
		/// <remarks>
		/// This is the directory containing the output from <see cref="FileScripter.Script"/>.
		/// </remarks>
		public string TargetDirectory { get; set; }
		public string TargetServerName { get; set; }

		public void GenerateScripts()
		{
			VerifyProperties();

			SchemaUpgradeScripter schemaUpgradeScripter = new SchemaUpgradeScripter
			{
				SourceServerName = SourceServerName,
				SourceDatabaseName = SourceDatabaseName,
				TargetServerName = TargetServerName,
				TargetDatabaseName = TargetDatabaseName
			};

			DataUpgradeScripter dataUpgradeScripter = new DataUpgradeScripter
			{
				SourceServerName = SourceServerName,
				SourceDatabaseName = SourceDatabaseName,
				TargetServerName = TargetServerName,
				TargetDatabaseName = TargetDatabaseName
			};

			if(!String.IsNullOrWhiteSpace(OutputDirectory) && !Directory.Exists(OutputDirectory))
				Directory.CreateDirectory(OutputDirectory);

			bool hasUpgradeScript = false;
			bool upgradeSucceeded;
			FileInfo sourcePackageFile = new FileInfo(Path.Combine(OutputDirectory, "Source.dacpac"));
			FileInfo upgradeFile = new FileInfo(Path.Combine(OutputDirectory, "Upgrade.sql"));
			FileInfo schemaPrepFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaPrep.sql"));
			FileInfo schemaUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaUpgrade.sql"));
			FileInfo dataPrepFile = new FileInfo(Path.Combine(OutputDirectory, "DataPrep.sql"));
			FileInfo dataUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "DataUpgrade.sql"));
			FileInfo afterUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "AfterUpgrade.sql"));
			FileInfo schemaFinalFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaFinal.sql"));
			TextWriter upgradeWriter = null;
			try
			{
				upgradeWriter = upgradeFile.CreateText();

				// If a schema prep file exists and is not empty then add it
				// to the upgrade script and run it on the target before comparing the schema.
				if(schemaPrepFile.Exists && schemaPrepFile.Length > 0)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, schemaPrepFile);
				}

				// Generate the schema upgrade script.
				bool hasSchemaChanges;
				using(TextWriter writer = schemaUpgradeFile.CreateText())
				{
					Console.WriteLine("Extracting the source package.");
					schemaUpgradeScripter.SourcePackage = schemaUpgradeScripter.ExtractSource(sourcePackageFile);

					Console.WriteLine("Comparing schema and generating {0} script.", schemaUpgradeFile.Name);
					// If there is a data prep script or an after upgrade script then don't drop
					// objects yet. The data prep or after upgrade scripts may need move data
					// from tables that will be dropped.
					bool dropObjectsNotInSource = dataPrepFile.Exists || afterUpgradeFile.Exists;
					hasSchemaChanges = schemaUpgradeScripter.GenerateScript(writer, dropObjectsNotInSource);
				}

				// If there are schema changes then add the schema upgrade script
				// to the upgrade script and run it on the target before comparing the data.
				// Otherwise, delete the file.
				if(hasSchemaChanges)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, schemaUpgradeFile);
				}
				else
				{
					Console.WriteLine("No schema changes detected.");
					schemaUpgradeFile.Delete();
				}

				// If a data prep file exists and is not empty then add it
				// to the upgrade script and run it on the target before comparing the data.
				if(dataPrepFile.Exists && dataPrepFile.Length > 0)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, dataPrepFile);
				}

				// Generate the data upgrade script.
				bool hasDataChanges;
				using(TextWriter writer = dataUpgradeFile.CreateText())
				{
					Console.WriteLine("Comparing data and generating {0} script.", dataUpgradeFile.Name);
					hasDataChanges = dataUpgradeScripter.GenerateScript(writer);
				}

				// If there are data changes then add the data upgrade script
				// to the upgrade script and run it before the after upgrade script.
				// Otherwise, delete the file.
				if(hasDataChanges)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, dataUpgradeFile);
				}
				else
				{
					Console.WriteLine("No data changes detected.");
					dataUpgradeFile.Delete();
				}

				// If an after upgrade file exists and is not empty then add it
				// to the upgrade script and run it on the target before verifying.
				if(afterUpgradeFile.Exists && afterUpgradeFile.Length > 0)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, dataPrepFile);
				}

				// Generate the final schema upgrade script.
				bool hasFinalSchemaChanges = false;
				using(TextWriter writer = schemaFinalFile.CreateText())
				{
					Console.WriteLine("Checking for final schema changes and generating {0} script.", schemaFinalFile.Name);
					hasFinalSchemaChanges = schemaUpgradeScripter.GenerateScript(writer);
				}

				// If there are final schema changes then add the schema final script
				// to the upgrade script and run it on the target before verifying the upgrade.
				// Otherwise, delete the file.
				if(hasFinalSchemaChanges)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, schemaFinalFile);
				}
				else
				{
					Console.WriteLine("No final schema changes detected.");
					schemaFinalFile.Delete();
				}

				// Generate a clean set of scripts for the source database (the "expected" result).
				Console.WriteLine("Generating clean scripts from source database (for verification).");
				DirectoryInfo expectedDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Expected"));
				GenerateCreateScripts(SourceServerName, SourceDatabaseName, expectedDirectory);

				// Generate a set of scripts for the upgraded target database (the "actual" result).
				Console.WriteLine("Generating scripts from upgraded target database (for verification).");
				DirectoryInfo actualDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Actual"));
				GenerateCreateScripts(SourceServerName, SourceDatabaseName, actualDirectory);

				// Verify if the upgrade scripts succeed by checking if the database
				// script files in the directories are identical.
				upgradeSucceeded = AreDirectoriesIdentical(expectedDirectory, actualDirectory);
			}
			finally
			{
				// Delete the temporary package file if it exists.
				// Note that we don't use the FileInfo.Exists property because
				// that is set when the object is initialized (and updated by Refresh())
				if(File.Exists(sourcePackageFile.FullName))
					sourcePackageFile.Delete();
				if(upgradeWriter != null)
					upgradeWriter.Close();
			}

			// If there were no upgrade scripts generated then delete the main script.
			if(!hasUpgradeScript)
				upgradeFile.Delete();

			OutputSummaryMessage(hasUpgradeScript, upgradeSucceeded);
		}

		private static void OutputSummaryMessage(bool hasUpgradeScript, bool upgradeSucceeded)
		{
			if(upgradeSucceeded)
			{
				if(hasUpgradeScript)
					Console.WriteLine("Upgrade scripts successfully generated and verified.");
				else
					Console.WriteLine("No upgrade necessary.");
			}
			else
			{
				Console.WriteLine("Upgrade scripts failed verification. Review the files that failed verification and add manual steps to on of the SchemaPrep.sql, DataPrep.sql or AfterUpgrade.sql scripts.");
			}
		}

		private void AddAndExecute(TextWriter writer, FileInfo scriptFile)
		{
			// Include a reference to the script in the Upgrade.sql script.
			writer.WriteLine("PRINT 'Starting {0}.';", scriptFile.Name);
			writer.WriteLine("GO");
			writer.WriteLine(":r \"{0}\"", scriptFile.Name);
			writer.WriteLine("GO");
			writer.WriteLine("PRINT '{0} complete.';", scriptFile.Name);
			writer.WriteLine("GO");

			// Run the script against the target database now.
			Console.WriteLine("Executing {0} script.", scriptFile.Name);
			string logFileName = Path.ChangeExtension(scriptFile.Name, ".txt");
			FileInfo logFile = new FileInfo(Path.Combine(OutputDirectory, "Log", logFileName));

			int exitCode = ScriptUtility.RunSqlCmd(TargetServerName, TargetDatabaseName, scriptFile, logFile: logFile);
			if(exitCode != 0)
			{
				Console.WriteLine("{0} script failed. Check the log file for error messages:", scriptFile.Name);
				Console.WriteLine(logFile.FullName);
				Console.WriteLine();
				// TODO: abort when non-zero exit code.
			}
		}

		private bool AreDirectoriesIdentical(DirectoryInfo expectedDirectory, DirectoryInfo actualDirectory)
		{
			Console.WriteLine("Comparing generated database scripts to verify upgrade scripts succeeded.");
			// Compare the files in the directories and convert to a lookup on status.
			var filesByStatus = DirectoryComparer.Compare(expectedDirectory.FullName, actualDirectory.FullName)
				.ToLookup(f => f.Status);
			bool allIdentical = filesByStatus.All(group => group.Key == FileCompareStatus.Identical);
			if(!allIdentical)
			{
				// TODO: Set console color.
				Console.Error.WriteLine("The upgraded target database does not match the source database.");
				ShowFiles(filesByStatus[FileCompareStatus.SourceOnly], "Missing (should have been added):");
				ShowFiles(filesByStatus[FileCompareStatus.Modified], "Different (should be identical):");
				ShowFiles(filesByStatus[FileCompareStatus.TargetOnly], "Extra (should have been removed):");
				Console.WriteLine("To review file level differences, use a tool such as WinMerge to compare these directories:");
				Console.WriteLine("\t{0}", expectedDirectory.FullName);
				Console.WriteLine("\t{0}", actualDirectory.FullName);
			}
			return allIdentical;
		}

		/// <summary>
		/// Generates a clean set of database creation scripts.
		/// </summary>
		/// <remarks>
		/// The database creation scripts are used to verify that the upgrade scripts
		/// correctly upgraded the target database (the "actual") to match the source database (the "expected");
		/// </remarks>
		private void GenerateCreateScripts(string serverName, string databaseName, DirectoryInfo outputDirectory)
		{
			FileScripter sourceScripter = new FileScripter
			{
				ServerName = SourceServerName,
				DatabaseName = SourceDatabaseName,
				OutputDirectory = outputDirectory.FullName
			};
			// Delete the directory if it already exists.
			if(Directory.Exists(sourceScripter.OutputDirectory))
				Directory.Delete(sourceScripter.OutputDirectory, true);
			sourceScripter.Script();
		}

		private void ShowFiles(IEnumerable<FileCompareInfo> files, string message)
		{
			if(files.Any())
			{
				Console.WriteLine(message);
				foreach(var file in files)
				{
					Console.WriteLine("\t{0}", file.RelativePath);
				}
			}
		}

		private void VerifyProperties()
		{
			if(String.IsNullOrWhiteSpace(this.SourceServerName))
				throw new InvalidOperationException("Set the SourceServerName property before calling the GenerateScripts() method.");
			if(String.IsNullOrWhiteSpace(this.SourceDatabaseName))
				throw new InvalidOperationException("Set the DatabaseName property before calling the GenerateScripts() method.");
			if(String.IsNullOrWhiteSpace(this.TargetServerName))
				throw new InvalidOperationException("Set the TargetServerName property before calling the GenerateScripts() method.");
			if(String.IsNullOrWhiteSpace(this.TargetDatabaseName))
				throw new InvalidOperationException("Set the TargetDatabaseName property before calling the GenerateScripts() method.");
			// The OutputDirectory property does not need to be set by the caller.
			// Default to empty string, which will use the current directory.
			// Path.Combine(OutputDirectory, "somefile") will work with empty string but not null.
			if(this.OutputDirectory == null)
				this.OutputDirectory = String.Empty;
		}
	}
}
