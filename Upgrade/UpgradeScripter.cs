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
using System.Diagnostics;
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
		private const string elapsedTimeFormat = @"hh\:mm\:ss";

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

			Stopwatch stopwatch = new Stopwatch();
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
					stopwatch.Restart();
					Console.WriteLine("Extracting the source package.");
					schemaUpgradeScripter.SourcePackage = schemaUpgradeScripter.ExtractSource(sourcePackageFile);
					stopwatch.Stop();
					Console.WriteLine("Finished extracting the source package ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));

					stopwatch.Restart();
					Console.WriteLine("Comparing schema and generating {0} script.", schemaUpgradeFile.Name);
					// If there is a data prep script or an after upgrade script then don't drop
					// objects yet. The data prep or after upgrade scripts may need move data
					// from tables that will be dropped.
					bool dropObjectsNotInSource = dataPrepFile.Exists || afterUpgradeFile.Exists;
					hasSchemaChanges = schemaUpgradeScripter.GenerateScript(writer, dropObjectsNotInSource);
					stopwatch.Stop();
				}

				// If there are schema changes then add the schema upgrade script
				// to the upgrade script and run it on the target before comparing the data.
				// Otherwise, delete the file.
				if(hasSchemaChanges)
				{
					hasUpgradeScript = true;
					Console.WriteLine
					(
						@"Finished comparing schema and generating {0} script. ({1} elapsed).",
						schemaUpgradeFile.Name,
						stopwatch.Elapsed.ToString(elapsedTimeFormat)
					);
					AddAndExecute(upgradeWriter, schemaUpgradeFile);
				}
				else
				{
					Console.WriteLine("No schema changes detected ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
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
					stopwatch.Restart();
					Console.WriteLine("Comparing data and generating {0} script.", dataUpgradeFile.Name);
					hasDataChanges = dataUpgradeScripter.GenerateScript(writer);
					stopwatch.Stop();
				}

				// If there are data changes then add the data upgrade script
				// to the upgrade script and run it before the after upgrade script.
				// Otherwise, delete the file.
				if(hasDataChanges)
				{
					hasUpgradeScript = true;
					Console.WriteLine
					(
						"Finished comparing data and generating {0} script ({1} elapsed).",
						dataUpgradeFile.Name,
						stopwatch.Elapsed.ToString(elapsedTimeFormat)
					);
					AddAndExecute(upgradeWriter, dataUpgradeFile);
				}
				else
				{
					Console.WriteLine("No data changes detected ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
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
					stopwatch.Restart();
					Console.WriteLine("Checking for final schema changes and generating {0} script.", schemaFinalFile.Name);
					hasFinalSchemaChanges = schemaUpgradeScripter.GenerateScript(writer);
					stopwatch.Stop();
				}

				// If there are final schema changes then add the schema final script
				// to the upgrade script and run it on the target before verifying the upgrade.
				// Otherwise, delete the file.
				if(hasFinalSchemaChanges)
				{
					hasUpgradeScript = true;
					Console.WriteLine
					(
						@"Finished checking for final schema changes and generating {0} script ({1} elapsed).",
						schemaFinalFile.Name,
						stopwatch.Elapsed.ToString(elapsedTimeFormat)
					);
					AddAndExecute(upgradeWriter, schemaFinalFile);
				}
				else
				{
					Console.WriteLine("No final schema changes detected ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
					schemaFinalFile.Delete();
				}

				// Generate a clean set of scripts for the source database (the "expected" result).
				stopwatch.Restart();
				Console.WriteLine("Generating clean scripts from source database (for verification).");
				DirectoryInfo expectedDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Expected"));
				GenerateCreateScripts(SourceServerName, SourceDatabaseName, expectedDirectory);
				stopwatch.Stop();
				Console.WriteLine("Finished generating scripts from source database ({0}).", stopwatch.Elapsed.ToString(elapsedTimeFormat));

				// Generate a set of scripts for the upgraded target database (the "actual" result).
				stopwatch.Restart();
				Console.WriteLine("Generating scripts from upgraded target database (for verification).");
				DirectoryInfo actualDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Actual"));
				GenerateCreateScripts(TargetServerName, TargetDatabaseName, actualDirectory);
				stopwatch.Stop();
				Console.WriteLine("Finished generating scripts from upgraded target database ({0}).", stopwatch.Elapsed.ToString(elapsedTimeFormat));

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
			Console.WriteLine();
			if(upgradeSucceeded)
			{
				if(hasUpgradeScript)
					Console.WriteLine("Upgrade scripts successfully generated and verified.");
				else
					Console.WriteLine("No upgrade necessary.");
			}
			else
			{
				Console.WriteLine("Upgrade scripts failed verification. Review the files that failed verification and add manual steps to a SchemaPrep.sql, DataPrep.sql or AfterUpgrade.sql script.");
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
				Console.WriteLine
				(
					"\r\n{0} script failed. Check the log file for error messages:\r\n{1}\r\n",
					scriptFile.Name,
					logFile.FullName
				);
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
			FileScripter fileScripter = new FileScripter
			{
				ServerName = serverName,
				DatabaseName = databaseName,
				OutputDirectory = outputDirectory.FullName
			};
			// Delete the directory if it already exists.
			if(Directory.Exists(fileScripter.OutputDirectory))
				Directory.Delete(fileScripter.OutputDirectory, true);
			
			string logFileName = outputDirectory.Name + ".txt";
			FileInfo logFile = new FileInfo(Path.Combine(this.OutputDirectory, "Log", logFileName));
			// Ensure the Log directory exists.
			logFile.Directory.Create();
			using(TextWriter logWriter = logFile.CreateText())
			{
				string lastProgressMessage = null;
				fileScripter.ErrorMessageReceived += (s, e) =>
				{
					if(e.Message != null)
					{
						logWriter.WriteLine(e.Message);
						// First output the last progress message.
						// This will hopefully give us context for the error message.
						if(lastProgressMessage != null)
						{
							Console.WriteLine(lastProgressMessage);
							lastProgressMessage = null;
						}
						Console.Error.WriteLine(e.Message);
					}
				};
				fileScripter.OutputMessageReceived += (s, e) =>
				{
					logWriter.WriteLine(e.Message);
				};
				fileScripter.ProgressMessageReceived += (s, e) =>
				{
					if(e.Message != null)
					{
						// Capture the last progress message so we can show it above
						// any error messages.
						lastProgressMessage = e.Message;
						logWriter.WriteLine(e.Message);
					}
				};
				fileScripter.Script();
			}
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
