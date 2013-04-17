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
using System.Threading.Tasks;
using System.Xml.Linq;
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

			if(!String.IsNullOrWhiteSpace(OutputDirectory) && !Directory.Exists(OutputDirectory))
				Directory.CreateDirectory(OutputDirectory);

			Stopwatch stopwatch = new Stopwatch();
			bool hasUpgradeScript = false;
			bool upgradeSucceeded;
			FileInfo sourcePackageFile = new FileInfo(Path.Combine(OutputDirectory, "Source.dacpac"));
			FileInfo targetPackageFile = new FileInfo(Path.Combine(OutputDirectory, "Target.dacpac"));
			FileInfo upgradeFile = new FileInfo(Path.Combine(OutputDirectory, "Upgrade.sql"));
			FileInfo schemaPrepFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaPrep.sql"));
			FileInfo schemaUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaUpgrade.sql"));
			FileInfo schemaUpgradeReportFile = new FileInfo(Path.Combine(OutputDirectory, "Log", "SchemaUpgradeReport.xml"));
			FileInfo dataPrepFile = new FileInfo(Path.Combine(OutputDirectory, "DataPrep.sql"));
			FileInfo dataUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "DataUpgrade.sql"));
			FileInfo afterUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "AfterUpgrade.sql"));
			FileInfo schemaFinalFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaFinal.sql"));
			DirectoryInfo expectedDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Expected"));
			DirectoryInfo actualDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Actual"));
			TextWriter upgradeWriter = null;
			try
			{
				upgradeWriter = upgradeFile.CreateText();
				
				// Extract the source package in parallel.
				Task<DacPackage> extractSourceTask = Task.Run(() => ExtractSource(schemaUpgradeScripter, sourcePackageFile));

				// If a schema prep file exists and is not empty then add it
				// to the upgrade script and run it on the target before comparing the schema.
				if(schemaPrepFile.Exists && schemaPrepFile.Length > 0)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, schemaPrepFile);
				}

				// Extract the target package.
				schemaUpgradeScripter.TargetPackage = ExtractTarget(schemaUpgradeScripter, targetPackageFile);

				// Get the result (the package) from the extract source parallel task.
				schemaUpgradeScripter.SourcePackage = extractSourceTask.Result;

				// Generate the schema upgrade script.
				// The SchemaUpgrade method has an option to delay dropping objects not in the source.
				// The purpose of this was to make it easier to write a custom script
				// to move data to a new table from a table that will be dropped.
				// It won't work to put the DML statment in a schema prep script
				// because the new table won't exist yet. On the other hand,
				// it won't work to the DML statement in a data prep or after upgrade script
				// because the old table will already be dropped.
				// To solve this dilema, I had experimented with delaying the drop statements
				// until the final schema upgrade script that runs last.
				// The dropObjectsNotInSource variable was set to false if a data prep or after upgrade script exists.
				// However, that caused schema upgrade failures when a new object in the source has the same
				// name as a target object to be dropped (e.g. replacing a table with a view of the same name).
				bool dropObjectsNotInSource = true;
				if(SchemaUpgrade(upgradeWriter, schemaUpgradeScripter, schemaUpgradeFile, schemaUpgradeReportFile, dropObjectsNotInSource))
					hasUpgradeScript = true;

				// If a data prep file exists and is not empty then add it
				// to the upgrade script and run it on the target before comparing the data.
				if(dataPrepFile.Exists && dataPrepFile.Length > 0)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, dataPrepFile);
				}

				// Generate the data upgrade script.
				if(DataUpgrade(upgradeWriter, dataUpgradeFile))
					hasUpgradeScript = true;

				// If an after upgrade file exists and is not empty then add it
				// to the upgrade script and run it on the target before verifying.
				if(afterUpgradeFile.Exists && afterUpgradeFile.Length > 0)
				{
					hasUpgradeScript = true;
					AddAndExecute(upgradeWriter, dataPrepFile);
				}

				// Generate the final schema upgrade script.
				if(SchemaUpgradeFinal(upgradeWriter, schemaUpgradeScripter, schemaFinalFile))
					hasUpgradeScript = true;

				// Generate a clean set of scripts for the source database (the "expected" result) in parallel.
				Task generateSourceCreateScriptsTask = Task.Run(() => GenerateSourceCreateScripts(expectedDirectory));

				// Generate a set of scripts for the upgraded target database (the "actual" result).
				GenerateTargetCreateScripts(actualDirectory);

				// Wait for the source scripts to be generated.
				generateSourceCreateScriptsTask.Wait();

				// Verify if the upgrade scripts succeed by checking if the database
				// script files in the directories are identical.
				upgradeSucceeded = AreDirectoriesIdentical(expectedDirectory, actualDirectory);
			}
			finally
			{
				// Delete the temporary package files if they exist.
				// Note that we don't use the FileInfo.Exists property because
				// that is set when the object is initialized (and updated by Refresh())
				if(File.Exists(sourcePackageFile.FullName))
					sourcePackageFile.Delete();
				if(File.Exists(targetPackageFile.FullName))
					targetPackageFile.Delete();
				if(upgradeWriter != null)
					upgradeWriter.Close();
			}

			// If there were no upgrade scripts generated then delete the main script.
			if(!hasUpgradeScript)
				upgradeFile.Delete();

			// Output potential data issues
			if(hasUpgradeScript && upgradeSucceeded)
				OutputDataIssues(schemaUpgradeReportFile);
					
			OutputSummaryMessage(hasUpgradeScript, upgradeSucceeded);
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
				Console.ForegroundColor = ConsoleColor.Red;
				Console.Error.WriteLine
				(
					"\r\n{0} script failed. Check the log file for error messages:\r\n{1}\r\n",
					scriptFile.Name,
					logFile.FullName
				);
				Console.ResetColor();
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
				try
				{
					Console.ForegroundColor = ConsoleColor.Red;
					Console.Error.WriteLine("\r\nThe upgraded target database does not match the source database.");
					ShowFiles(filesByStatus[FileCompareStatus.SourceOnly], "\r\nMissing (should have been added):");
					ShowFiles(filesByStatus[FileCompareStatus.Modified], "\r\nDifferent (should be identical):");
					ShowFiles(filesByStatus[FileCompareStatus.TargetOnly], "\r\nExtra (should have been removed):");
					Console.ResetColor();
					Console.WriteLine("\r\nTo review file level differences, use a tool such as WinMerge to compare these directories:");
					Console.WriteLine("\t{0}", expectedDirectory.FullName);
					Console.WriteLine("\t{0}", actualDirectory.FullName);
				}
				finally
				{
					Console.ResetColor();
				}
			}
			return allIdentical;
		}

		private bool DataUpgrade(TextWriter upgradeWriter, FileInfo dataUpgradeFile)
		{
			bool hasDataChanges;
			var stopwatch = Stopwatch.StartNew();

			Console.WriteLine("Comparing data and generating {0} script.", dataUpgradeFile.Name);

			DataUpgradeScripter dataUpgradeScripter = new DataUpgradeScripter
			{
				SourceServerName = SourceServerName,
				SourceDatabaseName = SourceDatabaseName,
				TargetServerName = TargetServerName,
				TargetDatabaseName = TargetDatabaseName
			};

			using(TextWriter writer = dataUpgradeFile.CreateText())
			{
				hasDataChanges = dataUpgradeScripter.GenerateScript(writer);
			}

			stopwatch.Stop();

			// If there are data changes then add the data upgrade script
			// to the upgrade script and run it before the after upgrade script.
			// Otherwise, delete the file.
			if(hasDataChanges)
			{
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
			return hasDataChanges;
		}

		private DacPackage ExtractSource(SchemaUpgradeScripter scripter, FileInfo packageFile)
		{
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("Extracting the source package.");
			var package = scripter.ExtractSource(packageFile);
			stopwatch.Stop();
			Console.WriteLine("Finished extracting the source package ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return package;
		}

		private DacPackage ExtractTarget(SchemaUpgradeScripter scripter, FileInfo packageFile)
		{
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("Extracting the target package.");
			var package = scripter.ExtractTarget(packageFile);
			stopwatch.Stop();
			Console.WriteLine("Finished extracting the target package ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return package;
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

		private void GenerateSchemaUpgradeReport(SchemaUpgradeScripter schemaUpgradeScripter, FileInfo reportFile)
		{
			using(TextWriter writer = reportFile.CreateText())
			{
				string report = schemaUpgradeScripter.GenerateReport();
				writer.Write(report);
			}
		}

		private DirectoryInfo GenerateSourceCreateScripts(DirectoryInfo directory)
		{
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("Generating clean scripts from source database (for verification).");
			GenerateCreateScripts(SourceServerName, SourceDatabaseName, directory);
			stopwatch.Stop();
			Console.WriteLine("Finished generating scripts from source database ({0}).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return directory;
		}

		private DirectoryInfo GenerateTargetCreateScripts(DirectoryInfo directory)
		{
			var stopwatch = Stopwatch.StartNew();
			Console.WriteLine("Generating scripts from upgraded target database (for verification).");
			GenerateCreateScripts(TargetServerName, TargetDatabaseName, directory);
			stopwatch.Stop();
			Console.WriteLine("Finished generating scripts from upgraded target database ({0}).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return directory;
		}

		private void OutputSummaryMessage(bool hasUpgradeScript, bool upgradeSucceeded)
		{
			Console.WriteLine();
			try
			{
				if(upgradeSucceeded)
				{
					Console.ForegroundColor = ConsoleColor.White;
					if(hasUpgradeScript)
					{
						Console.WriteLine("Upgrade scripts successfully generated and verified.");
					}
					else
						Console.WriteLine("No upgrade necessary.");
				}
				else
				{
					Console.ForegroundColor = ConsoleColor.Red;
					Console.Error.WriteLine("Upgrade scripts failed verification. Review the files that failed verification and add manual steps to a SchemaPrep.sql, DataPrep.sql or AfterUpgrade.sql script.");
				}
			}
			finally
			{
				Console.ResetColor();
			}
		}


		/// <summary>
		/// Warn the user about potential data issues.
		/// </summary>
		private void OutputDataIssues(FileInfo schemaUpgradeReportFile)
		{
			XElement deploymentReport = XElement.Load(schemaUpgradeReportFile.FullName);
			XNamespace xmlns = "http://schemas.microsoft.com/sqlserver/dac/DeployReport/2012/02";
			var dataIssues = deploymentReport
				.Elements(xmlns + "Alerts")
				.Elements(xmlns + "Alert")
				.Where(a => (string)a.Attribute("Name") == "DataIssue")
				.Elements(xmlns + "Issue")
				.ToList();
			if(dataIssues.Any())
			{
				Console.ForegroundColor = ConsoleColor.White;
				Console.WriteLine("\r\nReview issues for potential data loss:");
				Console.ForegroundColor = ConsoleColor.Yellow;
				try
				{
					foreach(var issue in dataIssues)
					{
						Console.WriteLine("\t{0}", (string)issue.Attribute("Value"));
					}
				}
				finally
				{
					Console.ResetColor();
				}
			}
		}

		private bool SchemaUpgrade(TextWriter upgradeWriter, SchemaUpgradeScripter schemaUpgradeScripter, FileInfo schemaUpgradeFile, FileInfo schemaUpgradeReportFile, bool dropObjectsNotInSource)
		{
			bool hasSchemaChanges;
			var stopwatch = Stopwatch.StartNew();

			Console.WriteLine("Comparing schema and generating {0} script.", schemaUpgradeFile.Name);

			// Generate the schema upgrade report in parallel.
			Task generateSchemaUpgradeReportTask = Task.Run(() => GenerateSchemaUpgradeReport(schemaUpgradeScripter, schemaUpgradeReportFile));

			using(TextWriter writer = schemaUpgradeFile.CreateText())
			{
				hasSchemaChanges = schemaUpgradeScripter.GenerateScript(writer, dropObjectsNotInSource);
			}

			// Wait for the upgrade report to finish before running the upgrade script.
			generateSchemaUpgradeReportTask.Wait();
			stopwatch.Stop();

			// If there are schema changes then add the schema upgrade script
			// to the upgrade script and run it on the target before comparing the data.
			// Otherwise, delete the file.
			if(hasSchemaChanges)
			{
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
			return hasSchemaChanges;
		}

		private bool SchemaUpgradeFinal(TextWriter upgradeWriter, SchemaUpgradeScripter schemaUpgradeScripter, FileInfo schemaFinalFile)
		{
			bool hasFinalSchemaChanges;
			var stopwatch = Stopwatch.StartNew();

			Console.WriteLine("Checking for final schema changes and generating {0} script.", schemaFinalFile.Name);

			// Don't use the TargetPackage because the database has been modified by other scripts.
			// The SchemaUpgradeScripter will compare directly against the target database instead of the target package.
			schemaUpgradeScripter.TargetPackage = null;

			using(TextWriter writer = schemaFinalFile.CreateText())
			{
				hasFinalSchemaChanges = schemaUpgradeScripter.GenerateScript(writer);
			}

			stopwatch.Stop();

			// If there are final schema changes then add the schema final script
			// to the upgrade script and run it on the target before verifying the upgrade.
			// Otherwise, delete the file.
			if(hasFinalSchemaChanges)
			{
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
			return hasFinalSchemaChanges;
		}

		private void ShowFiles(IEnumerable<FileCompareInfo> files, string message)
		{
			if(files.Any())
			{
				Console.ForegroundColor = ConsoleColor.Gray;
				Console.Error.WriteLine(message);
				Console.ForegroundColor = ConsoleColor.Yellow;
				foreach(var file in files)
				{
					Console.Error.WriteLine("\t{0}", file.RelativePath);
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
