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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
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
		private static readonly object consoleLock = new object();

		private bool hasUpgradeScript = false;
		private bool hasUpgradeScriptError = false;

		public UpgradeScripter()
		{
			// Default to empty string, which uses current directory.
			OutputDirectory = String.Empty;
			Encoding = Encoding.Default;
		}

		public Encoding Encoding { get; set; }
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
			hasUpgradeScript = false;
			hasUpgradeScriptError = false;

			var totalStopwatch = Stopwatch.StartNew();

			SchemaUpgradeScripter schemaUpgradeScripter = new SchemaUpgradeScripter
			{
				SourceServerName = SourceServerName,
				SourceDatabaseName = SourceDatabaseName,
				TargetServerName = TargetServerName,
				TargetDatabaseName = TargetDatabaseName
			};

			if(!String.IsNullOrWhiteSpace(OutputDirectory) && !Directory.Exists(OutputDirectory))
				Directory.CreateDirectory(OutputDirectory);

			CreateDatabases();

			bool upgradedTargetMatchesSource;
			FileInfo sourcePackageFile = new FileInfo(Path.Combine(OutputDirectory, "Temp", "Source.dacpac"));
			FileInfo targetPackageFile = new FileInfo(Path.Combine(OutputDirectory, "Temp", "Target.dacpac"));
			FileInfo upgradeFile = new FileInfo(Path.Combine(OutputDirectory, "Upgrade.sql"));
			FileInfo schemaUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaUpgrade.sql"));
			FileInfo schemaUpgradeReportFile = new FileInfo(Path.Combine(OutputDirectory, "Log", "SchemaUpgradeReport.xml"));
			FileInfo dataUpgradeFile = new FileInfo(Path.Combine(OutputDirectory, "DataUpgrade.sql"));
			FileInfo schemaFinalFile = new FileInfo(Path.Combine(OutputDirectory, "SchemaFinal.sql"));
			DirectoryInfo expectedDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Expected"));
			DirectoryInfo actualDirectory = new DirectoryInfo(Path.Combine(OutputDirectory, @"Compare\Actual"));
			TextWriter upgradeWriter = null;
			try
			{
				upgradeWriter = CreateText(upgradeFile);

				// Ensure that errors will cause the script to be aborted.
				upgradeWriter.WriteLine("SET XACT_ABORT ON;");
				upgradeWriter.WriteLine("GO");
				upgradeWriter.WriteLine(":on error exit");
				upgradeWriter.WriteLine("GO");

				// Run schema prep scripts (if any) and add them to the upgrade script
				// before comparing the schema.
				SchemaPrep(upgradeWriter);

				// Extract the source package in parallel.
				Task<DacPackage> extractSourceTask = Task.Run(() => ExtractSource(schemaUpgradeScripter, sourcePackageFile));

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
				SchemaUpgrade(upgradeWriter, schemaUpgradeScripter, schemaUpgradeFile, schemaUpgradeReportFile, dropObjectsNotInSource);

				// Run data prep scripts (if any) and add them to the upgrade script
				// before comparing the data.
				DataPrep(upgradeWriter);
				
				// Generate the data upgrade script.
				DataUpgrade(upgradeWriter, dataUpgradeFile);

				// Run after upgrade scripts files (if any) and add them to the upgrade script
				// before verifying.
				AfterUpgrade(upgradeWriter);

				// Generate the final schema upgrade script.
				SchemaUpgradeFinal(upgradeWriter, schemaUpgradeScripter, schemaFinalFile);

				// Generate a clean set of scripts for the source database (the "expected" result) in parallel.
				Task generateSourceCreateScriptsTask = Task.Run(() => GenerateSourceCreateScripts(expectedDirectory));

				// Generate a set of scripts for the upgraded target database (the "actual" result).
				GenerateTargetCreateScripts(actualDirectory);

				// Wait for the source scripts to be generated.
				generateSourceCreateScriptsTask.Wait();

				// Verify if the upgrade scripts succeed by checking if the database
				// script files in the directories are identical.
				upgradedTargetMatchesSource = AreDirectoriesIdentical(expectedDirectory, actualDirectory);
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

			// Output potential data issues only if there were no errors
			// and the upgraded target matches the source.
			// (Otherwise the user should focus on correcting the scripts
			// to get the target to match the source.)
			if(hasUpgradeScript && !hasUpgradeScriptError && upgradedTargetMatchesSource)
				OutputDataIssues(schemaUpgradeReportFile);

			totalStopwatch.Stop();
			OutputSummaryMessage(upgradedTargetMatchesSource, totalStopwatch.Elapsed);
		}

		private void AddAndExecute(TextWriter writer, IEnumerable<FileInfo> scriptFiles)
		{
			foreach(FileInfo scriptFile in scriptFiles)
			{
				AddAndExecute(writer, scriptFile);
			}
		}

		private void AddAndExecute(TextWriter writer, FileInfo scriptFile)
		{
			hasUpgradeScript = true;

			// Include a reference to the script in the Upgrade.sql script.
			writer.WriteLine("PRINT 'Starting {0}.';", scriptFile.Name);
			writer.WriteLine("GO");
			writer.WriteLine(":r \"{0}\"", scriptFile.Name);
			writer.WriteLine("GO");
			writer.WriteLine("PRINT '{0} complete.';", scriptFile.Name);
			writer.WriteLine("GO");

			// Run the script against the target database now.
			ConsoleWriteLine("Executing {0} script.", scriptFile.Name);
			string logFileName = Path.ChangeExtension(scriptFile.Name, ".txt");
			FileInfo logFile = new FileInfo(Path.Combine(OutputDirectory, "Log", logFileName));

			var stopwatch = Stopwatch.StartNew();
			int exitCode = ScriptUtility.RunSqlCmd(TargetServerName, TargetDatabaseName, scriptFile, logFile: logFile);
			stopwatch.Stop();
			if(exitCode != 0)
			{
				hasUpgradeScriptError = true;
				string message = String.Format
				(
					"{0} script failed. Check the log file for error messages:\r\n{1}\r\n",
					scriptFile.Name,
					logFile.FullName
				);
				ErrorWriteLine();
				ErrorWriteLine(message);
				if(!PromptContinue())
				{
					throw new AbortException(message);
				}
			}
			else if(stopwatch.ElapsedMilliseconds > 1000)
			{
				// If the script took more than 1 second, output the elapsed time.
				ConsoleWriteLine("Finished executing {0} script ({1} elapsed).", scriptFile.Name, stopwatch.Elapsed.ToString(elapsedTimeFormat));
			}
		}

		private IEnumerable<FileInfo> AddAndExecuteFiles(TextWriter writer, string filePattern)
		{
			DirectoryInfo directory = GetOutputDirectory();

			// Get the files, in sorted order.
			var scriptFiles = directory.GetFiles(filePattern)
				.OrderBy(f => f.Name)
				.ToList();

			// Add and execute the files.
			foreach(FileInfo scriptFile in scriptFiles)
			{
				AddAndExecute(writer, scriptFile);
			}

			return scriptFiles;
		}

		private bool AfterUpgrade(TextWriter writer)
		{
			var scriptFiles = AddAndExecuteFiles(writer, "AfterUpgrade*.sql");
			return scriptFiles.Any();
		}

		private bool AreDirectoriesIdentical(DirectoryInfo expectedDirectory, DirectoryInfo actualDirectory)
		{
			ConsoleWriteLine("Comparing generated database scripts to verify upgrade scripts succeeded.");
			// Compare the files in the directories and convert to a lookup on status.
			var filesByStatus = DirectoryComparer.Compare(expectedDirectory.FullName, actualDirectory.FullName)
				.ToLookup(f => f.Status);
			bool allIdentical = filesByStatus.All(group => group.Key == FileCompareStatus.Identical);
			if(!allIdentical)
			{
				ErrorWriteLine("\r\nThe upgraded target database does not match the source database.");
				ShowFiles(filesByStatus[FileCompareStatus.SourceOnly], "\r\nMissing (should have been added):");
				ShowFiles(filesByStatus[FileCompareStatus.Modified], "\r\nDifferent (should be identical):");
				ShowFiles(filesByStatus[FileCompareStatus.TargetOnly], "\r\nExtra (should have been removed):");
				ConsoleWriteLine("\r\nTo review file level differences, use a tool such as WinMerge to compare these directories:");
				ConsoleWriteLine("\t{0}", expectedDirectory.FullName);
				ConsoleWriteLine("\t{0}", actualDirectory.FullName);
			}
			return allIdentical;
		}

		private void ConsoleWriteLine()
		{
			lock(consoleLock)
			{
				Console.WriteLine();
			}
		}

		private void ConsoleWriteLine(string value)
		{
			lock(consoleLock)
			{
				Console.WriteLine(value);
			}
		}

		private void ConsoleWriteLine(string format, params object[] arg)
		{
			lock(consoleLock)
			{
				Console.WriteLine(format, arg);
			}
		}

		private void ConsoleWriteLine(ConsoleColor color, string format, params object[] arg)
		{
			lock(consoleLock)
			{
				Console.ForegroundColor = color;
				Console.WriteLine(format, arg);
				Console.ResetColor();
			}
		}

		private void ConsoleWriteLine(ConsoleColor color, string value)
		{
			lock(consoleLock)
			{
				Console.ForegroundColor = color;
				Console.WriteLine(value);
				Console.ResetColor();
			}
		}

		private void CreateDatabase(string serverName, string databaseName, string scriptDirectory)
		{
			ConsoleWriteLine("Creating database '{0}' on server '{1}'.", databaseName, serverName);
			FileInfo scriptFile = new FileInfo(Path.Combine(scriptDirectory, "CreateDatabaseObjects.sql"));
			FileInfo logFile = new FileInfo(Path.Combine(OutputDirectory, "Log", GetSafeFileName(databaseName) + ".txt"));

			var stopwatch = Stopwatch.StartNew();
			var variables = new Dictionary<string, string>
			{
				{ "DBNAME", databaseName },
				{ "DROPDB", "true" }
			};
			int exitCode = ScriptUtility.RunSqlCmd(TargetServerName, null, scriptFile, variables, logFile);
			stopwatch.Stop();
			if(exitCode != 0)
			{
				string message = String.Format
				(
					"Failed to create database {0}. Check the log file for error messages:\r\n{1}\r\n",
					databaseName,
					logFile.FullName
				);
				ErrorWriteLine();
				ErrorWriteLine(message);
				throw new AbortException(message);
			}
			else if(stopwatch.ElapsedMilliseconds > 1000)
			{
				// If the script took more than 1 second, output the elapsed time.
				ConsoleWriteLine("Finished creating database {0} ({1} elapsed).", databaseName, stopwatch.Elapsed.ToString(elapsedTimeFormat));
			}
		}

		/// <summary>
		/// Create the source and target databases based on the scripts in the source and target directories (if provided).
		/// </summary>
		/// <remarks>
		/// If the <see cref="SourceDirectory"/> then the source database is assumed to already exist.
		/// If the <see cref="TargetDirectory"/> then the target database is assumed to already exist.
		/// To improve performance this method creates the databases in parallel.
		/// </remarks>
		private void CreateDatabases()
		{
			Task createSourceDatabaseTask = null;

			// If a SourceDirectory was specified, then create the source database (in parallel).
			if(!String.IsNullOrEmpty(this.SourceDirectory))
				createSourceDatabaseTask = Task.Run(() => CreateDatabase(SourceServerName, SourceDatabaseName, SourceDirectory));

			// If a TargetDirectory was specified, then create the target database.
			if(!String.IsNullOrEmpty(this.TargetDirectory))
				CreateDatabase(TargetServerName, TargetDatabaseName, TargetDirectory);

			// Wait for the parallel task to finish creating the source (if the task was even created).
			if(createSourceDatabaseTask != null)
				createSourceDatabaseTask.Wait();
		}

		/// <summary>
		/// Creates a text writer for the script file.
		/// </summary>
		/// <remarks>
		/// There are several locations in the UpgradeScripter class that need a text writer
		/// to write to a script file. This method standardizes the creation of the writer
		/// with the correct options (particularly the encoding).
		/// </remarks>
		private TextWriter CreateText(FileInfo file)
		{
			return new StreamWriter(file.FullName, false, Encoding.Default);
		}

		private bool DataPrep(TextWriter writer)
		{
			var scriptFiles = AddAndExecuteFiles(writer, "DataPrep*.sql");
			return scriptFiles.Any();
		}

		private bool DataUpgrade(TextWriter upgradeWriter, FileInfo dataUpgradeFile)
		{
			bool hasDataChanges;
			var stopwatch = Stopwatch.StartNew();

			ConsoleWriteLine("Comparing data and generating {0} script.", dataUpgradeFile.Name);

			DataUpgradeScripter dataUpgradeScripter = new DataUpgradeScripter
			{
				SourceServerName = SourceServerName,
				SourceDatabaseName = SourceDatabaseName,
				TargetServerName = TargetServerName,
				TargetDatabaseName = TargetDatabaseName
			};

			using(TextWriter writer = CreateText(dataUpgradeFile))
			{
				hasDataChanges = dataUpgradeScripter.GenerateScript(writer);
			}

			stopwatch.Stop();

			// If there are data changes then add the data upgrade script
			// to the upgrade script and run it before the after upgrade script.
			// Otherwise, delete the file.
			if(hasDataChanges)
			{
				ConsoleWriteLine
				(
					"Finished comparing data and generating {0} script ({1} elapsed).",
					dataUpgradeFile.Name,
					stopwatch.Elapsed.ToString(elapsedTimeFormat)
				);
				AddAndExecute(upgradeWriter, dataUpgradeFile);
			}
			else
			{
				ConsoleWriteLine("No data changes detected ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
				dataUpgradeFile.Delete();
			}
			return hasDataChanges;
		}

		private void ErrorWriteLine()
		{
			lock(consoleLock)
			{
				Console.Error.WriteLine();
			}
		}

		private void ErrorWriteLine(string value)
		{
			ErrorWriteLine(ConsoleColor.Red, value);
		}

		private void ErrorWriteLine(ConsoleColor color, string value)
		{
			lock(consoleLock)
			{
				Console.ForegroundColor = color;
				Console.Error.WriteLine(value);
				Console.ResetColor();
			}
		}

		private void ErrorWriteLine(string format, params object[] arg)
		{
			ErrorWriteLine(ConsoleColor.Red, format, arg);
		}

		private void ErrorWriteLine(ConsoleColor color, string format, params object[] arg)
		{
			lock(consoleLock)
			{
				Console.ForegroundColor = color;
				Console.Error.WriteLine(format, arg);
				Console.ResetColor();
			}
		}

		private DacPackage ExtractSource(SchemaUpgradeScripter scripter, FileInfo packageFile)
		{
			var stopwatch = Stopwatch.StartNew();
			ConsoleWriteLine("Extracting the source package.");
			var package = scripter.ExtractSource(packageFile);
			stopwatch.Stop();
			ConsoleWriteLine("Finished extracting the source package ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return package;
		}

		private DacPackage ExtractTarget(SchemaUpgradeScripter scripter, FileInfo packageFile)
		{
			var stopwatch = Stopwatch.StartNew();
			ConsoleWriteLine("Extracting the target package.");
			var package = scripter.ExtractTarget(packageFile);
			stopwatch.Stop();
			ConsoleWriteLine("Finished extracting the target package ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
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
			while(Directory.Exists(fileScripter.OutputDirectory))
			{
				try
				{
					Directory.Delete(fileScripter.OutputDirectory, true);
				}
				catch(Exception ex)
				{
					string message = String.Format("Failed to delete the directory {0}\r\n{1}", fileScripter.OutputDirectory, ex.Message);
					ErrorWriteLine(message);
					if(!PromptRetry())
						throw new AbortException(message, ex);
				}
			}

			string logFileName = outputDirectory.Name + ".txt";
			FileInfo logFile = new FileInfo(Path.Combine(this.OutputDirectory, "Log", logFileName));
			// Ensure the Log directory exists.
			logFile.Directory.Create();
			using(TextWriter logWriter = CreateText(logFile))
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
							ConsoleWriteLine(lastProgressMessage);
							lastProgressMessage = null;
						}
						ErrorWriteLine(e.Message);
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
			string report = schemaUpgradeScripter.GenerateReport();
			reportFile.Directory.Create();
			XmlReaderSettings readerSettings = new XmlReaderSettings { IgnoreWhitespace = true };
			XmlWriterSettings writerSettings = new XmlWriterSettings { Indent = true, IndentChars = "\t" };
			using(XmlReader reader = XmlReader.Create(new StringReader(report), readerSettings))
			using(XmlWriter writer = XmlWriter.Create(reportFile.FullName, writerSettings))
			{
				if(reader.MoveToContent() != XmlNodeType.None)
				{
					writer.WriteNode(reader, false);
				}
			}
		}

		private DirectoryInfo GenerateSourceCreateScripts(DirectoryInfo directory)
		{
			var stopwatch = Stopwatch.StartNew();
			ConsoleWriteLine("Generating clean scripts from source database (for verification).");
			GenerateCreateScripts(SourceServerName, SourceDatabaseName, directory);
			stopwatch.Stop();
			ConsoleWriteLine("Finished generating scripts from source database ({0}).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return directory;
		}

		private DirectoryInfo GenerateTargetCreateScripts(DirectoryInfo directory)
		{
			var stopwatch = Stopwatch.StartNew();
			ConsoleWriteLine("Generating scripts from upgraded target database (for verification).");
			GenerateCreateScripts(TargetServerName, TargetDatabaseName, directory);
			stopwatch.Stop();
			ConsoleWriteLine("Finished generating scripts from upgraded target database ({0}).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
			return directory;
		}

		private DirectoryInfo GetOutputDirectory()
		{
			if(OutputDirectory == String.Empty)
				return new DirectoryInfo(Directory.GetCurrentDirectory());
			else
				return new DirectoryInfo(OutputDirectory);
		}

		/// <summary>
		/// Gets a safe file name by replacing sequences of invalid characters with an underscore.
		/// </summary>
		private string GetSafeFileName(string unsafeFileName)
		{
			char[] invalidChars = Path.GetInvalidFileNameChars();
			string invalidPattern = "[" + Regex.Escape(new String(invalidChars)) + "]+";
			return Regex.Replace(unsafeFileName, invalidPattern, "_");
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
				ConsoleWriteLine(ConsoleColor.White, "\r\nReview issues for potential data loss:");
				foreach(var issue in dataIssues)
				{
					ConsoleWriteLine(ConsoleColor.Yellow, "\t{0}", (string)issue.Attribute("Value"));
				}
			}
		}

		private void OutputSummaryMessage(bool upgradedTargetMatchesSource, TimeSpan elapsed)
		{
			ConsoleWriteLine();
			if(hasUpgradeScriptError)
			{
				ErrorWriteLine("Upgrade scripts failed to execute. Review the scripts that failed and add or correct manual steps in a SchemaPrep.sql, DataPrep.sql or AfterUpgrade.sql script.");
			}
			else if(upgradedTargetMatchesSource)
			{
				if(hasUpgradeScript)
				{
					ConsoleWriteLine(ConsoleColor.White, "Upgrade scripts successfully generated and verified.");
				}
				else
					ConsoleWriteLine(ConsoleColor.White, "No upgrade necessary.");
			}
			else
			{
				ErrorWriteLine("Upgrade scripts failed verification. Review the files that failed verification and add manual steps to a SchemaPrep.sql, DataPrep.sql or AfterUpgrade.sql script.");
			}
			ConsoleWriteLine("({0} total elapsed)", elapsed.ToString(elapsedTimeFormat));
		}

		/// <summary>
		/// Prompts the user whether to continue (y/n).
		/// </summary>
		/// <remarks>
		/// This method continues prompting until the user presses y, Y, n, or N.
		/// </remarks>
		/// <returns>
		/// true if the user presses 'y'; false if the user presses 'n'
		/// </returns>
		private bool PromptContinue()
		{
			bool response = PromptYesNo("Continue (y/n)? ");
			if(response)
				ConsoleWriteLine("\r\nContinuing...");
			else
				ConsoleWriteLine("\r\nAborting...");
			return response;
		}

		/// <summary>
		/// Prompts the user whether to retry (y/n).
		/// </summary>
		/// <remarks>
		/// This method continues prompting until the user presses y, Y, n, or N.
		/// </remarks>
		/// <returns>
		/// true if the user presses 'y'; false if the user presses 'n'
		/// </returns>
		private bool PromptRetry()
		{
			bool response = PromptYesNo("Retry (y/n)? ");
			if(response)
				ConsoleWriteLine("\r\nRetrying...");
			else
				ConsoleWriteLine("\r\nAborting...");
			return response;
		}

		/// <summary>
		/// Prompts the user with a yes/no question.
		/// </summary>
		/// <remarks>
		/// This method continues prompting until the user presses y, Y, n, or N.
		/// </remarks>
		/// <returns>
		/// true if the user presses 'y'; false if the user presses 'n'
		/// </returns>
		private bool PromptYesNo(string prompt)
		{
			// Since there are parallel tasks, ensure that we only prompt
			// the user for on parallel task at a time.
			lock(consoleLock)
			{
				// Clear any keys pressed before the prompt was displayed.
				// Use a for loop instead of a while loop to avoid any chance of an infinite loop.
				for(int i = 0; i < 1000 && Console.KeyAvailable; i++)
					Console.Read();
				while(true)
				{
					Console.Write(prompt);
					ConsoleKeyInfo key = Console.ReadKey();
					char ch = key.KeyChar;
					if(ch == 'y' || ch == 'Y')
						return true;
					else if(ch == 'n' || ch == 'N')
						return false;
				}
				Console.WriteLine();
			}
		}

		private bool SchemaPrep(TextWriter writer)
		{
			var scriptFiles = AddAndExecuteFiles(writer, "SchemaPrep*.sql");
			return scriptFiles.Any();
		}
		private bool SchemaUpgrade(TextWriter upgradeWriter, SchemaUpgradeScripter schemaUpgradeScripter, FileInfo schemaUpgradeFile, FileInfo schemaUpgradeReportFile, bool dropObjectsNotInSource)
		{
			bool hasSchemaChanges;
			var stopwatch = Stopwatch.StartNew();

			ConsoleWriteLine("Comparing schema and generating {0} script.", schemaUpgradeFile.Name);

			// Generate the schema upgrade report in parallel.
			Task generateSchemaUpgradeReportTask = Task.Run(() => GenerateSchemaUpgradeReport(schemaUpgradeScripter, schemaUpgradeReportFile));

			using(TextWriter writer = CreateText(schemaUpgradeFile))
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
				ConsoleWriteLine
				(
					@"Finished comparing schema and generating {0} script. ({1} elapsed).",
					schemaUpgradeFile.Name,
					stopwatch.Elapsed.ToString(elapsedTimeFormat)
				);
				AddAndExecute(upgradeWriter, schemaUpgradeFile);
			}
			else
			{
				ConsoleWriteLine("No schema changes detected ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
				schemaUpgradeFile.Delete();
			}
			return hasSchemaChanges;
		}

		private bool SchemaUpgradeFinal(TextWriter upgradeWriter, SchemaUpgradeScripter schemaUpgradeScripter, FileInfo schemaFinalFile)
		{
			bool hasFinalSchemaChanges;
			var stopwatch = Stopwatch.StartNew();

			ConsoleWriteLine("Checking for final schema changes and generating {0} script.", schemaFinalFile.Name);

			// Don't use the TargetPackage because the database has been modified by other scripts.
			// The SchemaUpgradeScripter will compare directly against the target database instead of the target package.
			schemaUpgradeScripter.TargetPackage = null;

			using(TextWriter writer = CreateText(schemaFinalFile))
			{
				hasFinalSchemaChanges = schemaUpgradeScripter.GenerateScript(writer);
			}

			stopwatch.Stop();

			// If there are final schema changes then add the schema final script
			// to the upgrade script and run it on the target before verifying the upgrade.
			// Otherwise, delete the file.
			if(hasFinalSchemaChanges)
			{
				ConsoleWriteLine
				(
					@"Finished checking for final schema changes and generating {0} script ({1} elapsed).",
					schemaFinalFile.Name,
					stopwatch.Elapsed.ToString(elapsedTimeFormat)
				);
				AddAndExecute(upgradeWriter, schemaFinalFile);
			}
			else
			{
				ConsoleWriteLine("No final schema changes detected ({0} elapsed).", stopwatch.Elapsed.ToString(elapsedTimeFormat));
				schemaFinalFile.Delete();
			}
			return hasFinalSchemaChanges;
		}

		private void ShowFiles(IEnumerable<FileCompareInfo> files, string message)
		{
			if(files.Any())
			{
				ErrorWriteLine(ConsoleColor.Gray, message);
				foreach(var file in files)
				{
					ErrorWriteLine(ConsoleColor.Yellow, "\t{0}", file.RelativePath);
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
