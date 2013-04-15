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
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using Microsoft.SqlServer.Management.Smo;

namespace Mercent.SqlServer.Management
{
	public class ScriptUtility
	{
		private Database database;

		public ScriptUtility(Database database)
		{
			if(database == null)
				throw new ArgumentNullException("database");
			this.database = database;
		}

		public static string ByteArrayToHexLiteral(byte[] a)
		{
			if(a == null)
			{
				return null;
			}
			StringBuilder builder = new StringBuilder(a.Length * 2);
			builder.Append("0x");
			foreach(byte b in a)
			{
				builder.Append(b.ToString("X02", System.Globalization.CultureInfo.InvariantCulture));
			}
			return builder.ToString();
		}

		public static string EscapeChar(string s, char c)
		{
			return s.Replace(new string(c, 1), new string(c, 2));
		}

		public static SqlDataType GetBaseSqlDataType(UserDefinedDataType uddt)
		{
			return (SqlDataType)Enum.Parse(typeof(SqlDataType), uddt.SystemType, true);
		}

		public static DataType GetDataType(SqlDataType sqlDataType, int precision, int scale, int maxLength)
		{
			switch(sqlDataType)
			{
				case SqlDataType.Binary:
				case SqlDataType.Char:
				case SqlDataType.NChar:
				case SqlDataType.NVarChar:
				case SqlDataType.VarBinary:
				case SqlDataType.VarChar:
				case SqlDataType.NVarCharMax:
				case SqlDataType.VarBinaryMax:
				case SqlDataType.VarCharMax:
					return new DataType(sqlDataType, maxLength);
				case SqlDataType.Decimal:
				case SqlDataType.Numeric:
					return new DataType(sqlDataType, precision, scale);
				default:
					return new DataType(sqlDataType);
			}
		}

		public static string GetSqlLiteral(object sqlValue, SqlDataType sqlDataType)
		{
			if(DBNull.Value == sqlValue || (sqlValue is INullable && ((INullable)sqlValue).IsNull))
				return "NULL";
			switch(sqlDataType)
			{
				case SqlDataType.BigInt:
				case SqlDataType.Decimal:
				case SqlDataType.Int:
				case SqlDataType.Money:
				case SqlDataType.Numeric:
				case SqlDataType.SmallInt:
				case SqlDataType.SmallMoney:
				case SqlDataType.TinyInt:
					return sqlValue.ToString();
				case SqlDataType.Binary:
				case SqlDataType.Image:
				case SqlDataType.Timestamp:
				case SqlDataType.VarBinary:
				case SqlDataType.VarBinaryMax:
					return ByteArrayToHexLiteral(((SqlBinary)sqlValue).Value);
				case SqlDataType.Bit:
					return ((SqlBoolean)sqlValue).Value ? "1" : "0";
				case SqlDataType.Char:
				case SqlDataType.Text:
				case SqlDataType.UniqueIdentifier:
				case SqlDataType.VarChar:
				case SqlDataType.VarCharMax:
					return "'" + EscapeChar(sqlValue.ToString(), '\'') + "'";
				case SqlDataType.Date:
					return "'" + ((DateTime)sqlValue).ToString("yyyy-MM-dd", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.DateTime:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm:ss.fff", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.DateTime2:
					return "'" + ((DateTime)sqlValue).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.DateTimeOffset:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF K", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.NChar:
				case SqlDataType.NText:
				case SqlDataType.NVarChar:
				case SqlDataType.NVarCharMax:
				case SqlDataType.SysName:
				case SqlDataType.UserDefinedType:
					return "N'" + EscapeChar(sqlValue.ToString(), '\'') + "'";
				case SqlDataType.Float:
					return ((SqlDouble)sqlValue).Value.ToString("r");
				case SqlDataType.Real:
					return ((SqlSingle)sqlValue).Value.ToString("r");
				case SqlDataType.SmallDateTime:
					return "'" + ((SqlDateTime)sqlValue).Value.ToString("yyyy-MM-dd HH:mm", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.Time:
					return "'" + ((TimeSpan)sqlValue).ToString("g", DateTimeFormatInfo.InvariantInfo) + "'";
				case SqlDataType.Xml:
					XmlWriterSettings settings = new XmlWriterSettings();
					settings.OmitXmlDeclaration = true;
					settings.Indent = true;
					settings.IndentChars = "\t";
					settings.NewLineOnAttributes = true;
					using(XmlReader xmlReader = ((SqlXml)sqlValue).CreateReader())
					{
						using(StringWriter stringWriter = new StringWriter())
						{
							using(XmlWriter xmlWriter = XmlWriter.Create(stringWriter, settings))
							{
								while(xmlReader.Read())
								{
									xmlWriter.WriteNode(xmlReader, false);
								}
							}
							return "N'" + EscapeChar(stringWriter.ToString(), '\'') + "'";
						}
					}
				//case SqlDataType.Geography:
				//case SqlDataType.Geometry:
				//case SqlDataType.HierarchyId:	
				default:
					throw new ApplicationException("Unsupported type :" + sqlDataType.ToString());
			}
		}

		/// <summary>
		/// Gets the SqlServerVersion for the specified CompatibilityLevel.
		/// </summary>
		public static SqlServerVersion GetSqlServerVersion(CompatibilityLevel compatibilityLevel)
		{
			switch(compatibilityLevel)
			{
				case CompatibilityLevel.Version100:
					return SqlServerVersion.Version100;
				// If the compatibility level is 90 (2005) then we target version 90
				case CompatibilityLevel.Version90:
					return SqlServerVersion.Version90;
				// If the compatibility level is 80 (2000) then we target version 80
				// If the compatibility level is 80 (2000) or less then we target version 80.
				case CompatibilityLevel.Version80:
				case CompatibilityLevel.Version70:
				case CompatibilityLevel.Version65:
				case CompatibilityLevel.Version60:
					return SqlServerVersion.Version80;
				// Default target version 110 (2012)
				default:
					return SqlServerVersion.Version110;
			}
		}

		public static string MakeSqlBracket(string name)
		{
			return "[" + EscapeChar(name, ']') + "]";
		}

		public static int RunSqlCmd(string args, DirectoryInfo workingDirectory = null)
		{
			using(Process process = StartSqlCmd(args, workingDirectory))
			{
				process.WaitForExit();
				return process.ExitCode;
			}
		}

		public static int RunSqlCmd(string serverName, string databaseName, FileInfo scriptFile, IDictionary<string, string> variables = null, FileInfo logFile = null)
		{
			using(Process process = StartSqlCmd(serverName, databaseName, scriptFile, variables, logFile))
			{
				process.WaitForExit();
				return process.ExitCode;
			}
		}

		public static Process StartSqlCmd(string serverName, string databaseName, FileInfo scriptFile, IDictionary<string, string> variables = null, FileInfo logFile = null)
		{
			if(serverName == null)
				throw new ArgumentNullException("serverName");
			if(scriptFile == null)
				throw new ArgumentNullException("scriptFile");

			DirectoryInfo workingDirectory = scriptFile.Directory;

			StringBuilder args = new StringBuilder();
			args.AppendFormat(" -S \"{0}\"", serverName);
			if(databaseName != null)
				args.AppendFormat(" -d \"{0}\"", databaseName);
			args.Append(" -E -b -r -I");
			args.AppendFormat(" -i \"{0}\"", scriptFile.FullName);
			if(variables != null && variables.Count > 0)
			{
				args.Append(" -v");
				foreach(var variable in variables)
				{
					args.AppendFormat(" {0}=\"{1}\"", variable.Key, variable.Value);
				}
			}

			return StartSqlCmd(args.ToString(), workingDirectory, logFile);
		}

		public static Process StartSqlCmd(string args, DirectoryInfo workingDirectory = null, FileInfo logFile = null)
		{
			bool logOutput = logFile != null;
			ProcessStartInfo startInfo = new ProcessStartInfo
			{
				FileName = "sqlcmd.exe",
				Arguments = args,
				UseShellExecute = false,
				RedirectStandardError = logOutput,
				RedirectStandardOutput = logOutput,
				WorkingDirectory = workingDirectory.FullName
			};

			Process process = new Process { StartInfo = startInfo };
			TextWriter logWriter = null;
			try
			{
				if(logOutput)
				{
					// Create the log file directory (if the directory already exists, Create() does nothing).
					logFile.Directory.Create();
					
					// Create the log writer.
					logWriter = logFile.CreateText();

					string lastOutputLine = null;
					// Attach events handlers to write to and close the log writer.
					process.EnableRaisingEvents = true;
					// Standard error goes to the log and console error out.
					process.ErrorDataReceived += (s, e) =>
					{
						if(e.Data != null)
						{
							logWriter.WriteLine(e.Data);
							// First output the last standard out line.
							// This will hopefully give us the last PRINT statement.
							if(lastOutputLine != null)
							{
								Console.WriteLine(lastOutputLine);
								lastOutputLine = null;
							}
							Console.Error.WriteLine(e.Data);
						}
					};
					// Standard out just goes to the log.
					process.OutputDataReceived += (s, e) =>
					{
						if(e.Data != null)
						{
							// Capture the last line so we can show it above
							// any error messages.
							lastOutputLine = e.Data;
							logWriter.WriteLine(e.Data);
						}
					};
					process.Disposed += (s, e) => logWriter.Close();
					process.Start();
					process.BeginErrorReadLine();
					process.BeginOutputReadLine();
				}
				else
				{
					process.Start();
				}
			}
			catch(Exception)
			{
				// If an exception occured starting the process, then Exited event will never fire
				// so close the log writer now.
				if(logWriter != null)
					logWriter.Close();
				throw;
			}
			return process;
		}

		public DataType GetBaseDataType(DataType dataType)
		{
			if(dataType.SqlDataType != SqlDataType.UserDefinedDataType)
				return dataType;

			UserDefinedDataType uddt = database.UserDefinedDataTypes[dataType.Name, dataType.Schema];
			SqlDataType baseSqlDataType = GetBaseSqlDataType(uddt);
			DataType baseDataType = GetDataType(baseSqlDataType, uddt.NumericPrecision, uddt.NumericScale, uddt.MaxLength);
			return baseDataType;
		}

		public SqlDataType GetBaseSqlDataType(DataType dataType)
		{
			if(dataType.SqlDataType != SqlDataType.UserDefinedDataType)
				return dataType.SqlDataType;

			UserDefinedDataType uddt = database.UserDefinedDataTypes[dataType.Name, dataType.Schema];
			return GetBaseSqlDataType(uddt);
		}

		public string GetDataTypeAsString(DataType dataType)
		{
			StringBuilder sb = new StringBuilder();
			switch(dataType.SqlDataType)
			{
				case SqlDataType.Binary:
				case SqlDataType.Char:
				case SqlDataType.NChar:
				case SqlDataType.NVarChar:
				case SqlDataType.VarBinary:
				case SqlDataType.VarChar:
					sb.Append(MakeSqlBracket(dataType.Name));
					sb.Append('(');
					sb.Append(dataType.MaximumLength);
					sb.Append(')');
					break;
				case SqlDataType.NVarCharMax:
				case SqlDataType.VarBinaryMax:
				case SqlDataType.VarCharMax:
					sb.Append(MakeSqlBracket(dataType.Name));
					sb.Append("(max)");
					break;
				case SqlDataType.Decimal:
				case SqlDataType.Numeric:
					sb.Append(MakeSqlBracket(dataType.Name));
					sb.AppendFormat("({0},{1})", dataType.NumericPrecision, dataType.NumericScale);
					break;
				case SqlDataType.UserDefinedDataType:
					// For a user defined type, get the base data type as string
					DataType baseDataType = GetBaseDataType(dataType);
					return GetDataTypeAsString(baseDataType);
				case SqlDataType.Xml:
					sb.Append("[xml]");
					if(!String.IsNullOrEmpty(dataType.Name))
						sb.AppendFormat("({0} {1})", dataType.XmlDocumentConstraint, dataType.Name);
					break;
				default:
					sb.Append(MakeSqlBracket(dataType.Name));
					break;
			}
			return sb.ToString();
		}

		public string GetSqlVariantLiteral(object sqlValue, SqlString baseType, SqlInt32 precision, SqlInt32 scale, SqlString collation, SqlInt32 maxLength)
		{
			if(DBNull.Value == sqlValue || (sqlValue is INullable && ((INullable)sqlValue).IsNull))
				return "NULL";

			SqlDataType sqlDataType = (SqlDataType)Enum.Parse(typeof(SqlDataType), baseType.Value, true);
			// The SQL_VARIANT_PROPERTY MaxLength is returned in bytes.
			// For nchar and nvarchar we need to halve this to get the max length used when specifying the type.
			// Note that I also included ntext and nvarcharmax in the case statement even though they can't be used
			// in a sql_varaint type.
			int adjustedMaxLength;
			switch(sqlDataType)
			{
				case SqlDataType.NChar:
				case SqlDataType.NText:
				case SqlDataType.NVarChar:
				case SqlDataType.NVarCharMax:
					adjustedMaxLength = maxLength.Value / 2;
					break;
				default:
					adjustedMaxLength = maxLength.Value;
					break;
			}
			DataType dataType = GetDataType(sqlDataType, precision.Value, scale.Value, adjustedMaxLength);
			string literal = "CAST(CAST(" + GetSqlLiteral(sqlValue, sqlDataType) + " AS " + GetDataTypeAsString(dataType) + ")";
			if(!collation.IsNull)
				literal += " COLLATE " + collation.Value;
			literal += " AS [sql_variant])";
			return literal;
		}
	}
}
