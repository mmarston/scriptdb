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
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Mercent.SqlServer.Management.IO;
using Microsoft.SqlServer.Management.Smo;

namespace Mercent.SqlServer.Management.Upgrade.Data
{
	public class DataUpgradeScripter
	{
		private IDictionary<TableIdentifier, TableInfo> changedTables = new Dictionary<TableIdentifier, TableInfo>(TableIdentifierComparer.OrdinalIgnoreCase);
		private List<ForeignKey> foreignKeysToDisable = new List<ForeignKey>();
		private List<Index> indexesToDisable = new List<Index>();
		private DataUpgradeOptions options;
		private List<TableInfo> orderedTables = new List<TableInfo>();
		private Database sourceSmoDatabase;
		private List<Trigger> triggersToDisable = new List<Trigger>();
		private ScriptUtility utility;
		private TextWriter writer;
		
		public string SourceDatabaseName { get; set; }

		/// <summary>
		/// Directory for source database scripts (optional).
		/// </summary>
		/// <remarks>
		/// This is the directory containing the output from <see cref="FileScripter.Script"/>.
		/// This is used to optimize the data compare steps by comparing
		/// the data files first. If the data files are equal then
		/// there is no need to query the database table.
		/// </remarks>
		public string SourceDirectory { get; set; }
		public string SourceServerName { get; set; }
		public string TargetDatabaseName { get; set; }

		/// <summary>
		/// Directory for target database scripts (optional).
		/// </summary>
		/// <remarks>
		/// This is the directory containing the output from <see cref="FileScripter.Script"/>.
		/// This is used to optimize the data compare steps by comparing
		/// the data files first. If the data files are equal then
		/// there is no need to query the database table.
		/// </remarks>
		public string TargetDirectory { get; set; }
		public string TargetServerName { get; set; }

		public bool GenerateScript(TextWriter writer, DataUpgradeOptions options = null)
		{
			if(writer == null)
				throw new ArgumentNullException("writer");

			VerifyProperties();

			this.writer = writer;

			if(options == null)
				options = new DataUpgradeOptions();

			this.options = options;

			// Clear the collections (in case this method is called multiple times on the same instance).
			ClearCollections();

			try
			{
				Server server = new Server(SourceServerName);
				this.sourceSmoDatabase = server.Databases[SourceDatabaseName];
				this.utility = new ScriptUtility(this.sourceSmoDatabase);

				// Compare the source and target tables and generate DELETE, INSERT, and UPDATE statements.
				GenerateStatements();

				// If there are not any changed tables then return false.
				if(!changedTables.Any())
					return false;

				SetOptions();

				CheckForIndexesToDisable();

				// Resolve the order that the changed tables should be scripted in.
				// (Try to script a primary table before a table that references it using a foreign key).
				ResolveOrder();

				DisableForeignKeys();

				DisableIndexes();

				DisableTriggers();

				DeleteRows();

				UpdateRows();

				InsertRows();

				EnableTriggers();

				RebuildIndexes();

				EnableForeignKeys();

				// Return true, indicating that there are changed tables.
				return true;
			}
			finally
			{
				ClearCollections();
				sourceSmoDatabase = null;
				writer = null;
			}
		}

		private static void AddTablesWithData(ICollection<TableIdentifier> collection, SqlConnection connection)
		{
			if(collection == null)
				throw new ArgumentNullException("collection");
			if(connection == null)
				throw new ArgumentNullException("connection");
			if(connection.State != ConnectionState.Open)
				connection.Open();
			string query =
@"SELECT s.name AS SchemaName, t.name AS TableName
FROM sys.tables AS t
	INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE EXISTS
(
	SELECT *
	FROM sys.partitions AS p
	WHERE p.object_id = t.object_id
		AND p.rows > 0
);";
			using(SqlCommand command = new SqlCommand(query, connection))
			using(SqlDataReader reader = command.ExecuteReader())
			{
				while(reader.Read())
				{
					string schemaName = reader.GetString(0);
					string tableName = reader.GetString(1);
					collection.Add(new TableIdentifier(schemaName, tableName));
				}
			}
		}

		private static string EscapeStringLiteral(string s)
		{
			return ScriptUtility.EscapeChar(s, '\'');
		}

		private static bool HasAnyColumnsUpdatedMultipleTimes(Index index, TableInfo tableInfo)
		{
			// Loop through the indexed columns.
			foreach(IndexedColumn indexedColumn in index.IndexedColumns)
			{
				// Skip the column if it is an "included" column (stored within the index leaf
				// but not part of the keys in the index).
				if(indexedColumn.IsIncluded)
					continue;
				// Get the column information.
				ColumnInfo columnInfo = tableInfo.Columns.Get(indexedColumn.Name);
				// If the column was updated in more than one row then return true.
				if(columnInfo != null && columnInfo.UpdatedRowCount > 1)
					return true;
			}
			return false;
		}

		private static string MakeSqlBracket(string name)
		{
			return ScriptUtility.MakeSqlBracket(name);
		}

		/// <summary>
		/// Checks if the source and target script data files for a table are equal.
		/// </summary>
		/// <remarks>
		/// This is used as an optimization to avoid querying the database tables.
		/// If the <see cref="SourceDirectory"/> or <see cref="TargetDirectory"/> are not specified
		/// or the files do not exist then this returns false.
		/// </remarks>
		[Obsolete("This method is not currently used. It was an optimization used to skip calling tablediff.exe.")]
		private bool AreDataFilesEqual(TableIdentifier table)
		{
			if(String.IsNullOrEmpty(SourceDirectory) || String.IsNullOrEmpty(TargetDirectory))
				return false;
			string[] dataFileExtensions = { ".txt", ".dat", ".sql" };
			// The file paths below include the directory and file name without extension.
			string sourceDataFile = Path.Combine(SourceDirectory, "Schemas", table.Schema, "Data", table.Name);
			string targetDataFile = Path.Combine(TargetDirectory, "Schemas", table.Schema, "Data", table.Name);
			foreach(string extension in dataFileExtensions)
			{
				// Check if the files are equal.
				// Note that the Equals method checks if the files exist,
				// so we don't need to add the extra check here.
				if(FileComparer.Equals(sourceDataFile + extension, targetDataFile + extension))
					return true;
			}
			return false;
		}

		/// <summary>
		/// Check to see if any rows will be updated with a unique value of another row.
		/// </summary>
		/// <remarks>
		/// This check is to avoid data updates that will temporarily result in unique violations.
		/// When this scenario is detected we need to disable the unique index and any foreign
		/// keys that use it.
		/// </remarks>
		private void CheckForIndexesToDisable()
		{
			// Loop through the changed tables.
			foreach(var tableInfo in changedTables.Values)
			{
				// Loop through the table's indexes.
				foreach(Index index in tableInfo.Table.Indexes)
				{
					// Check if we should disable the index.
					// If so, add it to the list of indexes to disable.
					if(ShouldDisable(index, tableInfo))
						indexesToDisable.Add(index);
				}
			}
		}

		private void ClearCollections()
		{
			changedTables.Clear();
			foreignKeysToDisable.Clear();
			indexesToDisable.Clear();
			orderedTables.Clear();
			triggersToDisable.Clear();
		}

		private ColumnInfo CreateColumnInfo(Column column, bool inKey)
		{
			SqlDataType sqlDataType = utility.GetBaseSqlDataType(column.DataType);
			return new ColumnInfo
			{
				Column = column,
				QuotedName = MakeSqlBracket(column.Name),
				InKey = inKey,
				SqlDataType = sqlDataType
			};
		}

		private TableInfo CreateTableInfo(Table table)
		{
			var tableInfo = new TableInfo
			{
				Table = table,
				QualifiedName = MakeSqlBracket(table.Schema) + "." + MakeSqlBracket(table.Name)
			};

			IList<ColumnInfo> columns = tableInfo.Columns;
			IList<Column> keyColumns = GetKeyColumns(tableInfo.Table);

			foreach(Column column in tableInfo.Table.Columns)
			{
				if(!column.Computed && column.DataType.SqlDataType != SqlDataType.Timestamp)
				{
					bool inKey = keyColumns.Contains(column);
					ColumnInfo columnInfo = CreateColumnInfo(column, inKey);
					columns.Add(columnInfo);
					if(inKey)
						tableInfo.KeyColumns.Add(columnInfo);
					if(column.Identity)
						tableInfo.HasIdentityColumn = true;
				}
			}
			return tableInfo;
		}

		private void DeleteRows()
		{
			// Delete from tables in reverse order.
			for(int i = orderedTables.Count - 1; i >= 0; i--)
			{
				var tableInfo = orderedTables[i];
				var statements = tableInfo.DeleteStatements;
				if(statements.Count > 0)
				{
					WriteBatch("PRINT 'Deleting {0} row(s) from {1}.';", statements.Count, EscapeStringLiteral(tableInfo.QualifiedName));
					WriteStatements(statements);
				}
			}
		}

		private void DisableForeignKeys()
		{
			// Check for tables that couldn't be resolved in dependency order.
			// This can occur if a table references itself or if there are any dependency cycles.
			foreach(var tableInfo in orderedTables)
			{
				foreach(var foreignKeyInfo in tableInfo.Relationships)
				{
					if(foreignKeyInfo.PrimaryTable == tableInfo && foreignKeyInfo.ShouldDisable)
					{
						foreignKeysToDisable.Add(foreignKeyInfo.ForeignKey);
					}
				}
			}

			if(foreignKeysToDisable.Any())
			{
				WriteBatch("PRINT 'Disabling foreign keys.';");
				foreach(ForeignKey foreignKey in foreignKeysToDisable)
				{
					WriteBatch
					(
						"ALTER TABLE {0} NOCHECK CONSTRAINT {1};",
						foreignKey.Parent.QualifiedName(),
						MakeSqlBracket(foreignKey.Name)
					);
				}
			}
		}

		private void DisableIndexes()
		{
			if(indexesToDisable.Any())
			{
				WriteBatch("PRINT 'Disabling unique indexes.';");
				foreach(Index index in indexesToDisable)
				{
					Table table = (Table)index.Parent;
					WriteBatch
					(
						"ALTER INDEX {0} ON {1} DISABLE;",
						MakeSqlBracket(index.Name),
						table.QualifiedName()
					);
				}
			}
		}

		private void DisableTriggers()
		{
			// Don't disable triggers if configured not to.
			if(!options.DisableTriggers)
				return;

			foreach(var tableInfo in changedTables.Values)
			{
				foreach(Trigger trigger in tableInfo.Table.Triggers)
				{
					if(!trigger.IsEnabled)
						continue;
					if
					(
						(trigger.Delete && tableInfo.DeleteStatements.Count > 0)
						|| (trigger.Insert && tableInfo.InsertStatements.Count > 0)
						|| (trigger.Update && tableInfo.UpdateStatements.Count > 0)
					)
					{
						triggersToDisable.Add(trigger);
					}
				}
			}

			if(triggersToDisable.Any())
			{
				WriteBatch("PRINT 'Disabling triggers.';");
				foreach(Trigger trigger in triggersToDisable)
				{
					Table table = (Table)trigger.Parent;
					WriteBatch
					(
						"DISABLE TRIGGER {0}.{1} ON {0}.{2};",
						MakeSqlBracket(table.Schema),
						MakeSqlBracket(trigger.Name),
						MakeSqlBracket(table.Name)
					);
				}
			}
		}

		private void EnableForeignKeys()
		{
			foreach(ForeignKey foreignKey in foreignKeysToDisable)
			{
				string qualifiedTableName = foreignKey.Parent.QualifiedName();
				string quotedForeignKeyName = MakeSqlBracket(foreignKey.Name);
				WriteBatch
				(
					"PRINT 'Enabling foreign key {0} on {1}.';",
					EscapeStringLiteral(quotedForeignKeyName),
					EscapeStringLiteral(qualifiedTableName)
				);
				WriteBatch
				(
					"ALTER TABLE {0} WITH {1} CHECK CONSTRAINT {2};",
					qualifiedTableName,
					foreignKey.IsChecked ? "CHECK" : "NOCHECK",
					quotedForeignKeyName
				);
			}
		}

		private void EnableTriggers()
		{
			if(triggersToDisable.Any())
			{
				WriteBatch("PRINT 'Enabling triggers.';");
				foreach(Trigger trigger in triggersToDisable)
				{
					Table table = (Table)trigger.Parent;
					WriteBatch
					(
						"ENABLE TRIGGER {0}.{1} ON {0}.{2};",
						MakeSqlBracket(table.Schema),
						MakeSqlBracket(trigger.Name),
						MakeSqlBracket(table.Name)
					);
				}
			}
		}

		private SqlDataReader ExecuteSourceReader(string commandText)
		{
			SqlConnection connection = GetConnection(SourceServerName, SourceDatabaseName);
			SqlCommand command = new SqlCommand(commandText, connection);
			connection.Open();
			try
			{
				return command.ExecuteReader(CommandBehavior.CloseConnection);
			}
			catch(Exception)
			{
				connection.Close();
				throw;
			}
		}

		private SqlDataReader ExecuteTargetReader(string commandText)
		{
			SqlConnection connection = GetConnection(TargetServerName, TargetDatabaseName);
			SqlCommand command = new SqlCommand(commandText, connection);
			connection.Open();
			try
			{
				return command.ExecuteReader(CommandBehavior.CloseConnection);
			}
			catch(Exception)
			{
				connection.Close();
				throw;
			}
		}

		/// <summary>
		/// Generates the SELECT query that compares the source and target rows.
		/// </summary>
		/// <remarks>
		/// This method also sets the <see cref="ColumnInfo.SourceOrdinal"/>
		/// and <see cref="ColumnInfo.TargetOrdinal"/> properties of the columns
		/// in the <see cref="TableInfo.Columns"/> collection.
		/// </remarks>
		private string GenerateSelectQuery(TableInfo tableInfo)
		{
			StringBuilder selectColumnListBuilder = new StringBuilder();
			string columnDelimiter = ",\r\n\t";

			// We start by selecting 2 special columns to see if it is in the source or target.
			int selectedColumnCount = 2;
			selectColumnListBuilder.AppendFormat("ISNULL([#__InSource], 0){0}ISNULL([#__InTarget], 0)", columnDelimiter);

			foreach(ColumnInfo column in tableInfo.Columns)
			{
				string quotedColumnName = column.QuotedName;
				foreach(char alias in new[] { 's', 't' })
				{
					if(alias == 's')
						column.SourceOrdinal = selectedColumnCount;
					else
						column.TargetOrdinal = selectedColumnCount;

					selectColumnListBuilder.AppendFormat("{0}{1}.{2}", columnDelimiter, alias, quotedColumnName);
					selectedColumnCount++;

					switch(column.SqlDataType)
					{
						case SqlDataType.UserDefinedType:
							selectColumnListBuilder.Append(".ToString()");
							break;
						case SqlDataType.Variant:
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}.{2}, 'BaseType') AS sysname)", columnDelimiter, alias, quotedColumnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}.{2}, 'Precision') AS int)", columnDelimiter, alias, quotedColumnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}.{2}, 'Scale') AS int)", columnDelimiter, alias, quotedColumnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}.{2}, 'Collation') AS sysname)", columnDelimiter, alias, quotedColumnName);
							selectColumnListBuilder.AppendFormat("{0}CAST(SQL_VARIANT_PROPERTY({1}.{2}, 'MaxLength') AS int)", columnDelimiter, alias, quotedColumnName);
							selectedColumnCount += 5;
							break;
					}
				}
			}

			string tableNameWithSchema = tableInfo.QualifiedName;
			string tableNameWithSourceDatabase = MakeSqlBracket(SourceDatabaseName) + '.' + tableNameWithSchema;
			string selectClause = "SELECT\r\n\t" + selectColumnListBuilder.ToString();
			string joinCondition = GetJoinCondition(tableInfo);
			string fromClause = "FROM (SELECT CONVERT(bit, 1) AS [#__InTarget], * FROM " + tableNameWithSchema + ") AS t\r\n\t"
				+ "FULL OUTER JOIN (SELECT CONVERT(bit, 1) AS [#__InSource], * FROM " + tableNameWithSourceDatabase + ") AS s ON " + joinCondition;
			return selectClause + "\r\n" + fromClause + ";";
		}

		private void GenerateStatements()
		{
			// Loop through the tables with data and generate
			// statements for the changed data.
			TableCollection sourceTables = this.sourceSmoDatabase.Tables;
			foreach(var tableKey in GetTablesWithData())
			{
				Table table = sourceTables[tableKey.Name, tableKey.Schema];
				TableInfo tableInfo = CreateTableInfo(table);
				// GenerateStatements will populate the collections of delete, insert, and update statements.
				// (the DeleteStatements, InsertStatements, and UpdateStatements properties of GeneratorTableInfo).
				GenerateStatements(tableInfo);
				if(tableInfo.HasChanges)
				{
					changedTables[tableKey] = tableInfo;
				}
			}
		}

		private bool GenerateStatements(TableInfo tableInfo)
		{
			// TODO: how do we want to handle a table with no key columns?
			if(!tableInfo.KeyColumns.Any())
				return false;

			string selectCommand = GenerateSelectQuery(tableInfo);
			using(SqlDataReader reader = ExecuteTargetReader(selectCommand))
			{
				object[] values = new object[reader.FieldCount];
				while(reader.Read())
				{
					bool inSource = reader.GetBoolean(0);
					bool inTarget = reader.GetBoolean(1);
					reader.GetSqlValues(values);
					if(!inTarget)
					{
						ScriptInsert(tableInfo, values);
					}
					else if(!inSource)
					{
						ScriptDelete(tableInfo, values);
					}
					else
					{
						ScriptUpdateIfModified(tableInfo, values);
					}
				}
			}

			return tableInfo.HasChanges;
		}

		private string GenerateUniqueMismatchQuery(Index index, TableInfo tableInfo)
		{
			string tableNameWithSchema = tableInfo.QualifiedName;
			string tableNameWithSourceDatabase = MakeSqlBracket(SourceDatabaseName) + '.' + tableNameWithSchema;

			// If the index is filtered then limit the source and target tables based on the filter.
			// This is a little bit of a hack, but use a subquery in place of the table name.
			if(index.HasFilter)
			{
				tableNameWithSchema = "(SELECT * FROM " + tableNameWithSchema + " WHERE " + index.FilterDefinition + ")";
				tableNameWithSourceDatabase = "(SELECT * FROM " + tableNameWithSourceDatabase + " WHERE " + index.FilterDefinition + ")";
			}

			string selectClause = "SELECT TOP(1) CONVERT(bit, 1) AS HasMismatch";
			string fromClause = "FROM " + tableNameWithSchema + " AS t\r\n\t"
				+ "INNER JOIN " + tableNameWithSourceDatabase + " AS s ON " + GetJoinCondition(index);
			string whereClause = "WHERE NOT(" + GetJoinCondition(tableInfo) + ")";
			return selectClause + "\r\n"
				+ fromClause + "\r\n"
				+ whereClause + ";";
		}

		private SqlConnection GetConnection(string serverName, string databaseName)
		{
			var builder = new SqlConnectionStringBuilder
			{
				DataSource = serverName,
				InitialCatalog = databaseName,
				IntegratedSecurity = true
			};
			return new SqlConnection(builder.ConnectionString);
		}

		private string GetJoinCondition(IEnumerable<Column> columns)
		{
			string nullableFormat = "(s.{0} = t.{0} OR (s.{0} IS NULL AND t.{0} IS NULL))";
			string notNullableFormat = "s.{0} = t.{0}";
			return String.Join
			(
				" AND ",
				from c in columns
				select String.Format
				(
					c.Nullable ? nullableFormat : notNullableFormat,
					MakeSqlBracket(c.Name)
				)
			);
		}

		private string GetJoinCondition(Index index)
		{
			Table table = (Table)index.Parent;
			ColumnCollection tableColumns = table.Columns;
			var joinColumns =
				from ic in index.IndexedColumns.Cast<IndexedColumn>()
				where !ic.IsIncluded
				select tableColumns[ic.Name];
			return GetJoinCondition(joinColumns);
		}

		/// <summary>
		/// Gets the condition to use to join the source and target tables.
		/// </summary>
		private string GetJoinCondition(TableInfo tableInfo)
		{
			var joinColumns = tableInfo.KeyColumns.Select(c => c.Column);
			return GetJoinCondition(joinColumns);
		}

		private Index GetJoinIndex(Table table)
		{
			Index bestIndex = null;
			int bestRank = int.MaxValue;
			// Find the best index to use for the join on clause.
			// In order of priority we want to use:
			// 1) the primary key,
			// 2) the clustered index, only if unique,
			// 3) a unique key,
			// or 4) a unique index
			// There could be multiple of unique keys/indexes so we go with
			// the one that comes first alphabetically. 
			foreach(Index index in table.Indexes)
			{
				// Only consider unique indexes.
				if(!index.IsUnique)
					continue;

				int currentRank = int.MaxValue;
				if(index.IndexKeyType == IndexKeyType.DriPrimaryKey)
					currentRank = 1;
				else if(index.IsClustered)
					currentRank = 2;
				else if(index.IndexKeyType == IndexKeyType.DriUniqueKey)
					currentRank = 3;
				else
					currentRank = 4;
				if
				(
					currentRank < bestRank ||
					(
						currentRank == bestRank
						&& String.Compare(index.Name, bestIndex.Name, StringComparison.OrdinalIgnoreCase) < 0
					)
				)
				{
					bestRank = currentRank;
					bestIndex = index;
				}
			}
			return bestIndex;
		}

		private IList<Column> GetKeyColumns(Table table)
		{
			List<Column> keyColumns = new List<Column>();
			Index joinIndex = GetJoinIndex(table);
			ColumnCollection tableColumns = table.Columns;
			if(joinIndex != null)
			{
				foreach(IndexedColumn indexColumn in joinIndex.IndexedColumns)
				{
					if(!indexColumn.IsIncluded)
						keyColumns.Add(tableColumns[indexColumn.Name]);
				}
			}
			else
			{
				// If there are no unique indexes (including primary keys and unique keys)
				// then check for an identity column.
				Column identityColumn = table.Columns.Cast<Column>().SingleOrDefault(c => c.Identity);
				if(identityColumn != null)
					keyColumns.Add(identityColumn);
			}
			return keyColumns;
		}

		private string GetSqlLiteral(object[] values, int ordinal, SqlDataType sqlDataType)
		{
			Object sqlValue = values[ordinal];
			if(sqlDataType == SqlDataType.Variant)
			{
				if(sqlValue == null || sqlValue == DBNull.Value || (sqlValue is INullable && ((INullable)sqlValue).IsNull))
					return "NULL";
				SqlString baseType = (SqlString)values[ordinal + 1];
				SqlInt32 precision = (SqlInt32)values[ordinal + 2];
				SqlInt32 scale = (SqlInt32)values[ordinal + 3];
				SqlString collation = (SqlString)values[ordinal + 4];
				SqlInt32 maxLength = (SqlInt32)values[ordinal + 5];
				return utility.GetSqlVariantLiteral(sqlValue, baseType, precision, scale, collation, maxLength);
			}
			else
			{
				return ScriptUtility.GetSqlLiteral(sqlValue, sqlDataType);
			}
		}

		private IEnumerable<TableIdentifier> GetTablesWithData()
		{
			var tablesWithData = new SortedSet<TableIdentifier>(TableIdentifierComparer.OrdinalIgnoreCase);
			using(var sourceConnection = GetConnection(SourceServerName, SourceDatabaseName))
			{
				AddTablesWithData(tablesWithData, sourceConnection);
			}
			using(var targetConnection = GetConnection(TargetServerName, TargetDatabaseName))
			{
				AddTablesWithData(tablesWithData, targetConnection);
			}
			return tablesWithData;
		}

		private void InsertRows()
		{
			foreach(var tableInfo in orderedTables)
			{
				var statements = tableInfo.InsertStatements;
				if(statements.Count > 0)
				{
					WriteBatch("PRINT 'Inserting {0} row(s) into {1}.';", statements.Count, EscapeStringLiteral(tableInfo.QualifiedName));
					if(tableInfo.HasIdentityColumn)
						WriteBatch("SET IDENTITY_INSERT {0} ON;", tableInfo.QualifiedName);
					WriteStatements(statements);
					if(tableInfo.HasIdentityColumn)
						WriteBatch("SET IDENTITY_INSERT {0} OFF;", tableInfo.QualifiedName);
				}
			}
		}

		private bool QueryForUniqueMismatch(Index index, TableInfo tableInfo)
		{
			string query = GenerateUniqueMismatchQuery(index, tableInfo);
			using(SqlDataReader reader = ExecuteTargetReader(query))
			{
				while(reader.Read())
				{
					if(reader.GetBoolean(0))
						return true;
				}
			}
			return false;
		}

		private void RebuildIndexes()
		{
			foreach(Index index in indexesToDisable)
			{
				Table table = (Table)index.Parent;
				string qualifiedTableName = table.QualifiedName();
				string quotedIndexName = MakeSqlBracket(index.Name);
				WriteBatch
				(
					"PRINT 'Rebuilding index {0} on {1}.';",
					EscapeStringLiteral(quotedIndexName),
					EscapeStringLiteral(qualifiedTableName)
				);
				WriteBatch
				(
					"ALTER INDEX {0} ON {1} REBUILD;",
					quotedIndexName,
					qualifiedTableName
				);
			}
		}

		/// <summary>
		/// Checks whether the foreign key references the specified index.
		/// </summary>
		/// <param name="foreignKey"></param>
		/// <param name="index"></param>
		/// <returns></returns>
		private bool ReferencesIndex(ForeignKey foreignKey, Index index)
		{
			Table indexTable = (Table)index.Parent;
			return String.Equals(foreignKey.ReferencedKey, index.Name, StringComparison.OrdinalIgnoreCase)
				&& String.Equals(foreignKey.ReferencedTable, indexTable.Name, StringComparison.OrdinalIgnoreCase)
				&& String.Equals(foreignKey.ReferencedTableSchema, indexTable.Schema, StringComparison.OrdinalIgnoreCase);
		}

		/// <summary>
		/// Checks to if the foreign key references an index that will be disabled.
		/// </summary>
		private bool ReferencesIndexToDisable(ForeignKey foreignKey)
		{
			return indexesToDisable.Any(i => ReferencesIndex(foreignKey, i));
		}

		private void ResolveOrder()
		{
			ResolveRelationships();

			// We first use an alphabetic order before resolving dependency order.
			// That way tables without dependencies will be in alphabetic order
			// (rather than hash key order from the dictionary).
			orderedTables.AddRange
			(
				changedTables.OrderBy(kvp => kvp.Key, TableIdentifierComparer.OrdinalIgnoreCase)
					.Select(kvp => kvp.Value)
					.DependencyOrder()
					.ToList()
			);
		}

		/// <summary>
		/// Resolve the foreign key relationships between tables that have changes.
		/// </summary>
		private void ResolveRelationships()
		{
			// Now loop through the tables with changes and check for foreign keys
			// that reference tables that have changes.
			foreach(Table table in sourceSmoDatabase.Tables)
			{
				foreach(ForeignKey foreignKey in table.ForeignKeys)
				{
					// Skip the foreign key if it is not enabled.
					if(!foreignKey.IsEnabled)
						continue;

					// Try to get the information for the primary (referenced) table.
					// Note that the referenced table may not have any changes
					// so it may not be in the dictionary.
					TableIdentifier primaryTableIdentifier = new TableIdentifier(foreignKey.ReferencedTableSchema, foreignKey.ReferencedTable);
					TableInfo primaryTableInfo;
					if(changedTables.TryGetValue(primaryTableIdentifier, out primaryTableInfo))
					{
						// If the foreign key references an index to be disabled,
						// then we should disable the foreign key too. Since we know
						// the foreign key will be disabled, we don't need to add it to the
						// relationships used for resolving dependency order.
						if(ReferencesIndexToDisable(foreignKey))
						{
							foreignKeysToDisable.Add(foreignKey);
						}
						else
						{
							// Try to get the information for the foreign table.
							// Note that the foreign table may not have any changes
							// so it may not be in the dictionary.
							Table foreignTable = foreignKey.Parent;
							TableIdentifier foreignTableIdentifier = foreignTable.TableIdentifier();
							TableInfo foreignTableInfo;
							if(changedTables.TryGetValue(foreignTableIdentifier, out foreignTableInfo))
							{
								// Create information about the foreign key. and
								// add it to the collection of relationships of both related tables.
								ForeignKeyInfo foreignKeyInfo = new ForeignKeyInfo
								{
									ForeignKey = foreignKey,
									PrimaryTable = primaryTableInfo,
									ForeignTable = foreignTableInfo
								};

								foreignTableInfo.Relationships.Add(foreignKeyInfo);
								// If it is a self-referencing relationship
								// then referenceTableInfo is the same instance as tableInfo.
								// We don't want to add it to the same Relationships collection twice.
								if(primaryTableInfo != foreignTableInfo)
									primaryTableInfo.Relationships.Add(foreignKeyInfo);
							}
						}
					}
				}
			}
		}

		private void RunTableDiff(string args)
		{
			ProcessStartInfo startInfo = new ProcessStartInfo
			{
				FileName = "tablediff.exe",
				UseShellExecute = false,
				Arguments = args
			};
			Process process = Process.Start(startInfo);
			process.WaitForExit();
		}

		private void ScriptDelete(TableInfo tableInfo, object[] values)
		{
			StringBuilder deleteStatement = new StringBuilder();
			StringBuilder valuesClause = new StringBuilder();
			deleteStatement.AppendFormat("DELETE FROM {0} WHERE ", tableInfo.QualifiedName);
			// Append WHERE clause with the key columns.
			string delimiter = null;
			foreach(var columnInfo in tableInfo.KeyColumns)
			{
				string targetLiteral = GetSqlLiteral(values, columnInfo.TargetOrdinal, columnInfo.SqlDataType);
				if(delimiter == null)
					delimiter = "\r\n\tAND ";
				else
					deleteStatement.Append(delimiter);
				if(targetLiteral == "NULL")
					deleteStatement.AppendFormat("{0} IS NULL", columnInfo.QuotedName);
				else
					deleteStatement.AppendFormat("{0} = {1}", columnInfo.QuotedName, targetLiteral);
			}
			deleteStatement.Append(";");
			tableInfo.DeleteStatements.Add(deleteStatement.ToString());
		}

		private void ScriptInsert(TableInfo tableInfo, object[] values)
		{
			StringBuilder insertStatement = new StringBuilder();
			StringBuilder valuesClause = new StringBuilder();
			insertStatement.AppendFormat("INSERT INTO {0} (", tableInfo.QualifiedName);
			valuesClause.Append("VALUES(");
			string delimiter = null;
			foreach(var columnInfo in tableInfo.Columns)
			{
				if(delimiter == null)
					delimiter = ", ";
				else
				{
					insertStatement.Append(delimiter);
					valuesClause.Append(delimiter);
				}
				insertStatement.Append(columnInfo.QuotedName);
				string sourceLiteral = GetSqlLiteral(values, columnInfo.SourceOrdinal, columnInfo.SqlDataType);
				valuesClause.Append(sourceLiteral);
			}
			insertStatement.AppendLine(")");
			valuesClause.Append(")");
			insertStatement.Append(valuesClause);
			insertStatement.Append(";");
			tableInfo.InsertStatements.Add(insertStatement.ToString());
		}

		private void ScriptUpdateIfModified(TableInfo tableInfo, object[] values)
		{
			StringBuilder updateStatement = new StringBuilder();
			StringBuilder setColumns = new StringBuilder();
			string delimiter = null;
			foreach(var columnInfo in tableInfo.Columns)
			{
				string sourceLiteral = GetSqlLiteral(values, columnInfo.SourceOrdinal, columnInfo.SqlDataType);
				string targetLiteral = GetSqlLiteral(values, columnInfo.TargetOrdinal, columnInfo.SqlDataType);
				if(sourceLiteral != targetLiteral)
				{
					// Increment the count of rows where this column has been updated.
					columnInfo.UpdatedRowCount++;

					if(delimiter == null)
						delimiter = ",\r\n\t";
					else
						setColumns.Append(delimiter);
					setColumns.AppendFormat("{0} = {1}", columnInfo.QuotedName, sourceLiteral);
				}
			}
			// Only generate an UPDATE statement if there are columns that have been modified and need to be set.
			if(setColumns.Length > 0)
			{
				// Append an UPDATE statement
				updateStatement.AppendFormat("UPDATE {0}\r\nSET ", tableInfo.QualifiedName);
				updateStatement.Append(setColumns);

				// Append a WHERE clause with the key columns.
				updateStatement.Append("\r\nWHERE ");
				delimiter = null;
				foreach(var columnInfo in tableInfo.KeyColumns)
				{
					string targetLiteral = GetSqlLiteral(values, columnInfo.TargetOrdinal, columnInfo.SqlDataType);
					if(delimiter == null)
						delimiter = "\r\n\tAND ";
					else
						updateStatement.Append(delimiter);
					if(targetLiteral == "NULL")
						updateStatement.AppendFormat("{0} IS NULL", columnInfo.QuotedName);
					else
						updateStatement.AppendFormat("{0} = {1}", columnInfo.QuotedName, targetLiteral);
				}
				updateStatement.Append(";");
				tableInfo.UpdateStatements.Add(updateStatement.ToString());
			}
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
		private void SetOptions()
		{
			WriteBatch
			(
				"SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON;\r\n"
				+ "SET NUMERIC_ROUNDABORT OFF;"
			);
		}

		/// <summary>
		/// Check whether we should disable the index before performing data updates.
		/// </summary>
		private bool ShouldDisable(Index index, TableInfo tableInfo)
		{
			// If the index is the primary key, is disabled, or is not unique then it won't have any unique mismatches.
			if(index.IndexKeyType == IndexKeyType.DriPrimaryKey || index.IsDisabled || !index.IsUnique)
				return false;

			// If the index does not have any updated columns then it won't have any unique mismatches.
			if(!HasAnyColumnsUpdatedMultipleTimes(index, tableInfo))
				return false;

			// Run a query to check for unique mismatches.
			return QueryForUniqueMismatch(index, tableInfo);
		}

		private void UpdateRows()
		{
			foreach(var tableInfo in orderedTables)
			{
				var statements = tableInfo.UpdateStatements;
				if(statements.Count > 0)
				{
					WriteBatch("PRINT 'Updating {0} row(s) in {1}.';", statements.Count, EscapeStringLiteral(tableInfo.QualifiedName));
					WriteStatements(statements);
				}
			}
		}

		private void VerifyProperties()
		{
			if(String.IsNullOrWhiteSpace(this.SourceServerName))
				throw new InvalidOperationException("Set the SourceServerName property before calling the GenerateScript() method.");
			if(String.IsNullOrWhiteSpace(this.SourceDatabaseName))
				throw new InvalidOperationException("Set the DatabaseName property before calling the GenerateScript() method.");
			if(String.IsNullOrWhiteSpace(this.TargetServerName))
				throw new InvalidOperationException("Set the TargetServerName property before calling the GenerateScript() method.");
			if(!String.Equals(this.SourceServerName, this.TargetServerName, StringComparison.OrdinalIgnoreCase))
				throw new InvalidOperationException("The source and target databases must be on the same server.");
			if(String.IsNullOrWhiteSpace(this.TargetDatabaseName))
				throw new InvalidOperationException("Set the TargetDatabaseName property before calling the GenerateScript() method.");
		}

		private void WriteBatch(string format, params object[] args)
		{
			writer.WriteLine(format, args);
			writer.WriteLine("GO");
		}

		private void WriteBatch(string batch)
		{
			writer.WriteLine(batch);
			writer.WriteLine("GO");
		}

		private void WriteBatches(IEnumerable<string> batches)
		{
			foreach(string batch in batches)
			{
				WriteBatch(batch);
			}
		}

		private void WriteStatements(IEnumerable<string> statements)
		{
			int counter = 0;
			foreach(string statement in statements)
			{
				writer.WriteLine(statement);
				// End the batch with a GO statement every 1000 statements.
				counter++;
				if(counter % 1000 == 0)
					writer.WriteLine("GO");
			}
			// Write a final GO statement.
			if(counter % 1000 != 0)
				writer.WriteLine("GO");
		}
	}
}
