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
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;

namespace Mercent.SqlServer.Management
{
	internal static class ScriptExtensions
	{
		public static bool HasAnyVariantColumns(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return table.Columns
				.Cast<Column>()
				.Any(c => c.DataType.SqlDataType == SqlDataType.Variant);
		}

		/// <summary>
		/// Returns true if the table has a column of a unicode data type (nchar, ntext, nvarchar).
		/// </summary>
		public static bool HasAnyUnicodeColumns(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return table.Columns
				.Cast<Column>()
				.Any(c => IsUnicode(c.DataType.SqlDataType));
		}

		public static bool HasAnyXmlColumns(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return table.Columns
				.Cast<Column>()
				.Any(c => c.DataType.SqlDataType == SqlDataType.Xml);
		}

		public static bool HasUniqueIndex(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return table.Indexes
				.Cast<Index>()
				.Any(i => i.IsUnique);
		}

		public static bool IsSysDiagramsWithData(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return table.Schema == "dbo"
				&& table.Name == "sysdiagrams"
				&& table.RowCount > 0;
		}

		public static string QualifiedName(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return ScriptUtility.MakeSqlBracket(table.Schema) + '.' + ScriptUtility.MakeSqlBracket(table.Name);
		}

		public static TableIdentifier TableIdentifier(this Table table)
		{
			if(table == null)
				throw new ArgumentNullException("table");

			return new TableIdentifier(table.Schema, table.Name);
		}

		/// <summary>
		/// Returns true if the data type is one of the following unicode types: NChar, NText, NVarChar or NVarCharMax.
		/// </summary>
		static bool IsUnicode(SqlDataType dataType)
		{
			switch(dataType)
			{
				case SqlDataType.NChar:
				case SqlDataType.NText:
				case SqlDataType.NVarChar:
				case SqlDataType.NVarCharMax:
					return true;
				default:
					return false;
			}
		}
	}
}
