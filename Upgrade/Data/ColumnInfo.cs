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
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace Mercent.SqlServer.Management.Upgrade.Data
{
	internal class ColumnInfo
	{
		/// <summary>
		/// The column in the source table.
		/// </summary>
		public Column Column { get; set; }
		public bool InKey { get; set; }
		public string QuotedName { get; set; }
		
		/// <summary>
		/// The column ordinal of the source value in the data reader used to compare the data.
		/// </summary>
		/// <remarks>
		/// This does not correspond to the column's position in the source table.
		/// This property is set by the <see cref="GenerateSelectQuery"/> method.
		/// </remarks>
		public int SourceOrdinal { get; set; }
		public SqlDataType SqlDataType { get; set; }

		/// <summary>
		/// The column ordinal of the target value in the data reader used to compare the data.
		/// </summary>
		/// <remarks>
		/// This does not correspond to the column's position in the target table.
		/// This property is set by the <see cref="GenerateSelectQuery"/> method.
		/// </remarks>
		public int TargetOrdinal { get; set; }
		public int UpdatedRowCount { get; set; }
	}

	internal static class ColumnInfoExtensions
	{
		public static ColumnInfo Get(this IEnumerable<ColumnInfo> columns, string name)
		{
			return columns.FirstOrDefault(c => String.Equals(c.Column.Name, name, StringComparison.OrdinalIgnoreCase));
		}
	}
}
