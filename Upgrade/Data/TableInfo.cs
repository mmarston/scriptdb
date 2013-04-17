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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace Mercent.SqlServer.Management.Upgrade.Data
{
	internal class TableInfo : IDependencyNode
	{
		public TableInfo()
		{
			this.Columns = new Collection<ColumnInfo>();
			this.DeleteStatements = new Collection<string>();
			this.InsertStatements = new Collection<string>();
			this.KeyColumns = new Collection<ColumnInfo>();
			this.Relationships = new Collection<ForeignKeyInfo>();
			this.UpdateStatements = new Collection<string>();
		}

		/// <summary>
		/// Information about the columns used to compare the data in the table.
		/// </summary>
		public Collection<ColumnInfo> Columns { get; private set; }
		public Collection<string> DeleteStatements { get; private set; }
		
		public bool HasChanges
		{
			get
			{
				int changeCount = DeleteStatements.Count + InsertStatements.Count + UpdateStatements.Count;
				return changeCount > 0;
			}
		}

		public bool HasIdentityColumn { get; set; }

		/// <summary>
		/// The values for INSERT statements.
		/// </summary>
		/// <remarks>
		/// As an optimization, instead of using individual insert statements for every row,
		/// we insert rows in batches using multiple rows in the VALUES clause.
		/// Each string in the InsertStatements collection only includes the values
		/// for one row, surrounded by parenthesis. It does not include the full INSERT statement
		/// or event the VALUES keyword.
		/// </remarks>
		public Collection<string> InsertStatements { get; private set; }

		/// <summary>
		/// Information about the key columns used to compare the data in the table.
		/// </summary>
		/// <remarks>
		/// See the <see cref="GetKeyColumns"/> method for details on how the key columns are determined
		/// when there is no primary key.
		/// </remarks>
		public Collection<ColumnInfo> KeyColumns { get; private set; }

		/// <summary>
		/// The quoted, schema-qualified name.
		/// </summary>
		/// <remarks>
		/// For example, "[dbo].[Product]".
		/// </remarks>
		public string QualifiedName { get; set; }

		/// <summary>
		/// Set of foreign keys with related tables that have changes.
		/// </summary>
		/// <remarks>
		/// The collection includes the foreign keys on this table
		/// (where this table references another table) and foreign keys on
		/// other tables that refer to this table (where this table is the primary table).
		/// </remarks>
		public Collection<ForeignKeyInfo> Relationships { get; private set; }

		/// <summary>
		/// The table in the source database.
		/// </summary>
		public Table Table { get; set; }
		public Collection<string> UpdateStatements { get; private set; }
		
		#region IDependencyNode Members

		public int? DependencyIndex { get; set; }

		//IEnumerable<IDependencyNode> IDependencyNode.Successors()
		//{
		//	return
		//		from fk in Relationships
		//		where fk.PrimaryTable == this
		//		select fk.ForeignTable;
		//}

		IEnumerable<IDependencyNode> IDependencyNode.Predecessors()
		{
			return
				from fk in Relationships
				where fk.ForeignTable == this
				select fk.PrimaryTable;
		}

		#endregion
	}
}
