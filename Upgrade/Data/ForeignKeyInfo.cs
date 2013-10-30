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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace Mercent.SqlServer.Management.Upgrade.Data
{
	internal class ForeignKeyInfo
	{
		public ForeignKey ForeignKey { get; set; }
		public TableInfo ForeignTable { get; set; }
		public TableInfo PrimaryTable { get; set; }
		
		public bool ShouldDisable
		{
			get
			{
				// If the dependency order was incorrectly resolved
				// (the primary table comes after the foreign table),
				// then the foreign key needs to be disabled.
				if(PrimaryTable.DependencyIndex >= ForeignTable.DependencyIndex)
					return true;

				// If any foreign key columns were updated and either any referenced columns were updated
				// or the primary table has deleted rows then disable the foreign key.
				return AnyForeignKeyColumnsUpdated &&
				(
					AnyReferencedColumnsUpdated || PrimaryTable.DeleteStatements.Count > 0
				);
			}
		}

		private bool AnyForeignKeyColumnsUpdated
		{
			get
			{
				// If no rows were updated in the foreign table then return false.
				if(ForeignTable.UpdateStatements.Count == 0)
					return false;

				// Loop through the foreign key columns and return true
				// if any of them have an updated row count greater than zero.
				foreach(ForeignKeyColumn column in ForeignKey.Columns)
				{
					ColumnInfo columnInfo = ForeignTable.Columns.Get(column.Name);
					if(columnInfo.UpdatedRowCount > 0)
						return true;
				}

				// If we reached here then no foreign key columns were updated.
				return false;
			}
		}

		private bool AnyReferencedColumnsUpdated
		{
			get
			{
				// If no rows were updated in the primary (referenced) table then return false.
				if(PrimaryTable.UpdateStatements.Count == 0)
					return false;

				// Loop through the foreign key columns and return true
				// if any of the referenced columns have an updated row count greater than zero.
				foreach(ForeignKeyColumn column in ForeignKey.Columns)
				{
					ColumnInfo columnInfo = PrimaryTable.Columns.Get(column.ReferencedColumn);
					if(columnInfo.UpdatedRowCount > 0)
						return true;
				}

				// If we reached here then no referenced columns were updated.
				return false;
			}
		}
	}
}
