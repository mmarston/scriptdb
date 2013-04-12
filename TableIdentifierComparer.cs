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

namespace Mercent.SqlServer.Management
{
	internal class TableIdentifierComparer : IComparer<TableIdentifier>, IEqualityComparer<TableIdentifier>
	{
		public static TableIdentifierComparer Ordinal = new TableIdentifierComparer(StringComparer.Ordinal);
		public static TableIdentifierComparer OrdinalIgnoreCase = new TableIdentifierComparer(StringComparer.OrdinalIgnoreCase);
		
		StringComparer stringComparer;

		public TableIdentifierComparer(StringComparer stringComparer)
		{
			if(stringComparer == null)
				throw new ArgumentNullException("stringComparer");
			this.stringComparer = stringComparer;
		}


		public int Compare(TableIdentifier table1, TableIdentifier table2)
		{
			if(ReferenceEquals(table1, table2))
				return 0;
			else if(table1 == null)
				return -1;
			else if(table2 == null)
				return 1;
			else
				return stringComparer.Compare(table1.QualifiedName, table2.QualifiedName);
		}

		public bool Equals(TableIdentifier table1, TableIdentifier table2)
		{
			if(ReferenceEquals(table1, table2))
				return true;
			else if(table1 == null || table2 == null)
				return false;
			else
				return stringComparer.Equals(table1.QualifiedName, table2.QualifiedName);
		}

		public int GetHashCode(TableIdentifier obj)
		{
			if(obj == null)
				throw new ArgumentNullException("obj");
			return stringComparer.GetHashCode(obj.QualifiedName);
		}
	}
}
