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
	internal class TableIdentifier : IEquatable<TableIdentifier>, IComparable<TableIdentifier>
	{
		public TableIdentifier(string schemaName, string tableName)
		{
			if(String.IsNullOrEmpty(schemaName))
				throw new ArgumentNullException("schemaName");
			if(String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException("tableName");
			this.Schema = schemaName;
			this.Name = tableName;
			this.QualifiedName = ScriptUtility.MakeSqlBracket(schemaName) + '.' + ScriptUtility.MakeSqlBracket(tableName);
		}

		public string Name { get; private set; }
		public string QualifiedName { get; private set; }
		public string Schema { get; private set; }
		
		public int CompareTo(TableIdentifier other)
		{
			if(other == null)
				return 1;
			else
				return String.Compare(this.QualifiedName, other.QualifiedName, StringComparison.Ordinal);
		}

		public override bool Equals(object obj)
		{
			var other = obj as TableIdentifier;
			if(other == null)
				return false;
			else
				return this.Equals(other);
		}

		public bool Equals(TableIdentifier other)
		{
			if(other == null)
				return false;
			else
				return this.QualifiedName == other.QualifiedName;
		}

		public override int GetHashCode()
		{
			return this.QualifiedName.GetHashCode();
		}

		public override string ToString()
		{
			return QualifiedName;
		}
	}
}
