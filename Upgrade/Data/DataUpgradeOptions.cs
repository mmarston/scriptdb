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

namespace Mercent.SqlServer.Management.Upgrade.Data
{
	public class DataUpgradeOptions
	{
		public DataUpgradeOptions()
		{
			DisableTriggers = true;
		}

		public bool DisableTriggers { get; set; }

		/// <summary>
		/// Ingore tables that are empty in the source database.
		/// </summary>
		/// <remarks>
		/// Use this option when you want to keep target database rows
		/// in tables that are empty in the source database.
		/// </remarks>
		public bool IgnoreEmptySourceTables { get; set; }

		public bool SyncMode { get; set; }
	}
}
