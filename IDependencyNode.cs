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

namespace Mercent.SqlServer.Management
{
	/// <summary>
	/// Interface for objects to be sorted in dependency order.
	/// </summary>
	/// <seealso cref="DependencySorter"/>
	internal interface IDependencyNode
	{
		/// <summary>
		/// Gets the index of the node in a list ordered by dependencies.
		/// </summary>
		/// <remarks>
		/// This property is set by the <see cref="DependencySorter.DependencyOrder"/>
		/// extension method.
		/// </remarks>
		int? DependencyIndex { get; set; }
		
		///// <summary>
		///// Gets the dependency nodes that should come after the current node.
		///// </summary>
		//IEnumerable<IDependencyNode> Successors();

		/// <summary>
		/// Gets the dependency nodes that should come before the current node.
		/// </summary>
		IEnumerable<IDependencyNode> Predecessors();
	}
}
