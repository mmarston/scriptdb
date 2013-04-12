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
	/// <summary>
	/// Class containing an extension method to sort items in dependency order.
	/// </summary>
	/// <seealso cref="IDependencyNode"/>
	internal static class DependencySorter
	{
		///// <summary>
		///// Returns items in dependency order.
		///// </summary>
		///// <remarks>
		///// This method sets the <see cref="IDependencyNode.DependencyIndex"/>.
		///// </remarks>
		//public static IEnumerable<T> DependencyOrder<T>(this IEnumerable<T> items)
		//	where T : IDependencyNode
		//{
		//	if(items == null)
		//		throw new ArgumentNullException("item");

		//	// Convert the items enumerable to a list so that
		//	// we can enumerate the collection twice without getting
		//	// new items each time and without having to re-evaluate
		//	// whatever query may be returning the IEnumerable<T> results.
		//	IList<T> itemList = items as IList<T>;
		//	if(itemList == null)
		//		itemList = items.ToList();

		//	// On the first pass, just make sure the DependencyIndex is null.
		//	foreach(T item in itemList)
		//		item.DependencyIndex = null;
			
		//	// Now loop through the list and insert them in dependency order.
		//	// This is effectively an insertion sort algorithm.
		//	List<T> orderedList = new List<T>();
		//	foreach(T item in itemList)
		//	{
		//		// The current item should come before all of its successors.
		//		// Find the successor with the minimum dependency index.
		//		int? dependencyIndex = item.Successors().Min(d => d.DependencyIndex);
				
		//		// If there are no successors (or none have been added to the ordered list yet)
		//		// then add the item to the end of the list.
		//		if(dependencyIndex == null)
		//		{
		//			item.DependencyIndex = orderedList.Count;
		//			orderedList.Add(item);
		//		}
		//		else
		//		{
		//			// Otherwise, insert the item before the "earliest" successor
		//			// (the one with the minimum dependency index).
		//			orderedList.Insert(dependencyIndex.Value, item);
					
		//			// Adjust the dependency index of the items that were shifted
		//			// due to the insertion of the current item.
		//			for(int i = dependencyIndex.Value; i < orderedList.Count; i++)
		//			{
		//				orderedList[i].DependencyIndex = i;
		//			}
		//		}
		//	}
		//	return orderedList;
		//}

		/// <summary>
		/// Returns items in dependency order.
		/// </summary>
		/// <remarks>
		/// This method sets the <see cref="IDependencyNode.DependencyIndex"/>.
		/// </remarks>
		public static IEnumerable<T> RecursiveDependencyOrder<T>(this IEnumerable<T> items)
			where T : IDependencyNode
		{
			if(items == null)
				throw new ArgumentNullException("item");

			// Convert the items enumerable to a list so that
			// we can enumerate the collection twice without getting
			// new items each time and without having to re-evaluate
			// whatever query may be returning the IEnumerable<T> results.
			IList<T> itemList = items as IList<T>;
			if(itemList == null)
				itemList = items.ToList();

			// On the first pass, just make sure the DependencyIndex is null.
			foreach(T item in itemList)
				item.DependencyIndex = null;

			// Now loop through the list and insert them in dependency order.
			List<T> orderedList = new List<T>();
			foreach(T item in itemList)
			{
				Visit(orderedList, item);
			}
			return orderedList;
		}

		private static void Visit<T>(List<T> orderedList, T item)
			where T : IDependencyNode
		{
			// If we've already determined the index, just return.
			if(item.DependencyIndex.HasValue)
				return;

			// Temporarily set the index.
			// This avoids an infinite loop if there is a cycle in the dependencies.
			item.DependencyIndex = -1;

			// Visit each of the predecessors.
			foreach(T predecessor in item.Predecessors())
			{
				Visit(orderedList, predecessor);
			}

			// Now add the item to the list since all predecessors have already been added to the list.
			item.DependencyIndex = orderedList.Count;
			orderedList.Add(item);
		}

		/// <summary>
		/// Returns items in dependency order.
		/// </summary>
		/// <remarks>
		/// This method sets the <see cref="IDependencyNode.DependencyIndex"/>.
		/// </remarks>
		public static IEnumerable<T> DependencyOrder<T>(this IEnumerable<T> items)
			where T : IDependencyNode
		{
			if(items == null)
				throw new ArgumentNullException("item");

			// Convert the items enumerable to a list so that
			// we can enumerate the collection twice without getting
			// new items each time and without having to re-evaluate
			// whatever query may be returning the IEnumerable<T> results.
			IList<T> itemList = items as IList<T>;
			if(itemList == null)
				itemList = items.ToList();

			// We use a stack to store items while we do
			// a depth-first traversal (instead of using recursion).
			Stack<T> stack = new Stack<T>();

			// Add the items to the stack.
			// Also, make sure the DependencyIndex is null.
			// We want to preserve the order of items without any dependencies.
			// Since a stack reverses items, we add them in reverse order.
			for(int i = itemList.Count - 1; i >= 0; i--)
			{
				T item = itemList[i];
				item.DependencyIndex = null;
				stack.Push(item);
			}

			// The ordered list will store the results in the correct order.
			List<T> orderedList = new List<T>();
			
			// Keep looping as long as there are items left in the stack.
			while(stack.Count > 0)
			{
				// Peek at an item in the stack.
				T item = stack.Peek();

				// If the item has a value for DependencyIndex, then pop it off the stack.
				if(item.DependencyIndex.HasValue)
				{
					item = stack.Pop();

					// At this point, all the item's predecessors have been added to the ordered list.
					// If this item has not been added to the ordered list yet
					// (DependencyIndex = -1) then add it now.
					if(item.DependencyIndex == -1)
					{
						item.DependencyIndex = orderedList.Count;
						orderedList.Add(item);
					}
				}
				else
				{
					// If the items DependencyIndex has not been set,
					// then the items predecessors have not been added to the stack.
					// Temporarily set the index to -1.
					// This avoids an infinite loop if there is a cycle in the dependencies.
					item.DependencyIndex = -1;
					
					// Now add the predecessors to the stack.
					foreach(T predecessor in item.Predecessors())
						stack.Push(predecessor);
				}
			}
			return orderedList;
		}
	}
}
