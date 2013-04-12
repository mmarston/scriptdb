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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mercent.SqlServer.Management.IO
{
	public class DirectoryComparer
	{
		IList<FileCompareInfo> results;
		string sourceBaseDirectory;
		string targetBaseDirectory;

		private DirectoryComparer()
		{
		}
		
		public static IEnumerable<FileCompareInfo> Compare(string sourceDirectory, string targetDirectory)
		{
			DirectoryComparer comparer = new DirectoryComparer();
			return comparer.InternalCompare(sourceDirectory, targetDirectory);
		}

		private void AddFileResult(string relativeDirectory, InternalCompareInfo internalInfo)
		{
			FileCompareInfo result = ToFileCompareInfo(relativeDirectory, internalInfo);
			if(internalInfo.InSource && internalInfo.InTarget)
			{
				result.Status = FileComparer.Compare(result.SourcePath, result.TargetPath);
			}
			this.results.Add(result);
		}

		private void AddWithAction(IDictionary<string, InternalCompareInfo> dictionary, IEnumerable<FileSystemInfo> files, Action<InternalCompareInfo> action)
		{
			foreach(FileSystemInfo file in files)
			{
				InternalCompareInfo info = GetOrAdd(dictionary, file.Name);
				action(info);
			}
		}

		private void CompareSubdirectory(string relativeDirectory)
		{
			DirectoryInfo sourceDirectory = new DirectoryInfo(Path.Combine(sourceBaseDirectory, relativeDirectory));
			DirectoryInfo targetDirectory = new DirectoryInfo(Path.Combine(targetBaseDirectory, relativeDirectory));

			var childDictionary = new SortedDictionary<string, InternalCompareInfo>(StringComparer.OrdinalIgnoreCase);

			// Enumerate the child files in source and target.
			AddWithAction(childDictionary, sourceDirectory.EnumerateFiles(), i => i.InSource = true);
			AddWithAction(childDictionary, targetDirectory.EnumerateFiles(), i => i.InTarget = true);
			foreach(var childInfo in childDictionary.Values)
			{
				AddFileResult(relativeDirectory, childInfo);
			}

			// Clear the files from the dictionary and now enumerate the child directories.
			childDictionary.Clear();
			AddWithAction(childDictionary, sourceDirectory.EnumerateDirectories(), i => i.InSource = true);
			AddWithAction(childDictionary, targetDirectory.EnumerateDirectories(), i => i.InTarget = true);
			foreach(var childInfo in childDictionary.Values)
			{
				if(childInfo.InSource && childInfo.InTarget)
				{
					CompareSubdirectory(Path.Combine(relativeDirectory, childInfo.Name));
				}
				else
				{
					FileCompareInfo result = ToFileCompareInfo(relativeDirectory, childInfo);
					result.IsDirectory = true;
					results.Add(result);
				}
			}
		}

		private InternalCompareInfo GetOrAdd(IDictionary<string, InternalCompareInfo> dictionary, string name)
		{
			InternalCompareInfo info;
			if(!dictionary.TryGetValue(name, out info))
			{
				info = new InternalCompareInfo { Name = name };
				dictionary.Add(name, info);
			}
			return info;
		}

		private IEnumerable<FileCompareInfo> InternalCompare(string sourceDirectory, string targetDirectory)
		{
			if(sourceDirectory == null)
				throw new ArgumentNullException("sourceDirectory");
			if(targetDirectory == null)
				throw new ArgumentNullException("targetDirectory");

			this.sourceBaseDirectory = sourceDirectory;
			this.targetBaseDirectory = targetDirectory;

			results = new List<FileCompareInfo>();
			CompareSubdirectory("");
			return results;
		}

		private FileCompareInfo ToFileCompareInfo(string relativeDirectory, InternalCompareInfo internalInfo)
		{
			string relativePath = Path.Combine(relativeDirectory, internalInfo.Name);
			var result = new FileCompareInfo
			{
				Name = internalInfo.Name,
				RelativePath = relativePath
			};
			result.SourcePath = Path.Combine(sourceBaseDirectory, relativePath);
			result.TargetPath = Path.Combine(targetBaseDirectory, relativePath);
			if(!internalInfo.InSource)
				result.Status = FileCompareStatus.TargetOnly;
			else if(!internalInfo.InTarget)
				result.Status = FileCompareStatus.SourceOnly;
			return result;
		}

		private class InternalCompareInfo
		{
			public bool InSource { get; set; }
			public bool InTarget { get; set; }
			public string Name { get; set; }
		}
	}
}
