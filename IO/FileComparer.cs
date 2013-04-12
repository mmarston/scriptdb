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
	public class FileComparer
	{
		public static bool Equals(string sourcePath, string targetPath)
		{
			return Compare(sourcePath, targetPath) == FileCompareStatus.Identical;
		}

		public static FileCompareStatus Compare(string sourcePath, string targetPath)
		{
			if(sourcePath == null)
				throw new ArgumentNullException("sourcePath");
			if(targetPath == null)
				throw new ArgumentNullException("targetPath");
			
			FileInfo sourceFile = new FileInfo(sourcePath);
			FileInfo targetFile = new FileInfo(targetPath);
			
			// If neither file exists, return Unknown
			if(!sourceFile.Exists && !targetFile.Exists)
				return FileCompareStatus.Unknown;
			else if(!sourceFile.Exists)
				return FileCompareStatus.TargetOnly;
			else if(!targetFile.Exists)
				return FileCompareStatus.SourceOnly;

			// If the file paths are the same, then the files are identical.
			if(String.Equals(sourceFile.FullName, targetFile.FullName, StringComparison.OrdinalIgnoreCase))
				return FileCompareStatus.Identical;

			// If the file length doesn't match, then the files are different.
			if(sourceFile.Length != targetFile.Length)
				return FileCompareStatus.Modified;

			// Otherwise, do a byte-for-byte comparison.
			// This code could likely be optimized by
			//  1. using async IO, and/or
			//  2. reading into a byte array and doing an array comparison.
			// For now I'm keeping it simple.
			using(Stream stream1 = sourceFile.OpenRead())
			using(Stream stream2 = targetFile.OpenRead())
			{
				int value1, value2;
				// Keep reading from the streams...
				while(true)
				{
					value1 = stream1.ReadByte();
					value2 = stream2.ReadByte();

					// Until we find a value that is different (return Modified)
					// or reach the end of the streams (return Identical).
					if(value1 != value2)
						return FileCompareStatus.Modified;
					else if(value1 == -1)
						return FileCompareStatus.Identical;
				}
			}
		}
	}
}
