using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mercent.SqlServer.Management
{
	/// <summary>
	/// Exception thrown when the user indicates that processing should be aborted
	/// instead of continueing after an error.
	/// </summary>
	public class AbortException : Exception
	{
		public AbortException()
		{
		}

		public AbortException(string message)
			: base(message)
		{
		}

		public AbortException(string message, Exception inner)
			: base(message, inner)
		{
		}
	}
}
