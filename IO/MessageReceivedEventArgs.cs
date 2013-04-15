using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mercent.SqlServer.Management.IO
{
	/// <summary>
	/// Event arguments to pass a message.
	/// </summary>
	/// <remarks>
	/// This class mirrors the functionality of <see cref="System.Diagnostics.DataReceivedEventArgs"/>.
	/// Sinceince that class does not have a public constructor it can't be used when we need to create our own events.
	/// </remarks>
	public class MessageReceivedEventArgs : EventArgs
	{
		public MessageReceivedEventArgs(string message)
		{
			this.Message = message;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <remarks>
		/// Just like the <see cref="System.Diagnostics.DataReceivedEventArgs">DataReceivedEventArgs.Data</see> property,
		/// this Message property may be null. Event handlers should be prepared to handle a null value.
		/// </remarks>
		public string Message { get; private set; }
	}
}
