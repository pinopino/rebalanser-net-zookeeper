using System;

namespace Rebalanser.Core
{
    public class TerminateClientException : Exception
    {
        public TerminateClientException(string message)
        : base(message)
        { }

        public TerminateClientException(string message, Exception ex)
            : base(message, ex)
        { }
    }
}
