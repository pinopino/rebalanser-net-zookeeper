﻿using System;

namespace Rebalanser
{
    public class RebalanserException : Exception
    {
        public RebalanserException(string message)
            : base(message)
        { }

        public RebalanserException(string message, Exception ex)
            : base(message, ex)
        { }
    }
}
