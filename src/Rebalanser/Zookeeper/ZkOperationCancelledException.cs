using System;

namespace Rebalanser.ZooKeeper
{
    public class ZkOperationCancelledException : Exception
    {
        public ZkOperationCancelledException(string message)
            : base(message)
        { }
    }
}
