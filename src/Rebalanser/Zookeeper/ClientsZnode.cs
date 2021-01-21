using System.Collections.Generic;

namespace Rebalanser.ZooKeeper
{
    public class ClientsZnode
    {
        public int Version { get; set; }
        public List<string> ClientPaths { get; set; }
    }
}
