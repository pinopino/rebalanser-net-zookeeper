namespace Rebalanser.ZooKeeper
{
    public class StatusZnode
    {
        public RebalancingStatus RebalancingStatus { get; set; }
        public int Version { get; set; }
    }
}
