using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Rebalanser.ZooKeeper
{
    [Serializable, DataContract(Namespace = "Rebalanser", Name = "ResourcesZnodeData")]
    public class ResourcesZnodeData
    {
        public ResourcesZnodeData()
        {
            Assignments = new List<ResourceAssignment>();
        }

        [DataMember(Name = "Assignments")]
        public List<ResourceAssignment> Assignments { get; set; }
    }
}
