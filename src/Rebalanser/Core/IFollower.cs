using System.Threading.Tasks;

namespace Rebalanser.Core
{
    public interface IFollower
    {
        Task<BecomeFollowerResult> BecomeFollowerAsync();
        Task<FollowerExitReason> StartEventLoopAsync();
    }
}
