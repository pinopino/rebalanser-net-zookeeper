using System.Threading.Tasks;

namespace Rebalanser.Core
{
    public interface ICoordinator
    {
        Task<BecomeCoordinatorResult> BecomeCoordinatorAsync(int currentEpoch);
        Task<CoordinatorExitReason> StartEventLoopAsync();
    }
}
