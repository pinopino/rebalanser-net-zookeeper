namespace Rebalanser.Core
{
    public enum CoordinatorExitReason
    {
        NoLongerCoordinator,
        Cancelled,
        SessionExpired,
        PotentialInconsistentState,
        FatalError
    }
}
