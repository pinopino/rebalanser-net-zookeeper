namespace Rebalanser.Core
{
    public enum FollowerExitReason
    {
        PossibleRoleChange,
        Cancelled,
        SessionExpired,
        PotentialInconsistentState,
        FatalError
    }
}
