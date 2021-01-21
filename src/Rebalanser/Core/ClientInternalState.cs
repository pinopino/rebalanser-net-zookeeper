namespace Rebalanser.Core
{
    public enum ClientInternalState
    {
        NoSession,
        NoClientNode,
        NoRole,
        Error,
        IsLeader,
        IsFollower,
        Terminated
    }
}
