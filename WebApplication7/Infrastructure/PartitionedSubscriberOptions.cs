namespace WebApplication7.Infrastructure
{
    public sealed class PartitionedSubscriberOptions
    {
        public int Partitions { get; set; } = 8;
        public int MaxLockRetries { get; set; } = 6;
        public int RequeueDelayMs { get; set; } = 500;
        public int RequeueJitterMs { get; set; } = 200;
        public int RequeueMaxConcurrency { get; set; } = 4;
    }
}
