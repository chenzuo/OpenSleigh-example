namespace WebApplication7.Domain.States
{
    public record RefundSagaState
    {
        public string CorrelationId { get; set; } = string.Empty;
        public Guid OrderId { get; set; }
        public decimal Amount { get; set; }
        public string? Reason { get; set; }
        public string Status { get; set; } = "Created";
        public string? LastFailedStep { get; set; }
        public DateTimeOffset LastUpdated { get; set; } = DateTimeOffset.UtcNow;
    }
}
