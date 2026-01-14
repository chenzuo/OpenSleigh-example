namespace WebApplication7.Domain.States
{
    public record MySagaState
    {
        public Guid OrderId { get; set; }
        public string Status { get; set; } = "Pending";
        public int Foo { get; set; } = 42;
        public string Bar { get; set; } = "71";
        public DateTimeOffset LastUpdated { get; set; } = DateTimeOffset.UtcNow;
    }
}
