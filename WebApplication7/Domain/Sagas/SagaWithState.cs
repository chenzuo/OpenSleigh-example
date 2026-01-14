using OpenSleigh;
using OpenSleigh.Transport;
using WebApplication7.Domain.Messages;
using WebApplication7.Domain.States;

namespace WebApplication7.Domain.Sagas
{
    public class SagaWithState
        : Saga<MySagaState>,
            IStartedBy<StartSaga>,
            IHandleMessage<ProcessMySaga>,
            IHandleMessage<MySagaCompleted>
    {
        private readonly ILogger<SagaWithState> _logger;

        public SagaWithState(ILogger<SagaWithState> logger, ISagaInstance<MySagaState> context)
            : base(context)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async ValueTask HandleAsync(
            IMessageContext<StartSaga> context,
            CancellationToken cancellationToken = default
        )
        {
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "starting saga with state '{InstanceId}' for order {OrderId}...",
                    this.Context.InstanceId,
                    context.Message.OrderId
                );

            this.Context.State.OrderId = context.Message.OrderId;
            this.Context.State.Status = "OrderReceived";
            this.Context.State.LastUpdated = DateTimeOffset.UtcNow;
            this.Context.State.Foo = 30;
            this.Context.State.Bar = "lorem ipsum loading";

            Random random = new Random(1500);
            await Task.Delay(random.Next(1000, 3000), cancellationToken); // simulate some work

            var message = new ProcessMySaga();
            this.Publish(message);
        }

        public async ValueTask HandleAsync(
            IMessageContext<ProcessMySaga> context,
            CancellationToken cancellationToken = default
        )
        {
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "processing saga with state '{InstanceId}'...",
                    this.Context.InstanceId
                );

            this.Context.State.Status = "Processing";
            this.Context.State.LastUpdated = DateTimeOffset.UtcNow;

            this.Context.State.Foo = 70;
            this.Context.State.Bar = "lorem ipsum";

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "state updated for order {OrderId}: Foo = {Foo}, Bar = {Bar}",
                    this.Context.State.OrderId,
                    this.Context.State.Foo,
                    this.Context.State.Bar
                );

            Random random = new Random(1500);
            await Task.Delay(random.Next(1000, 3000), cancellationToken); // simulate some work
            var message = new MySagaCompleted();
            this.Publish(message);
        }

        public async ValueTask HandleAsync(
            IMessageContext<MySagaCompleted> context,
            CancellationToken cancellationToken = default
        )
        {
            Random random = new Random(1500);
            await Task.Delay(random.Next(1000, 3000), cancellationToken); // simulate some work
            this.Context.State.Status = "Completed";
            this.Context.State.LastUpdated = DateTimeOffset.UtcNow;
            this.Context.State.Foo = 100;
            this.Context.State.Bar = "done";
            this.Context.MarkAsCompleted();

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "order {OrderId} state: Foo = {Foo}, Bar = {Bar}",
                    this.Context.State.OrderId,
                    this.Context.State.Foo,
                    this.Context.State.Bar
                );

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "saga with state '{InstanceId}' completed!",
                    this.Context.InstanceId
                );
        }
    }
}
