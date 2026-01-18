using OpenSleigh;
using OpenSleigh.Outbox;
using OpenSleigh.Transport;

namespace WebApplication7.Infrastructure
{
    public enum ProcessOutcome
    {
        Processed,
        LockConflict,
        Failed,
    }

    public interface IResultMessageProcessor
    {
        ValueTask<ProcessOutcome> ProcessAsync(
            MessageEnvelope outboxMessage,
            CancellationToken cancellationToken = default
        );
    }

    public sealed class ResultMessageProcessor : IResultMessageProcessor
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ResultMessageProcessor> _logger;

        public ResultMessageProcessor(
            IServiceScopeFactory scopeFactory,
            ILogger<ResultMessageProcessor> logger
        )
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async ValueTask<ProcessOutcome> ProcessAsync(
            MessageEnvelope outboxMessage,
            CancellationToken cancellationToken = default
        )
        {
            ArgumentNullException.ThrowIfNull(outboxMessage);

            var message =
                outboxMessage.Message
                ?? throw new InvalidOperationException("message payload was not found.");

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var provider = scope.ServiceProvider;
                var sagaRunner = provider.GetRequiredService<ISagaRunner>();
                var descriptors = provider
                    .GetRequiredService<ISagaDescriptorsResolver>()
                    .Resolve(message);

                var context = ToContext((dynamic)message, outboxMessage);
                foreach (var descriptor in descriptors)
                    await ProcessDescriptorAsync(
                            sagaRunner,
                            context,
                            descriptor,
                            outboxMessage,
                            cancellationToken
                        )
                        .ConfigureAwait(false);

                return ProcessOutcome.Processed;
            }
            catch (Exception e) when (e is LockException or OptimisticLockException)
            {
                return ProcessOutcome.LockConflict;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "failed to process {MessageId}", outboxMessage.MessageId);
                return ProcessOutcome.Failed;
            }
        }

        private ValueTask ProcessDescriptorAsync<TMessage>(
            ISagaRunner sagaRunner,
            IMessageContext<TMessage> context,
            SagaDescriptor descriptor,
            MessageEnvelope envelope,
            CancellationToken cancellationToken
        )
            where TMessage : IMessage
        {
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "processing message {MessageId} of type {MessageType} for saga {SagaType}...",
                    envelope.MessageId,
                    envelope.MessageType,
                    descriptor.SagaType.FullName
                );

            return sagaRunner.ProcessAsync(context, descriptor, cancellationToken);
        }

        private static IMessageContext<TMessage> ToContext<TMessage>(
            TMessage message,
            MessageEnvelope envelope
        )
            where TMessage : IMessage =>
            new EnvelopeMessageContext<TMessage>(
                message,
                envelope.MessageId,
                envelope.CorrelationId,
                envelope.SenderId
            );

        private sealed record EnvelopeMessageContext<TMessage>(
            TMessage Message,
            string MessageId,
            string CorrelationId,
            string SenderId
        ) : IMessageContext<TMessage>
            where TMessage : IMessage;
    }
}
