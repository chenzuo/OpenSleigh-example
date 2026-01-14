using System.Collections.Immutable;
using OpenSleigh;
using OpenSleigh.Outbox;
using OpenSleigh.Transport;

namespace WebApplication7.Infrastructure
{
    public class ScopedMessageProcessor : IMessageProcessor
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ScopedMessageProcessor> _logger;

        public ScopedMessageProcessor(
            IServiceScopeFactory scopeFactory,
            ILogger<ScopedMessageProcessor> logger
        )
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async ValueTask ProcessAsync(
            MessageEnvelope outboxMessage,
            CancellationToken cancellationToken = default
        )
        {
            ArgumentNullException.ThrowIfNull(outboxMessage);

            using var scope = _scopeFactory.CreateScope();

            var sagaRunner = scope.ServiceProvider.GetRequiredService<ISagaRunner>();
            var descriptorsResolver =
                scope.ServiceProvider.GetRequiredService<ISagaDescriptorsResolver>();

            var message =
                outboxMessage.Message
                ?? throw new InvalidOperationException("message payload was not found.");
            var messageContext = ToContext((dynamic)message, outboxMessage);

            //    await foreach (var descriptor in descriptorsResolver.Resolve(message))
            //     {
            //         await sagaRunner
            //             .ProcessAsync(messageContext, descriptor, cancellationToken)
            //             .ConfigureAwait(false);
            //     }

            var descriptors = descriptorsResolver.Resolve(message).ToAsyncEnumerable();

            await foreach (var descriptor in descriptors)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation(
                        "processing message {MessageId} of type {MessageType} for saga {SagaType}...",
                        outboxMessage.MessageId,
                        outboxMessage.MessageType,
                        descriptor.SagaType.FullName
                    );

                await sagaRunner
                    .ProcessAsync(messageContext, descriptor, cancellationToken)
                    .ConfigureAwait(false);
            }
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
