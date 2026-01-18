using System.Threading.Channels;
using OpenSleigh;
using OpenSleigh.Outbox;
using OpenSleigh.Transport;

namespace WebApplication7.Infrastructure
{
    public sealed class PartitionedInMemorySubscriber : IMessageSubscriber, IDisposable
    {
        private readonly IMessageProcessor _messageProcessor;
        private readonly ILogger<PartitionedInMemorySubscriber> _logger;
        private readonly ChannelReader<MessageEnvelope> _reader;
        private readonly IPublisher _publisher;
        private readonly int _partitionCount;
        private readonly Channel<MessageEnvelope>[] _partitions;
        private readonly CancellationTokenSource _cts = new();
        private Task? _routerTask;
        private Task[]? _workers;

        public PartitionedInMemorySubscriber(
            IMessageProcessor messageProcessor,
            ChannelReader<MessageEnvelope> reader,
            ILogger<PartitionedInMemorySubscriber> logger,
            IPublisher publisher,
            int partitionCount = 8
        )
        {
            _messageProcessor =
                messageProcessor ?? throw new ArgumentNullException(nameof(messageProcessor));
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
            _partitionCount = Math.Max(1, partitionCount);
            _partitions = Enumerable
                .Range(0, _partitionCount)
                .Select(_ => Channel.CreateUnbounded<MessageEnvelope>())
                .ToArray();
        }

        public ValueTask StartAsync(CancellationToken cancellationToken = default)
        {
            var linked = CancellationTokenSource.CreateLinkedTokenSource(
                _cts.Token,
                cancellationToken
            );
            _routerTask = Task.Run(() => RouteAsync(linked.Token), linked.Token);
            _workers = _partitions
                .Select(p =>
                    Task.Run(() => ConsumePartitionAsync(p.Reader, linked.Token), linked.Token)
                )
                .ToArray();
            return ValueTask.CompletedTask;
        }

        public async ValueTask StopAsync(CancellationToken cancellationToken = default)
        {
            _cts.Cancel();
            if (_routerTask is not null)
                await _routerTask.ConfigureAwait(false);
            if (_workers is not null)
                await Task.WhenAll(_workers).ConfigureAwait(false);
        }

        private async Task RouteAsync(CancellationToken ct)
        {
            await foreach (var msg in _reader.ReadAllAsync(ct))
            {
                var key = msg.CorrelationId ?? string.Empty;
                var idx = (key.GetHashCode() & 0x7fffffff) % _partitionCount;
                await _partitions[idx].Writer.WriteAsync(msg, ct).ConfigureAwait(false);
            }
        }

        private async Task ConsumePartitionAsync(
            ChannelReader<MessageEnvelope> reader,
            CancellationToken ct
        )
        {
            await foreach (var msg in reader.ReadAllAsync(ct))
            {
                await ProcessWithRetryAsync(msg, ct).ConfigureAwait(false);
            }
        }

        private async Task ProcessWithRetryAsync(MessageEnvelope msg, CancellationToken ct)
        {
            var delay = TimeSpan.FromMilliseconds(50);
            for (var attempt = 0; attempt < 6; attempt++)
            {
                try
                {
                    await _messageProcessor.ProcessAsync(msg, ct).ConfigureAwait(false);
                    return;
                }
                catch (Exception e) when (e is LockException or OptimisticLockException)
                {
                    _logger.LogWarning(
                        e,
                        "lock conflict for {MessageId}, retry {Attempt}",
                        msg.MessageId,
                        attempt + 1
                    );
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 1000));
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "failed to process {MessageId}", msg.MessageId);
                    return;
                }
            }

            await _publisher.PublishAsync(msg, ct).ConfigureAwait(false);
        }

        public void Dispose() => _cts.Cancel();
    }
}
