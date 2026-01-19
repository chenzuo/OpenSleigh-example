using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using OpenSleigh.Outbox;
using OpenSleigh.Transport;

namespace WebApplication7.Infrastructure
{
    public sealed class PartitionedInMemorySubscriber : IMessageSubscriber, IDisposable
    {
        private readonly IResultMessageProcessor _messageProcessor;
        private readonly ILogger<PartitionedInMemorySubscriber> _logger;
        private readonly ChannelReader<MessageEnvelope> _reader;
        private readonly IPublisher _publisher;
        private readonly PartitionedSubscriberOptions _options;
        private readonly int _partitionCount;
        private readonly Channel<MessageEnvelope>[] _partitions;
        private readonly KeyedSequentialQueue[] _keyedPartitions;
        private readonly SemaphoreSlim _requeueLimiter;
        private readonly CancellationTokenSource _cts = new();
        private Task? _routerTask;
        private Task[]? _workers;
        private static readonly ActivitySource ActivitySource = new(
            "WebApplication7.PartitionedInMemorySubscriber"
        );
        private long _processed;
        private long _lockConflicts;
        private long _failed;
        private long _requeued;
        private readonly int _maxLockRetries;

        public PartitionedInMemorySubscriber(
            IResultMessageProcessor messageProcessor,
            ChannelReader<MessageEnvelope> reader,
            ILogger<PartitionedInMemorySubscriber> logger,
            IPublisher publisher,
            IOptions<PartitionedSubscriberOptions> options
        )
        {
            _messageProcessor =
                messageProcessor ?? throw new ArgumentNullException(nameof(messageProcessor));
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _partitionCount = Math.Max(1, _options.Partitions);
            _partitions = Enumerable
                .Range(0, _partitionCount)
                .Select(_ => Channel.CreateUnbounded<MessageEnvelope>())
                .ToArray();
            _keyedPartitions = Enumerable
                .Range(0, _partitionCount)
                .Select(_ => new KeyedSequentialQueue(ProcessWithRetryAsync))
                .ToArray();
            var maxConcurrency = Math.Max(1, _options.RequeueMaxConcurrency);
            _requeueLimiter = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            _maxLockRetries = Math.Max(1, _options.MaxLockRetries);
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
            while (true)
            {
                if (ct.IsCancellationRequested)
                    break;

                var readyTask = _reader.WaitToReadAsync().AsTask();
                var cancelTask = WaitForCancellationAsync(ct);
                var completed = await Task.WhenAny(readyTask, cancelTask).ConfigureAwait(false);

                if (completed == cancelTask)
                    break;

                if (!await readyTask.ConfigureAwait(false))
                    break;

                while (_reader.TryRead(out var msg))
                {
                    var key = msg.CorrelationId ?? string.Empty;
                    var idx = (key.GetHashCode() & 0x7fffffff) % _partitionCount;
                    await _partitions[idx].Writer.WriteAsync(msg, ct).ConfigureAwait(false);
                }
            }
        }

        private async Task ConsumePartitionAsync(
            ChannelReader<MessageEnvelope> reader,
            CancellationToken ct
        )
        {
            while (true)
            {
                if (ct.IsCancellationRequested)
                    break;

                var readyTask = reader.WaitToReadAsync().AsTask();
                var cancelTask = WaitForCancellationAsync(ct);
                var completed = await Task.WhenAny(readyTask, cancelTask).ConfigureAwait(false);

                if (completed == cancelTask)
                    break;

                if (!await readyTask.ConfigureAwait(false))
                    break;

                while (reader.TryRead(out var msg))
                {
                    var key = msg.CorrelationId ?? string.Empty;
                    var idx = (key.GetHashCode() & 0x7fffffff) % _partitionCount;
                    await _keyedPartitions[idx].EnqueueAsync(msg, ct).ConfigureAwait(false);
                }
            }
        }

        private async Task ProcessWithRetryAsync(MessageEnvelope msg, CancellationToken ct)
        {
            using var activity = ActivitySource.StartActivity(
                "ProcessMessage",
                ActivityKind.Consumer
            );
            activity?.SetTag("message.id", msg.MessageId);
            activity?.SetTag("message.type", msg.MessageType);
            activity?.SetTag("message.correlation_id", msg.CorrelationId ?? string.Empty);

            var delay = TimeSpan.FromMilliseconds(50);
            for (var attempt = 0; attempt < _maxLockRetries; attempt++)
            {
                var outcome = await _messageProcessor.ProcessAsync(msg, ct).ConfigureAwait(false);
                switch (outcome)
                {
                    case ProcessOutcome.Processed:
                        Interlocked.Increment(ref _processed);
                        activity?.SetTag("result", "processed");
                        return;
                    case ProcessOutcome.LockConflict:
                        Interlocked.Increment(ref _lockConflicts);
                        _logger.LogWarning(
                            "lock conflict for {MessageId}, retry {Attempt}",
                            msg.MessageId,
                            attempt + 1
                        );
                        await Task.Delay(delay, ct).ConfigureAwait(false);
                        delay = TimeSpan.FromMilliseconds(
                            Math.Min(delay.TotalMilliseconds * 2, 1000)
                        );
                        break;
                    case ProcessOutcome.Failed:
                        Interlocked.Increment(ref _failed);
                        activity?.SetTag("result", "failed");
                        return;
                }
            }

            activity?.SetTag("result", "requeue");
            await RequeueWithBackoffAsync(msg, ct).ConfigureAwait(false);
        }

        private async Task RequeueWithBackoffAsync(MessageEnvelope msg, CancellationToken ct)
        {
            var baseDelayMs = Math.Max(0, _options.RequeueDelayMs);
            var jitterMs = Math.Max(0, _options.RequeueJitterMs);
            var jitter = jitterMs > 0 ? Random.Shared.Next(0, jitterMs) : 0;
            var delay = TimeSpan.FromMilliseconds(baseDelayMs + jitter);

            await using var _ = await WaitReleaserAsync(_requeueLimiter, ct).ConfigureAwait(false);
            await Task.Delay(delay, ct).ConfigureAwait(false);
            await _publisher.PublishAsync(msg, ct).ConfigureAwait(false);
            Interlocked.Increment(ref _requeued);

            if (_logger.IsEnabled(LogLevel.Warning))
                _logger.LogWarning(
                    "requeued {MessageId} after lock retries. delay={DelayMs}ms processed={Processed} lockConflicts={LockConflicts} failed={Failed} requeued={Requeued}",
                    msg.MessageId,
                    delay.TotalMilliseconds,
                    Interlocked.Read(ref _processed),
                    Interlocked.Read(ref _lockConflicts),
                    Interlocked.Read(ref _failed),
                    Interlocked.Read(ref _requeued)
                );
        }

        private static async ValueTask<SemaphoreReleaser> WaitReleaserAsync(
            SemaphoreSlim semaphore,
            CancellationToken ct
        )
        {
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            return new SemaphoreReleaser(semaphore);
        }

        private static Task WaitForCancellationAsync(CancellationToken ct)
        {
            if (!ct.CanBeCanceled)
                return Task.Delay(Timeout.InfiniteTimeSpan, ct);

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var registration = ct.Register(
                static state =>
                {
                    var source = (TaskCompletionSource)state!;
                    source.TrySetResult();
                },
                tcs
            );

            tcs.Task.ContinueWith(_ => registration.Dispose(), TaskScheduler.Default);

            return tcs.Task;
        }

        private readonly struct SemaphoreReleaser : IAsyncDisposable
        {
            private readonly SemaphoreSlim _semaphore;

            public SemaphoreReleaser(SemaphoreSlim semaphore)
            {
                _semaphore = semaphore;
            }

            public ValueTask DisposeAsync()
            {
                _semaphore.Release();
                return ValueTask.CompletedTask;
            }
        }

        private sealed class KeyedSequentialQueue
        {
            private readonly Func<MessageEnvelope, CancellationToken, Task> _handler;
            private readonly ConcurrentDictionary<string, Task> _tails = new();

            public KeyedSequentialQueue(Func<MessageEnvelope, CancellationToken, Task> handler)
            {
                _handler = handler ?? throw new ArgumentNullException(nameof(handler));
            }

            public ValueTask EnqueueAsync(MessageEnvelope msg, CancellationToken ct)
            {
                var key = msg.CorrelationId ?? string.Empty;
                Task next = _tails.AddOrUpdate(
                    key,
                    _ => _handler(msg, ct),
                    (_, prev) =>
                        prev.ContinueWith(
                                _ => _handler(msg, ct),
                                ct,
                                TaskContinuationOptions.ExecuteSynchronously,
                                TaskScheduler.Default
                            )
                            .Unwrap()
                );

                return new ValueTask(next);
            }
        }

        public void Dispose() => _cts.Cancel();
    }
}
