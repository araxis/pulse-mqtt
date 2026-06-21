using System.Threading.Tasks.Dataflow;
using System.Diagnostics.CodeAnalysis;

namespace Pulse.Mqtt.Dataflow;

/// <summary>A bounded Dataflow source block backed by an MQTT stream.</summary>
/// <typeparam name="T">The message type produced by the source.</typeparam>
public sealed class MqttDataflowSource<T> : IReceivableSourceBlock<T>, IAsyncDisposable
{
    private readonly BufferBlock<T> _buffer;
    private readonly CancellationTokenSource _lifetime;
    private readonly IAsyncDisposable? _ownedResource;
    private readonly Task _pump;
    private readonly Task _completion;
    private int _stopping;
    private bool _disposed;

    internal MqttDataflowSource(
        IAsyncEnumerable<T> source,
        MqttDataflowSourceOptions? options = null,
        IAsyncDisposable? ownedResource = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);

        var capacity = MqttDataflowSourceOptions.Validate(options);

        _buffer = new BufferBlock<T>(new DataflowBlockOptions
        {
            BoundedCapacity = capacity,
            EnsureOrdered = true,
        });
        _lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ownedResource = ownedResource;
        _pump = Task.Run(() => PumpAsync(source, _lifetime.Token), CancellationToken.None);
        _completion = Task.WhenAll(_pump, _buffer.Completion);
    }

    /// <inheritdoc />
    public Task Completion => _completion;

    /// <inheritdoc />
    public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(linkOptions);

        return _buffer.LinkTo(target, linkOptions);
    }

    /// <inheritdoc />
    public void Complete()
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            _buffer.Complete();
            _lifetime.Cancel();
        }
    }

    /// <inheritdoc />
    public void Fault(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            ((IDataflowBlock)_buffer).Fault(exception);
            _lifetime.Cancel();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        Complete();

        try
        {
            await _pump.ConfigureAwait(false);
        }
        finally
        {
            _lifetime.Dispose();
        }
    }

    T ISourceBlock<T>.ConsumeMessage(
        DataflowMessageHeader messageHeader,
        ITargetBlock<T> target,
        out bool messageConsumed) =>
        ((ISourceBlock<T>)_buffer).ConsumeMessage(messageHeader, target, out messageConsumed)!;

    bool ISourceBlock<T>.ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
        ((ISourceBlock<T>)_buffer).ReserveMessage(messageHeader, target);

    void ISourceBlock<T>.ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
        ((ISourceBlock<T>)_buffer).ReleaseReservation(messageHeader, target);

    bool IReceivableSourceBlock<T>.TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item) =>
        _buffer.TryReceive(filter, out item!);

    bool IReceivableSourceBlock<T>.TryReceiveAll([NotNullWhen(true)] out IList<T>? items) =>
        _buffer.TryReceiveAll(out items);

    private async Task PumpAsync(IAsyncEnumerable<T> source, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await _buffer.SendAsync(item, cancellationToken).ConfigureAwait(false);
            }

            _buffer.Complete();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _buffer.Complete();
        }
        catch (Exception error)
        {
            ((IDataflowBlock)_buffer).Fault(error);
        }
        finally
        {
            if (_ownedResource is not null)
            {
                await _ownedResource.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
