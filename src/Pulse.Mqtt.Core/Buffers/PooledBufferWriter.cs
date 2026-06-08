using System.Buffers;

namespace Pulse.Mqtt.Buffers;

/// <summary>
/// A growable <see cref="IBufferWriter{T}"/> backed by an array rented from <see cref="ArrayPool{T}"/>.
/// Use it to assemble a length-prefixed section whose size is not known up front, read the result
/// via <see cref="WrittenSpan"/>, then dispose to return the buffer to the pool.
/// </summary>
public sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;

    /// <summary>Creates a writer with an initial pooled capacity.</summary>
    /// <param name="initialCapacity">The minimum initial buffer size, in bytes.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialCapacity"/> is not positive.</exception>
    public PooledBufferWriter(int initialCapacity = 256)
    {
        if (initialCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialCapacity), initialCapacity, "Initial capacity must be positive.");
        }

        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        _written = 0;
    }

    /// <summary>Number of bytes written so far.</summary>
    public int WrittenCount => _written;

    /// <summary>The bytes written so far.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

    /// <inheritdoc />
    public void Advance(int count)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "Count must be non-negative.");
        }

        if (_written + count > _buffer.Length)
        {
            throw new InvalidOperationException("Cannot advance past the end of the buffer.");
        }

        _written += count;
    }

    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    /// <summary>Resets the writer to empty without returning the buffer, so it can be reused.</summary>
    public void Reset() => _written = 0;

    /// <summary>Returns the rented buffer to the pool.</summary>
    public void Dispose()
    {
        var toReturn = _buffer;
        _buffer = [];
        _written = 0;
        if (toReturn.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(toReturn);
        }
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 1)
        {
            sizeHint = 1;
        }

        if (sizeHint <= _buffer.Length - _written)
        {
            return;
        }

        var required = _written + sizeHint;
        var newSize = Math.Max(required, _buffer.Length * 2);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _written).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }
}
