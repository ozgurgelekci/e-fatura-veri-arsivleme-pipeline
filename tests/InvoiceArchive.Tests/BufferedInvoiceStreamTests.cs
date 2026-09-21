using InvoiceArchive.Application.Services;
using InvoiceArchive.Domain.Invoices;

namespace InvoiceArchive.Tests;

public class BufferedInvoiceStreamTests
{
    [Fact]
    public async Task TakeOneAsync_returns_null_when_source_empty()
    {
        await using var buffer = new BufferedInvoiceStream(Empty());

        var next = await buffer.TakeOneAsync(CancellationToken.None);

        Assert.Null(next);
    }

    [Fact]
    public async Task TakeOneAsync_yields_items_in_order_then_null()
    {
        await using var buffer = new BufferedInvoiceStream(Produce(3));

        var a = await buffer.TakeOneAsync(CancellationToken.None);
        var b = await buffer.TakeOneAsync(CancellationToken.None);
        var c = await buffer.TakeOneAsync(CancellationToken.None);
        var end = await buffer.TakeOneAsync(CancellationToken.None);

        Assert.NotNull(a); Assert.Equal("INV-1", a!.InvoiceId);
        Assert.NotNull(b); Assert.Equal("INV-2", b!.InvoiceId);
        Assert.NotNull(c); Assert.Equal("INV-3", c!.InvoiceId);
        Assert.Null(end);
    }

    [Fact]
    public async Task TakeOneAsync_returns_null_after_completion_without_moving_source()
    {
        var source = new TrackingEnumerable(2);
        await using var buffer = new BufferedInvoiceStream(source);

        await buffer.TakeOneAsync(CancellationToken.None);
        await buffer.TakeOneAsync(CancellationToken.None);
        await buffer.TakeOneAsync(CancellationToken.None); // triggers completion
        var movesBefore = source.MoveCount;

        var second = await buffer.TakeOneAsync(CancellationToken.None);

        Assert.Null(second);
        Assert.Equal(movesBefore, source.MoveCount);
    }

    [Fact]
    public async Task TakeOneAsync_throws_on_cancellation()
    {
        await using var buffer = new BufferedInvoiceStream(Produce(5));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await buffer.TakeOneAsync(cts.Token));
    }

    private static async IAsyncEnumerable<Invoice> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<Invoice> Produce(int count)
    {
        for (var i = 1; i <= count; i++)
        {
            yield return new Invoice
            {
                InvoiceId = $"INV-{i}",
                Xml = $"<Invoice id=\"{i}\"/>",
                CreatedAt = new DateTime(2026, 1, 1, 0, 0, i, DateTimeKind.Utc)
            };
            await Task.Yield();
        }
    }

    private sealed class TrackingEnumerable : IAsyncEnumerable<Invoice>
    {
        private readonly int _count;
        public int MoveCount;

        public TrackingEnumerable(int count) => _count = count;

        public IAsyncEnumerator<Invoice> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => new Enumerator(this, _count);

        private sealed class Enumerator : IAsyncEnumerator<Invoice>
        {
            private readonly TrackingEnumerable _owner;
            private readonly int _count;
            private int _i;

            public Enumerator(TrackingEnumerable owner, int count)
            {
                _owner = owner;
                _count = count;
            }

            public Invoice Current { get; private set; } = default!;

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            public ValueTask<bool> MoveNextAsync()
            {
                _owner.MoveCount++;
                if (_i >= _count)
                {
                    return ValueTask.FromResult(false);
                }
                _i++;
                Current = new Invoice
                {
                    InvoiceId = $"INV-{_i}",
                    Xml = "<Invoice/>",
                    CreatedAt = new DateTime(2026, 1, 1, 0, 0, _i, DateTimeKind.Utc)
                };
                return ValueTask.FromResult(true);
            }
        }
    }
}
