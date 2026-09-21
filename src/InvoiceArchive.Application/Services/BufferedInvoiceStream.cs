using InvoiceArchive.Domain.Invoices;

namespace InvoiceArchive.Application.Services;

internal sealed class BufferedInvoiceStream : IAsyncDisposable
{
    private readonly IAsyncEnumerator<Invoice> _source;
    private bool _completed;

    public BufferedInvoiceStream(IAsyncEnumerable<Invoice> source)
    {
        _source = source.GetAsyncEnumerator();
    }

    public async ValueTask<Invoice?> TakeOneAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_completed)
        {
            return null;
        }

        if (await _source.MoveNextAsync().ConfigureAwait(false))
        {
            return _source.Current;
        }

        _completed = true;
        await _source.DisposeAsync().ConfigureAwait(false);
        return null;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            _completed = true;
            await _source.DisposeAsync().ConfigureAwait(false);
        }
    }
}
