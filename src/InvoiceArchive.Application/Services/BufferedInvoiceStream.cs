using InvoiceArchive.Domain.Invoices;

namespace InvoiceArchive.Application.Services;

internal sealed class BufferedInvoiceStream
{
    private readonly IAsyncEnumerator<Invoice> _source;
    private Invoice? _peeked;
    private bool _completed;

    public BufferedInvoiceStream(IAsyncEnumerable<Invoice> source)
    {
        _source = source.GetAsyncEnumerator();
    }

    public async ValueTask<Invoice?> PeekAsync(CancellationToken cancellationToken)
    {
        if (_peeked is not null)
        {
            return _peeked;
        }
        if (_completed)
        {
            return null;
        }

        if (await _source.MoveNextAsync().ConfigureAwait(false))
        {
            _peeked = _source.Current;
            return _peeked;
        }

        _completed = true;
        await _source.DisposeAsync().ConfigureAwait(false);
        return null;
    }

    public async IAsyncEnumerable<Invoice> TakeAsync(
        int maxCount,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var taken = 0;
        while (taken < maxCount)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Invoice? next;
            if (_peeked is not null)
            {
                next = _peeked;
                _peeked = null;
            }
            else
            {
                if (_completed)
                {
                    yield break;
                }
                if (!await _source.MoveNextAsync().ConfigureAwait(false))
                {
                    _completed = true;
                    await _source.DisposeAsync().ConfigureAwait(false);
                    yield break;
                }
                next = _source.Current;
            }

            taken++;
            yield return next;
        }
    }
}
