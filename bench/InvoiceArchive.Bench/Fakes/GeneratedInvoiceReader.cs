using System.Runtime.CompilerServices;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Domain.Invoices;

namespace InvoiceArchive.Bench.Fakes;

internal sealed class GeneratedInvoiceReader : IInvoiceReader
{
    private readonly int _count;
    private readonly int _xmlBytes;

    public GeneratedInvoiceReader(int count, int xmlBytes)
    {
        _count = count;
        _xmlBytes = xmlBytes;
    }

    public async IAsyncEnumerable<Invoice> ReadAsync(
        InvoiceQuery query,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var payload = new string('X', Math.Max(1, _xmlBytes - 64));
        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        for (var i = 1; i <= _count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new Invoice
            {
                InvoiceId = $"INV-{i:D8}",
                Uuid = Guid.NewGuid().ToString("N"),
                Sender = "sender-bench",
                Receiver = "receiver-bench",
                Xml = $"<Invoice id=\"{i}\">{payload}</Invoice>",
                CreatedAt = baseTime.AddSeconds(i),
                TenantId = query.TenantId
            };
            if ((i & 0xFFF) == 0)
            {
                await Task.Yield();
            }
        }
    }
}
