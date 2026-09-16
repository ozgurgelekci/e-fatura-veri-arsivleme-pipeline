using InvoiceArchive.Domain.Invoices;

namespace InvoiceArchive.Application.Abstractions;

public interface IInvoiceReader
{
    IAsyncEnumerable<Invoice> ReadAsync(InvoiceQuery query, CancellationToken cancellationToken);
}

public sealed record InvoiceQuery(
    string IndexName,
    DateTime? CreatedBefore,
    string? TenantId,
    int PageSize,
    TimeSpan PitKeepAlive
);
