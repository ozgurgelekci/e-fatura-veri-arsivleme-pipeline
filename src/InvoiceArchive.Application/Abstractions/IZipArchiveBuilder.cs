using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Domain.Invoices;

namespace InvoiceArchive.Application.Abstractions;

public interface IZipArchiveBuilder
{
    Task<ZipBuildResult> BuildAsync(
        string batchId,
        string? tenantId,
        IAsyncEnumerable<Invoice> invoices,
        Stream destination,
        BatchLimits limits,
        CancellationToken cancellationToken);
}

public sealed record ZipBuildResult(
    int InvoiceCount,
    long SizeInBytes,
    string Sha256,
    string? FirstInvoiceId,
    string? LastInvoiceId,
    bool LimitReached
);
