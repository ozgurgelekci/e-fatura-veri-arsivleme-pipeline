using InvoiceArchive.Domain.Archives;

namespace InvoiceArchive.Application.Abstractions;

public interface IArchivedInvoiceReaper
{
    Task<ReapResult> ReapAsync(
        ArchiveBatch batch,
        string indexName,
        bool dryRun,
        CancellationToken cancellationToken);
}

public sealed record ReapResult(long MatchedCount, long DeletedCount, bool DryRun);
