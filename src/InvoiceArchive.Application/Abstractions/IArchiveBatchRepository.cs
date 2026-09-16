using InvoiceArchive.Domain.Archives;

namespace InvoiceArchive.Application.Abstractions;

public interface IArchiveBatchRepository
{
    Task SaveAsync(ArchiveBatch batch, CancellationToken cancellationToken);
    Task<ArchiveBatch?> FindAsync(string batchId, CancellationToken cancellationToken);
}
