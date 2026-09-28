using System.Collections.Concurrent;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Domain.Archives;

namespace InvoiceArchive.Infrastructure.Persistence;

public sealed class InMemoryArchiveBatchRepository : IArchiveBatchRepository
{
    private readonly ConcurrentDictionary<string, ArchiveBatch> _store = new();

    public Task SaveAsync(ArchiveBatch batch, CancellationToken cancellationToken)
    {
        _store[batch.BatchId] = Clone(batch);
        return Task.CompletedTask;
    }

    public Task<ArchiveBatch?> FindAsync(string batchId, CancellationToken cancellationToken)
    {
        _store.TryGetValue(batchId, out var found);
        return Task.FromResult(found is null ? null : Clone(found));
    }

    public Task<IReadOnlyList<ArchiveBatch>> FindByStatusCompletedBeforeAsync(
        ArchiveStatus status,
        DateTime completedBeforeUtc,
        int limit,
        CancellationToken cancellationToken)
    {
        var results = _store.Values
            .Where(b => b.Status == status
                        && b.CompletedAt.HasValue
                        && b.CompletedAt.Value < completedBeforeUtc)
            .OrderBy(b => b.CompletedAt!.Value)
            .Take(limit)
            .Select(Clone)
            .ToList();

        return Task.FromResult<IReadOnlyList<ArchiveBatch>>(results);
    }

    private static ArchiveBatch Clone(ArchiveBatch b) => new()
    {
        BatchId = b.BatchId,
        TenantId = b.TenantId,
        InvoiceCount = b.InvoiceCount,
        SizeInBytes = b.SizeInBytes,
        FileName = b.FileName,
        StoragePath = b.StoragePath,
        Sha256 = b.Sha256,
        CreatedAt = b.CreatedAt,
        CompletedAt = b.CompletedAt,
        Status = b.Status,
        RetryCount = b.RetryCount,
        ErrorMessage = b.ErrorMessage,
        FirstInvoiceId = b.FirstInvoiceId,
        LastInvoiceId = b.LastInvoiceId,
        FirstInvoiceCreatedAt = b.FirstInvoiceCreatedAt,
        LastInvoiceCreatedAt = b.LastInvoiceCreatedAt
    };
}
