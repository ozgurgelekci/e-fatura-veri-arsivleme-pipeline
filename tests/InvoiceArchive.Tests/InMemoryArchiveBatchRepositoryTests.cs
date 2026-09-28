using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Infrastructure.Persistence;

namespace InvoiceArchive.Tests;

public class InMemoryArchiveBatchRepositoryTests
{
    [Fact]
    public async Task FindByStatusCompletedBefore_returns_only_matching_status_and_cutoff()
    {
        var repo = new InMemoryArchiveBatchRepository();
        var now = DateTime.UtcNow;

        await repo.SaveAsync(BuildBatch("old-verified", ArchiveStatus.Verified, now.AddDays(-40)), CancellationToken.None);
        await repo.SaveAsync(BuildBatch("recent-verified", ArchiveStatus.Verified, now.AddDays(-1)), CancellationToken.None);
        await repo.SaveAsync(BuildBatch("old-uploaded", ArchiveStatus.Uploaded, now.AddDays(-40)), CancellationToken.None);
        await repo.SaveAsync(BuildBatch("null-completed", ArchiveStatus.Verified, null), CancellationToken.None);

        var found = await repo.FindByStatusCompletedBeforeAsync(
            ArchiveStatus.Verified,
            now.AddDays(-30),
            100,
            CancellationToken.None);

        Assert.Single(found);
        Assert.Equal("old-verified", found[0].BatchId);
    }

    [Fact]
    public async Task FindByStatusCompletedBefore_respects_limit_and_orders_by_completed_at()
    {
        var repo = new InMemoryArchiveBatchRepository();
        var now = DateTime.UtcNow;

        await repo.SaveAsync(BuildBatch("b3", ArchiveStatus.Verified, now.AddDays(-40)), CancellationToken.None);
        await repo.SaveAsync(BuildBatch("b1", ArchiveStatus.Verified, now.AddDays(-60)), CancellationToken.None);
        await repo.SaveAsync(BuildBatch("b2", ArchiveStatus.Verified, now.AddDays(-50)), CancellationToken.None);

        var found = await repo.FindByStatusCompletedBeforeAsync(
            ArchiveStatus.Verified,
            now.AddDays(-30),
            2,
            CancellationToken.None);

        Assert.Equal(2, found.Count);
        Assert.Equal("b1", found[0].BatchId);
        Assert.Equal("b2", found[1].BatchId);
    }

    private static ArchiveBatch BuildBatch(string id, ArchiveStatus status, DateTime? completedAt) => new()
    {
        BatchId = id,
        Status = status,
        CreatedAt = DateTime.UtcNow.AddDays(-90),
        CompletedAt = completedAt,
        FileName = $"{id}.zip",
        StoragePath = $"2024/{id}.zip",
        InvoiceCount = 1
    };
}
