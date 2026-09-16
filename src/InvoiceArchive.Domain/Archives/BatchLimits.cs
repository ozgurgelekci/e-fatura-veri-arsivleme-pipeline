namespace InvoiceArchive.Domain.Archives;

public sealed record BatchLimits(int MaxRecordCount, long MaxArchiveSizeBytes)
{
    public bool IsFull(int currentCount, long currentSize)
        => currentCount >= MaxRecordCount || currentSize >= MaxArchiveSizeBytes;
}
