namespace InvoiceArchive.Domain.Archives;

public sealed class ArchiveBatch
{
    public required string BatchId { get; init; }
    public string? TenantId { get; init; }
    public int InvoiceCount { get; set; }
    public long SizeInBytes { get; set; }
    public string FileName { get; set; } = default!;
    public string StoragePath { get; set; } = default!;
    public string? Sha256 { get; set; }
    public DateTime CreatedAt { get; init; }
    public DateTime? CompletedAt { get; set; }
    public ArchiveStatus Status { get; set; } = ArchiveStatus.Pending;
    public int RetryCount { get; set; }
    public string? ErrorMessage { get; set; }
    public string? FirstInvoiceId { get; set; }
    public string? LastInvoiceId { get; set; }
}
