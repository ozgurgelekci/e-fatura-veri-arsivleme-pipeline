namespace InvoiceArchive.Domain.Archives;

public sealed class ArchiveManifest
{
    public required string BatchId { get; init; }
    public required DateTime CreatedAt { get; init; }
    public required int InvoiceCount { get; init; }
    public required string Compression { get; init; }
    public int ArchiveVersion { get; init; } = 1;
    public string? FirstInvoiceId { get; init; }
    public string? LastInvoiceId { get; init; }
    public string? TenantId { get; init; }
}
