namespace InvoiceArchive.Contracts.Events;

public sealed record ArchiveCreatedEvent
{
    public required string EventId { get; init; }
    public required string BatchId { get; init; }
    public required string FileName { get; init; }
    public required long Size { get; init; }
    public required int InvoiceCount { get; init; }
    public required string Storage { get; init; }
    public required string Bucket { get; init; }
    public required string Path { get; init; }
    public required string Sha256 { get; init; }
    public required DateTime CreatedAt { get; init; }
    public string? TenantId { get; init; }
    public string? FirstInvoiceId { get; init; }
    public string? LastInvoiceId { get; init; }
}
