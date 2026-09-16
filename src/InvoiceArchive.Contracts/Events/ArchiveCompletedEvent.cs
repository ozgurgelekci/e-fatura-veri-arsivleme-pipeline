namespace InvoiceArchive.Contracts.Events;

public sealed record ArchiveCompletedEvent
{
    public required string EventId { get; init; }
    public required string BatchId { get; init; }
    public required DateTime CompletedAt { get; init; }
    public required string Path { get; init; }
    public required string Sha256 { get; init; }
    public string? TenantId { get; init; }
}
