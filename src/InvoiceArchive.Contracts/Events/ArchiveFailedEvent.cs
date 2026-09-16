namespace InvoiceArchive.Contracts.Events;

public sealed record ArchiveFailedEvent
{
    public required string EventId { get; init; }
    public required string BatchId { get; init; }
    public required string Reason { get; init; }
    public required int AttemptCount { get; init; }
    public required DateTime FailedAt { get; init; }
    public string? TenantId { get; init; }
}
