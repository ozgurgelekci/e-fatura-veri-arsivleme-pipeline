namespace InvoiceArchive.Infrastructure.Zip;

internal sealed record InvoiceMetadata
{
    public required string InvoiceId { get; init; }
    public string? Uuid { get; init; }
    public string? Sender { get; init; }
    public string? Receiver { get; init; }
    public DateTime CreatedAt { get; init; }
    public string? TenantId { get; init; }
}
