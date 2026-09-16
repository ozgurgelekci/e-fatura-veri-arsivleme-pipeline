namespace InvoiceArchive.Domain.Invoices;

public sealed class Invoice
{
    public required string InvoiceId { get; init; }
    public string? Uuid { get; init; }
    public string? Sender { get; init; }
    public string? Receiver { get; init; }
    public required string Xml { get; init; }
    public required DateTime CreatedAt { get; init; }
    public string? TenantId { get; init; }
}
