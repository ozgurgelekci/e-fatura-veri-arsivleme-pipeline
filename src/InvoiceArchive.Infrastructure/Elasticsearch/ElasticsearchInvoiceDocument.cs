using System.Text.Json.Serialization;

namespace InvoiceArchive.Infrastructure.Elasticsearch;

internal sealed class ElasticsearchInvoiceDocument
{
    [JsonPropertyName("invoiceId")]
    public string InvoiceId { get; set; } = default!;

    [JsonPropertyName("uuid")]
    public string? Uuid { get; set; }

    [JsonPropertyName("sender")]
    public string? Sender { get; set; }

    [JsonPropertyName("receiver")]
    public string? Receiver { get; set; }

    [JsonPropertyName("xml")]
    public string Xml { get; set; } = default!;

    [JsonPropertyName("createdAt")]
    public DateTime CreatedAt { get; set; }

    [JsonPropertyName("tenantId")]
    public string? TenantId { get; set; }
}
