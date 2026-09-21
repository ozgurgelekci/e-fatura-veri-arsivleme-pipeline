using System.ComponentModel.DataAnnotations;

namespace InvoiceArchive.Infrastructure.Configuration;

public sealed class ElasticsearchOptions
{
    public const string SectionName = "Elasticsearch";

    [Required, MinLength(1)]
    public string Uri { get; set; } = "http://localhost:9200";

    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? ApiKey { get; set; }
    public bool DisableCertificateValidation { get; set; }

    [Range(1, 600)]
    public int RequestTimeoutSeconds { get; set; } = 60;
}
