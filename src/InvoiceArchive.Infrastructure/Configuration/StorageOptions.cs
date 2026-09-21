using System.ComponentModel.DataAnnotations;

namespace InvoiceArchive.Infrastructure.Configuration;

public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    [Required, MinLength(1)]
    public string Provider { get; set; } = "MinIO";

    public string? ServiceUrl { get; set; } = "http://localhost:9000";
    public string? Region { get; set; } = "us-east-1";
    public string? AccessKey { get; set; }
    public string? SecretKey { get; set; }
    public bool ForcePathStyle { get; set; } = true;
    public bool ServerSideEncryption { get; set; }

    [Range(0, 20)]
    public int MaxRetryAttempts { get; set; } = 5;

    [Range(1, 300)]
    public int InitialRetryDelaySeconds { get; set; } = 5;
}
