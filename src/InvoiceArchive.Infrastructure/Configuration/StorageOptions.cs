namespace InvoiceArchive.Infrastructure.Configuration;

public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    public string Provider { get; set; } = "MinIO";
    public string? ServiceUrl { get; set; } = "http://localhost:9000";
    public string? Region { get; set; } = "us-east-1";
    public string? AccessKey { get; set; }
    public string? SecretKey { get; set; }
    public bool ForcePathStyle { get; set; } = true;
    public bool ServerSideEncryption { get; set; }
    public int MaxRetryAttempts { get; set; } = 5;
    public int InitialRetryDelaySeconds { get; set; } = 5;
}
