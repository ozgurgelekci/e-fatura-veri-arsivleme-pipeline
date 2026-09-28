using System.ComponentModel.DataAnnotations;

namespace InvoiceArchive.Infrastructure.Configuration;

public sealed class PersistenceOptions
{
    public const string SectionName = "Persistence";

    [Required, MinLength(1)]
    public string Provider { get; set; } = "InMemory";

    public string? ConnectionString { get; set; }

    [MinLength(1)]
    public string SchemaName { get; set; } = "public";

    [MinLength(1)]
    public string TableName { get; set; } = "archive_batches";

    public bool AutoMigrate { get; set; } = true;
}
