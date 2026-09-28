using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Infrastructure;
using InvoiceArchive.Infrastructure.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace InvoiceArchive.Tests;

public class ArchiveBatchRepositoryDependencyInjectionTests
{
    [Fact]
    public void Defaults_to_in_memory_repository_when_provider_missing()
    {
        var services = BuildServices(new Dictionary<string, string?>());
        var repo = services.GetRequiredService<IArchiveBatchRepository>();
        Assert.IsType<InMemoryArchiveBatchRepository>(repo);
    }

    [Fact]
    public void Registers_in_memory_repository_when_provider_is_in_memory()
    {
        var services = BuildServices(new Dictionary<string, string?>
        {
            ["Persistence:Provider"] = "InMemory"
        });
        var repo = services.GetRequiredService<IArchiveBatchRepository>();
        Assert.IsType<InMemoryArchiveBatchRepository>(repo);
    }

    [Fact]
    public void Registers_postgres_repository_when_provider_is_postgres()
    {
        var services = BuildServices(new Dictionary<string, string?>
        {
            ["Persistence:Provider"] = "Postgres",
            ["Persistence:ConnectionString"] = "Host=localhost;Username=u;Password=p;Database=d"
        });
        var repo = services.GetRequiredService<IArchiveBatchRepository>();
        Assert.IsType<PostgresArchiveBatchRepository>(repo);
    }

    [Fact]
    public void Postgres_provider_without_connection_string_throws_at_resolve()
    {
        var services = BuildServices(new Dictionary<string, string?>
        {
            ["Persistence:Provider"] = "Postgres"
        });
        var ex = Assert.Throws<InvalidOperationException>(
            () => services.GetRequiredService<IArchiveBatchRepository>());
        Assert.Contains("ConnectionString", ex.Message);
    }

    private static IServiceProvider BuildServices(Dictionary<string, string?> settings)
    {
        var baseline = new Dictionary<string, string?>
        {
            ["Elasticsearch:Uri"] = "http://localhost:9200",
            ["Storage:Provider"] = "MinIO",
            ["Storage:ServiceUrl"] = "http://localhost:9000",
            ["Kafka:BootstrapServers"] = "localhost:9092",
            ["Kafka:ClientId"] = "test"
        };
        foreach (var kvp in settings)
        {
            baseline[kvp.Key] = kvp.Value;
        }

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(baseline)
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInvoiceArchiveInfrastructure(configuration);
        return services.BuildServiceProvider();
    }
}
