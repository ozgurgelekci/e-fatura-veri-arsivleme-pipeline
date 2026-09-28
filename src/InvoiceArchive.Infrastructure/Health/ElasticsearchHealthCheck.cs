using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Infrastructure.Elasticsearch;

namespace InvoiceArchive.Infrastructure.Health;

public sealed class ElasticsearchHealthCheck : IHealthCheck
{
    private readonly ElasticsearchClientFactory _factory;

    public ElasticsearchHealthCheck(ElasticsearchClientFactory factory)
    {
        _factory = factory;
    }

    public string Name => "elasticsearch";

    public async Task<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
    {
        try
        {
            var response = await _factory.Client.PingAsync(cancellationToken).ConfigureAwait(false);
            return response.IsValidResponse
                ? HealthCheckResult.Ok()
                : HealthCheckResult.Fail(response.DebugInformation);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Fail(ex.Message);
        }
    }
}
