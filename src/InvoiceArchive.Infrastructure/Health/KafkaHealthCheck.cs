using Confluent.Kafka;
using Confluent.Kafka.Admin;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Infrastructure.Health;

public sealed class KafkaHealthCheck : IHealthCheck
{
    private readonly KafkaOptions _options;

    public KafkaHealthCheck(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
    }

    public string Name => "kafka";

    public async Task<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId + "-health"
        };

        if (!string.IsNullOrEmpty(_options.SecurityProtocol))
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true);
        }
        if (!string.IsNullOrEmpty(_options.SaslMechanism))
        {
            config.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true);
        }
        if (!string.IsNullOrEmpty(_options.SaslUsername))
        {
            config.SaslUsername = _options.SaslUsername;
            config.SaslPassword = _options.SaslPassword;
        }

        try
        {
            using var admin = new AdminClientBuilder(config).Build();
            var metadata = await Task.Run(
                () => admin.GetMetadata(TimeSpan.FromSeconds(3)),
                cancellationToken).ConfigureAwait(false);
            return metadata.Brokers.Count > 0
                ? HealthCheckResult.Ok()
                : HealthCheckResult.Fail("No brokers reachable");
        }
        catch (KafkaException ex)
        {
            return HealthCheckResult.Fail(ex.Message);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Fail(ex.Message);
        }
    }
}
