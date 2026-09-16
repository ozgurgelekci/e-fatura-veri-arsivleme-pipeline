using System.Text.Json;
using Confluent.Kafka;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Contracts.Events;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Infrastructure.Kafka;

public sealed class KafkaArchiveEventPublisher : IArchiveEventPublisher, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaArchiveEventPublisher> _logger;

    public KafkaArchiveEventPublisher(
        IOptions<KafkaOptions> options,
        ILogger<KafkaArchiveEventPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId,
            EnableIdempotence = _options.EnableIdempotence,
            MessageSendMaxRetries = _options.MessageSendMaxRetries,
            Acks = Acks.All,
            CompressionType = CompressionType.Snappy
        };

        ApplySasl(config);

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka producer error: {Reason}", e.Reason))
            .Build();
    }

    public Task PublishCreatedAsync(ArchiveCreatedEvent evt, CancellationToken cancellationToken)
        => PublishAsync(_options.ArchiveCreatedTopic, evt.BatchId, evt, evt.TenantId, cancellationToken);

    public Task PublishCompletedAsync(ArchiveCompletedEvent evt, CancellationToken cancellationToken)
        => PublishAsync(_options.ArchiveCompletedTopic, evt.BatchId, evt, evt.TenantId, cancellationToken);

    public Task PublishFailedAsync(ArchiveFailedEvent evt, CancellationToken cancellationToken)
        => PublishAsync(_options.ArchiveFailedTopic, evt.BatchId, evt, evt.TenantId, cancellationToken);

    private async Task PublishAsync<T>(string topic, string key, T payload, string? tenantId, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(payload, JsonOptions);
        var partitionKey = string.IsNullOrEmpty(tenantId) ? key : tenantId;

        var message = new Message<string, string>
        {
            Key = partitionKey,
            Value = json,
            Headers = new Headers
            {
                { "content-type", System.Text.Encoding.UTF8.GetBytes("application/json") },
                { "event-type", System.Text.Encoding.UTF8.GetBytes(typeof(T).Name) }
            }
        };

        var result = await _producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Published event to {Topic} Partition={Partition} Offset={Offset} BatchId={BatchId}",
            topic, result.Partition.Value, result.Offset.Value, key);
    }

    private void ApplySasl(ClientConfig config)
    {
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
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
