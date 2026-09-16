using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using InvoiceArchive.Contracts.Events;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Infrastructure.Kafka;

public sealed class KafkaArchiveEventConsumer : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _dlqProducer;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaArchiveEventConsumer> _logger;

    public KafkaArchiveEventConsumer(
        IOptions<KafkaOptions> options,
        ILogger<KafkaArchiveEventConsumer> logger)
    {
        _options = options.Value;
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            ClientId = _options.ClientId + "-consumer"
        };
        ApplySasl(consumerConfig);

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka consumer error: {Reason}", e.Reason))
            .Build();

        var dlqConfig = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId + "-dlq",
            EnableIdempotence = true,
            Acks = Acks.All
        };
        ApplySasl(dlqConfig);

        _dlqProducer = new ProducerBuilder<string, string>(dlqConfig).Build();
    }

    public async Task ConsumeAsync(
        Func<ArchiveCreatedEvent, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_options.ArchiveCreatedTopic);
        _logger.LogInformation(
            "Subscribed to topic {Topic} group {Group}",
            _options.ArchiveCreatedTopic, _options.ConsumerGroupId);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult;
                try
                {
                    consumeResult = _consumer.Consume(cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error");
                    continue;
                }

                if (consumeResult?.Message is null)
                {
                    continue;
                }

                try
                {
                    var evt = JsonSerializer.Deserialize<ArchiveCreatedEvent>(
                        consumeResult.Message.Value, JsonOptions);
                    if (evt is null)
                    {
                        throw new InvalidOperationException("Deserialized event was null");
                    }

                    await handler(evt, cancellationToken).ConfigureAwait(false);
                    _consumer.Commit(consumeResult);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to process message BatchId={Key}, sending to DLQ",
                        consumeResult.Message.Key);

                    await SendToDlqAsync(consumeResult, ex, cancellationToken).ConfigureAwait(false);
                    _consumer.Commit(consumeResult);
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private async Task SendToDlqAsync(
        ConsumeResult<string, string> failed,
        Exception error,
        CancellationToken cancellationToken)
    {
        var headers = new Headers();
        if (failed.Message.Headers is not null)
        {
            foreach (var header in failed.Message.Headers)
            {
                headers.Add(header.Key, header.GetValueBytes());
            }
        }
        headers.Add("x-original-topic", Encoding.UTF8.GetBytes(failed.Topic));
        headers.Add("x-original-partition", Encoding.UTF8.GetBytes(failed.Partition.Value.ToString()));
        headers.Add("x-original-offset", Encoding.UTF8.GetBytes(failed.Offset.Value.ToString()));
        headers.Add("x-error", Encoding.UTF8.GetBytes(error.Message));

        await _dlqProducer.ProduceAsync(_options.DeadLetterTopic, new Message<string, string>
        {
            Key = failed.Message.Key,
            Value = failed.Message.Value,
            Headers = headers
        }, cancellationToken).ConfigureAwait(false);
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
        _consumer.Dispose();
        _dlqProducer.Flush(TimeSpan.FromSeconds(5));
        _dlqProducer.Dispose();
    }
}
