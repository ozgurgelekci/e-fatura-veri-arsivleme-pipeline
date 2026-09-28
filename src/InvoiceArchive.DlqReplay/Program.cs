using System.Text;
using Confluent.Kafka;
using InvoiceArchive.DlqReplay;

var opts = ReplayOptions.Parse(args);
if (opts is null)
{
    ReplayOptions.PrintUsage();
    return 2;
}

Console.WriteLine(
    $"[dlq-replay] bootstrap={opts.BootstrapServers} dlq={opts.DlqTopic} " +
    $"group={opts.GroupId} target={opts.TargetTopic ?? "<header:x-original-topic>"} " +
    $"max={opts.MaxMessages?.ToString() ?? "∞"} idle={opts.IdleTimeout}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("[dlq-replay] cancellation requested, draining…");
};

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = opts.BootstrapServers,
    GroupId = opts.GroupId,
    EnableAutoCommit = false,
    AutoOffsetReset = opts.FromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
    ClientId = "invoice-archive-dlq-replay"
};
opts.ApplySasl(consumerConfig);

var producerConfig = new ProducerConfig
{
    BootstrapServers = opts.BootstrapServers,
    ClientId = "invoice-archive-dlq-replay-producer",
    EnableIdempotence = true,
    Acks = Acks.All
};
opts.ApplySasl(producerConfig);

using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
    .SetErrorHandler((_, e) => Console.Error.WriteLine($"[dlq-replay] consumer error: {e.Reason}"))
    .Build();
using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

consumer.Subscribe(opts.DlqTopic);

var replayed = 0;
var skipped = 0;
try
{
    while (!cts.IsCancellationRequested)
    {
        ConsumeResult<string, string>? result;
        try
        {
            result = consumer.Consume(opts.IdleTimeout);
        }
        catch (ConsumeException ex)
        {
            Console.Error.WriteLine($"[dlq-replay] consume error: {ex.Error.Reason}");
            continue;
        }

        if (result is null || result.Message is null)
        {
            Console.WriteLine($"[dlq-replay] no messages within {opts.IdleTimeout}, exiting.");
            break;
        }

        var target = opts.TargetTopic ?? ReadHeader(result.Message.Headers, "x-original-topic");
        if (string.IsNullOrEmpty(target))
        {
            Console.Error.WriteLine(
                $"[dlq-replay] skipping offset {result.Offset.Value}: no target topic (no --to and no x-original-topic header)");
            skipped++;
            if (!opts.DryRun)
            {
                consumer.Commit(result);
            }
            continue;
        }

        var headers = new Headers();
        if (result.Message.Headers is not null)
        {
            foreach (var h in result.Message.Headers)
            {
                if (h.Key.StartsWith("x-original-", StringComparison.OrdinalIgnoreCase)
                    || h.Key.Equals("x-error", StringComparison.OrdinalIgnoreCase)
                    || h.Key.Equals("x-error-message", StringComparison.OrdinalIgnoreCase)
                    || h.Key.Equals("x-failed-at", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                headers.Add(h.Key, h.GetValueBytes());
            }
        }
        headers.Add("x-replayed-from", Encoding.UTF8.GetBytes(opts.DlqTopic));
        headers.Add("x-replayed-at", Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")));

        if (opts.DryRun)
        {
            Console.WriteLine(
                $"[dlq-replay] DRY-RUN would republish offset={result.Offset.Value} key={result.Message.Key} → {target}");
        }
        else
        {
            var delivery = await producer.ProduceAsync(target, new Message<string, string>
            {
                Key = result.Message.Key,
                Value = result.Message.Value,
                Headers = headers
            }, cts.Token);

            consumer.Commit(result);
            Console.WriteLine(
                $"[dlq-replay] replayed offset={result.Offset.Value} key={result.Message.Key} → {target} partition={delivery.Partition.Value} offset={delivery.Offset.Value}");
        }

        replayed++;
        if (opts.MaxMessages is int max && replayed >= max)
        {
            Console.WriteLine($"[dlq-replay] reached --max {max}, exiting.");
            break;
        }
    }
}
catch (OperationCanceledException)
{
    // graceful shutdown
}
finally
{
    consumer.Close();
    producer.Flush(TimeSpan.FromSeconds(5));
}

Console.WriteLine($"[dlq-replay] done. replayed={replayed} skipped={skipped}");
return 0;

static string? ReadHeader(Headers? headers, string key)
{
    if (headers is null)
    {
        return null;
    }
    foreach (var h in headers)
    {
        if (string.Equals(h.Key, key, StringComparison.OrdinalIgnoreCase))
        {
            return Encoding.UTF8.GetString(h.GetValueBytes());
        }
    }
    return null;
}
