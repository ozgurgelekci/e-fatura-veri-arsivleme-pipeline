using Confluent.Kafka;

namespace InvoiceArchive.DlqReplay;

public sealed class ReplayOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string DlqTopic { get; init; } = "invoice.archive.dlq";
    public string GroupId { get; init; } = "invoice-archive-dlq-replay";
    public string? TargetTopic { get; init; }
    public int? MaxMessages { get; init; }
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public bool FromBeginning { get; init; } = true;
    public bool DryRun { get; init; }
    public string? SecurityProtocol { get; init; }
    public string? SaslMechanism { get; init; }
    public string? SaslUsername { get; init; }
    public string? SaslPassword { get; init; }

    public void ApplySasl(ClientConfig config)
    {
        if (!string.IsNullOrEmpty(SecurityProtocol))
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(SecurityProtocol, ignoreCase: true);
        }
        if (!string.IsNullOrEmpty(SaslMechanism))
        {
            config.SaslMechanism = Enum.Parse<SaslMechanism>(SaslMechanism, ignoreCase: true);
        }
        if (!string.IsNullOrEmpty(SaslUsername))
        {
            config.SaslUsername = SaslUsername;
            config.SaslPassword = SaslPassword;
        }
    }

    public static ReplayOptions? Parse(string[] args)
    {
        string bootstrap = "localhost:9092";
        string dlq = "invoice.archive.dlq";
        string group = "invoice-archive-dlq-replay";
        string? target = null;
        int? max = null;
        var idle = TimeSpan.FromSeconds(10);
        var fromBeginning = true;
        var dryRun = false;
        string? securityProtocol = null;
        string? saslMechanism = null;
        string? saslUsername = null;
        string? saslPassword = null;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--bootstrap":
                case "-b":
                    if (++i >= args.Length) return null;
                    bootstrap = args[i];
                    break;
                case "--dlq":
                    if (++i >= args.Length) return null;
                    dlq = args[i];
                    break;
                case "--group":
                case "-g":
                    if (++i >= args.Length) return null;
                    group = args[i];
                    break;
                case "--to":
                    if (++i >= args.Length) return null;
                    target = args[i];
                    break;
                case "--max":
                    if (++i >= args.Length || !int.TryParse(args[i], out var m)) return null;
                    max = m;
                    break;
                case "--idle":
                    if (++i >= args.Length || !int.TryParse(args[i], out var s)) return null;
                    idle = TimeSpan.FromSeconds(s);
                    break;
                case "--from-latest":
                    fromBeginning = false;
                    break;
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--security-protocol":
                    if (++i >= args.Length) return null;
                    securityProtocol = args[i];
                    break;
                case "--sasl-mechanism":
                    if (++i >= args.Length) return null;
                    saslMechanism = args[i];
                    break;
                case "--sasl-username":
                    if (++i >= args.Length) return null;
                    saslUsername = args[i];
                    break;
                case "--sasl-password":
                    if (++i >= args.Length) return null;
                    saslPassword = args[i];
                    break;
                case "-h":
                case "--help":
                    return null;
                default:
                    Console.Error.WriteLine($"Unknown argument: {args[i]}");
                    return null;
            }
        }

        return new ReplayOptions
        {
            BootstrapServers = bootstrap,
            DlqTopic = dlq,
            GroupId = group,
            TargetTopic = target,
            MaxMessages = max,
            IdleTimeout = idle,
            FromBeginning = fromBeginning,
            DryRun = dryRun,
            SecurityProtocol = securityProtocol,
            SaslMechanism = saslMechanism,
            SaslUsername = saslUsername,
            SaslPassword = saslPassword
        };
    }

    public static void PrintUsage()
    {
        Console.Error.WriteLine(@"
InvoiceArchive.DlqReplay — replay messages from a Kafka DLQ topic back to the
original topic (or a caller-provided override).

Usage:
  dotnet run --project src/InvoiceArchive.DlqReplay -- [options]

Options:
  -b, --bootstrap <servers>   Kafka bootstrap servers (default: localhost:9092)
      --dlq <topic>           DLQ topic name (default: invoice.archive.dlq)
  -g, --group <id>            Consumer group id (default: invoice-archive-dlq-replay)
      --to <topic>            Override target topic; otherwise x-original-topic header is used
      --max <n>               Stop after N messages
      --idle <seconds>        Exit if no message arrives within N seconds (default: 10)
      --from-latest           Start from latest instead of beginning
      --dry-run               Log intended action without producing or committing
      --security-protocol <p> Kafka security protocol (e.g., SaslSsl)
      --sasl-mechanism <m>    e.g., Plain, ScramSha256
      --sasl-username <u>
      --sasl-password <p>
  -h, --help                  Show this help
");
    }
}
