using InvoiceArchive.DlqReplay;

namespace InvoiceArchive.Tests;

public class ReplayOptionsTests
{
    [Fact]
    public void Parses_defaults_from_empty_args()
    {
        var opts = ReplayOptions.Parse(Array.Empty<string>());
        Assert.NotNull(opts);
        Assert.Equal("localhost:9092", opts!.BootstrapServers);
        Assert.Equal("invoice.archive.dlq", opts.DlqTopic);
        Assert.Equal("invoice-archive-dlq-replay", opts.GroupId);
        Assert.Null(opts.TargetTopic);
        Assert.Null(opts.MaxMessages);
        Assert.True(opts.FromBeginning);
        Assert.False(opts.DryRun);
    }

    [Fact]
    public void Parses_all_common_flags()
    {
        var opts = ReplayOptions.Parse(new[]
        {
            "-b", "broker:9092",
            "--dlq", "custom.dlq",
            "-g", "replay-1",
            "--to", "invoice.archive.created",
            "--max", "5",
            "--idle", "3",
            "--from-latest",
            "--dry-run"
        });

        Assert.NotNull(opts);
        Assert.Equal("broker:9092", opts!.BootstrapServers);
        Assert.Equal("custom.dlq", opts.DlqTopic);
        Assert.Equal("replay-1", opts.GroupId);
        Assert.Equal("invoice.archive.created", opts.TargetTopic);
        Assert.Equal(5, opts.MaxMessages);
        Assert.Equal(TimeSpan.FromSeconds(3), opts.IdleTimeout);
        Assert.False(opts.FromBeginning);
        Assert.True(opts.DryRun);
    }

    [Fact]
    public void Returns_null_on_unknown_flag()
    {
        var opts = ReplayOptions.Parse(new[] { "--nope" });
        Assert.Null(opts);
    }

    [Fact]
    public void Returns_null_on_missing_value_for_flag()
    {
        var opts = ReplayOptions.Parse(new[] { "--bootstrap" });
        Assert.Null(opts);
    }

    [Fact]
    public void Returns_null_on_non_integer_max()
    {
        var opts = ReplayOptions.Parse(new[] { "--max", "abc" });
        Assert.Null(opts);
    }
}
