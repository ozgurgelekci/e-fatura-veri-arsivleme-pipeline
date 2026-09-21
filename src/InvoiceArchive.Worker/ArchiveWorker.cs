using System.Diagnostics;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Application.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Worker;

public sealed class ArchiveWorker : BackgroundService
{
    private readonly BatchProcessor _processor;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly IOptionsMonitor<ArchiveOptions> _options;
    private readonly ILogger<ArchiveWorker> _logger;

    public ArchiveWorker(
        BatchProcessor processor,
        IHostApplicationLifetime lifetime,
        IOptionsMonitor<ArchiveOptions> options,
        ILogger<ArchiveWorker> logger)
    {
        _processor = processor;
        _lifetime = lifetime;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = _options.CurrentValue;
        _logger.LogInformation("ArchiveWorker starting. RunOnceAndExit={RunOnce}", options.RunOnceAndExit);

        try
        {
            var stopwatch = Stopwatch.StartNew();
            var completed = await _processor.RunAsync(stoppingToken).ConfigureAwait(false);
            stopwatch.Stop();

            _logger.LogInformation(
                "ArchiveWorker run finished. BatchesCompleted={Count} DurationSeconds={Duration}",
                completed, stopwatch.Elapsed.TotalSeconds);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("ArchiveWorker stopping.");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "ArchiveWorker crashed.");
        }
        finally
        {
            if (options.RunOnceAndExit)
            {
                _lifetime.StopApplication();
            }
        }
    }
}
