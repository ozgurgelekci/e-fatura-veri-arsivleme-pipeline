using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace InvoiceArchive.Worker;

public sealed class MetricServerHostedService : IHostedService
{
    private readonly ILogger<MetricServerHostedService> _logger;
    private MetricServer? _server;
    private readonly int _port;

    public MetricServerHostedService(ILogger<MetricServerHostedService> logger, int port = 9464)
    {
        _logger = logger;
        _port = port;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            _server = new MetricServer(port: _port);
            _server.Start();
            _logger.LogInformation("Prometheus metrics endpoint listening on http://+:{Port}/metrics", _port);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to start Prometheus MetricServer on port {Port}", _port);
        }
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _server?.Stop();
        return Task.CompletedTask;
    }
}
