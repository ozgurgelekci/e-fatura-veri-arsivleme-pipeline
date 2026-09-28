using System.Net;
using System.Text;
using System.Text.Json;
using InvoiceArchive.Application.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InvoiceArchive.Infrastructure.Health;

public sealed class HealthCheckServer : IHostedService, IAsyncDisposable
{
    private readonly IEnumerable<IHealthCheck> _checks;
    private readonly ILogger<HealthCheckServer> _logger;
    private readonly HealthCheckServerOptions _options;
    private HttpListener? _listener;
    private CancellationTokenSource? _stopCts;
    private Task? _loop;

    public HealthCheckServer(
        IEnumerable<IHealthCheck> checks,
        HealthCheckServerOptions options,
        ILogger<HealthCheckServer> logger)
    {
        _checks = checks;
        _options = options;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://{_options.Host}:{_options.Port}/");
        try
        {
            _listener.Start();
        }
        catch (HttpListenerException ex)
        {
            _logger.LogWarning(ex,
                "HealthCheckServer could not bind to {Host}:{Port}. Health endpoints will be unavailable.",
                _options.Host, _options.Port);
            _listener = null;
            return Task.CompletedTask;
        }

        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = Task.Run(() => AcceptLoopAsync(_stopCts.Token));
        _logger.LogInformation(
            "Health endpoints listening on http://+:{Port} ({Live}, {Ready})",
            _options.Port, _options.LivePath, _options.ReadyPath);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopCts?.Cancel();
        _listener?.Stop();
        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        if (_listener is null)
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync().ConfigureAwait(false);
            }
            catch (HttpListenerException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            _ = Task.Run(() => HandleAsync(ctx, cancellationToken), cancellationToken);
        }
    }

    private async Task HandleAsync(HttpListenerContext ctx, CancellationToken cancellationToken)
    {
        try
        {
            var path = ctx.Request.Url?.AbsolutePath ?? string.Empty;

            if (string.Equals(path, _options.LivePath, StringComparison.OrdinalIgnoreCase))
            {
                await WriteAsync(ctx, HttpStatusCode.OK,
                    new { status = "healthy" }).ConfigureAwait(false);
                return;
            }

            if (string.Equals(path, _options.ReadyPath, StringComparison.OrdinalIgnoreCase))
            {
                var results = new Dictionary<string, object>();
                var overall = true;

                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(_options.ProbeTimeout);

                foreach (var check in _checks)
                {
                    try
                    {
                        var r = await check.CheckAsync(timeoutCts.Token).ConfigureAwait(false);
                        overall &= r.Healthy;
                        results[check.Name] = r.Detail is null
                            ? (object)new { status = r.Healthy ? "healthy" : "unhealthy" }
                            : new { status = r.Healthy ? "healthy" : "unhealthy", detail = r.Detail };
                    }
                    catch (Exception ex)
                    {
                        overall = false;
                        results[check.Name] = new { status = "unhealthy", detail = ex.Message };
                    }
                }

                await WriteAsync(ctx,
                    overall ? HttpStatusCode.OK : HttpStatusCode.ServiceUnavailable,
                    new { status = overall ? "healthy" : "unhealthy", checks = results })
                    .ConfigureAwait(false);
                return;
            }

            ctx.Response.StatusCode = (int)HttpStatusCode.NotFound;
            ctx.Response.Close();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "HealthCheckServer request handling failed");
            try { ctx.Response.Abort(); } catch { }
        }
    }

    private static async Task WriteAsync(HttpListenerContext ctx, HttpStatusCode status, object payload)
    {
        ctx.Response.StatusCode = (int)status;
        ctx.Response.ContentType = "application/json";
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
        ctx.Response.ContentLength64 = bytes.Length;
        await ctx.Response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
        ctx.Response.OutputStream.Close();
    }

    public async ValueTask DisposeAsync()
    {
        _stopCts?.Cancel();
        _listener?.Close();
        if (_loop is not null)
        {
            try { await _loop.ConfigureAwait(false); } catch { }
        }
        _stopCts?.Dispose();
    }
}

public sealed class HealthCheckServerOptions
{
    public string Host { get; set; } = "+";
    public int Port { get; set; } = 8080;
    public string LivePath { get; set; } = "/health/live";
    public string ReadyPath { get; set; } = "/health/ready";
    public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
