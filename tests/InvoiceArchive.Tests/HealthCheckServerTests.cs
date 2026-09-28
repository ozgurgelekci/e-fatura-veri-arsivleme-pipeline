using System.Net;
using System.Text.Json;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Infrastructure.Health;
using Microsoft.Extensions.Logging.Abstractions;

namespace InvoiceArchive.Tests;

public class HealthCheckServerTests
{
    [Fact]
    public async Task Live_endpoint_returns_200_even_when_dependencies_unhealthy()
    {
        var checks = new IHealthCheck[] { new StubCheck("db", healthy: false, "boom") };
        await using var server = await StartServerAsync(checks);

        using var http = new HttpClient();
        var response = await http.GetAsync($"http://localhost:{server.Port}/health/live");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Ready_endpoint_returns_200_when_all_checks_healthy()
    {
        var checks = new IHealthCheck[]
        {
            new StubCheck("es", healthy: true),
            new StubCheck("kafka", healthy: true)
        };
        await using var server = await StartServerAsync(checks);

        using var http = new HttpClient();
        var response = await http.GetAsync($"http://localhost:{server.Port}/health/ready");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal("healthy", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("healthy", doc.RootElement.GetProperty("checks").GetProperty("es").GetProperty("status").GetString());
    }

    [Fact]
    public async Task Ready_endpoint_returns_503_when_any_check_fails()
    {
        var checks = new IHealthCheck[]
        {
            new StubCheck("es", healthy: true),
            new StubCheck("kafka", healthy: false, "connection refused")
        };
        await using var server = await StartServerAsync(checks);

        using var http = new HttpClient();
        var response = await http.GetAsync($"http://localhost:{server.Port}/health/ready");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal("unhealthy", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("connection refused",
            doc.RootElement.GetProperty("checks").GetProperty("kafka").GetProperty("detail").GetString());
    }

    private static async Task<RunningServer> StartServerAsync(IHealthCheck[] checks)
    {
        var port = GetFreePort();
        var options = new HealthCheckServerOptions { Host = "localhost", Port = port };
        var server = new HealthCheckServer(checks, options, NullLogger<HealthCheckServer>.Instance);
        await server.StartAsync(CancellationToken.None);
        return new RunningServer(server, port);
    }

    private static int GetFreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed record RunningServer(HealthCheckServer Server, int Port) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Server.StopAsync(CancellationToken.None);
            await Server.DisposeAsync();
        }
    }

    private sealed class StubCheck : IHealthCheck
    {
        private readonly bool _healthy;
        private readonly string? _detail;

        public StubCheck(string name, bool healthy, string? detail = null)
        {
            Name = name;
            _healthy = healthy;
            _detail = detail;
        }

        public string Name { get; }

        public Task<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
            => Task.FromResult(_healthy ? HealthCheckResult.Ok() : HealthCheckResult.Fail(_detail ?? "fail"));
    }
}
