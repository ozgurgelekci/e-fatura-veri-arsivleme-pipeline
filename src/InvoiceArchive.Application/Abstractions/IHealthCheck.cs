namespace InvoiceArchive.Application.Abstractions;

public interface IHealthCheck
{
    string Name { get; }

    Task<HealthCheckResult> CheckAsync(CancellationToken cancellationToken);
}

public sealed record HealthCheckResult(bool Healthy, string? Detail = null)
{
    public static HealthCheckResult Ok(string? detail = null) => new(true, detail);
    public static HealthCheckResult Fail(string detail) => new(false, detail);
}
