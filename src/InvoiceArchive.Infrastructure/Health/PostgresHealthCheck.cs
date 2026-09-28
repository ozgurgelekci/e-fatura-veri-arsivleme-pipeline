using InvoiceArchive.Application.Abstractions;
using Npgsql;

namespace InvoiceArchive.Infrastructure.Health;

public sealed class PostgresHealthCheck : IHealthCheck
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresHealthCheck(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public string Name => "postgres";

    public async Task<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var cmd = _dataSource.CreateCommand("SELECT 1");
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is int i && i == 1
                ? HealthCheckResult.Ok()
                : HealthCheckResult.Fail("Unexpected scalar result");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Fail(ex.Message);
        }
    }
}
