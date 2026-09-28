using System.Data;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace InvoiceArchive.Infrastructure.Persistence;

public sealed class PostgresArchiveBatchRepository : IArchiveBatchRepository
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PersistenceOptions _options;
    private readonly ILogger<PostgresArchiveBatchRepository> _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public PostgresArchiveBatchRepository(
        NpgsqlDataSource dataSource,
        IOptions<PersistenceOptions> options,
        ILogger<PostgresArchiveBatchRepository> logger)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
    }

    public async Task SaveAsync(ArchiveBatch batch, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
INSERT INTO {QualifiedTable} (
    batch_id, tenant_id, invoice_count, size_in_bytes, file_name, storage_path,
    sha256, first_invoice_id, last_invoice_id, status, retry_count, error_message,
    created_at, completed_at
) VALUES (
    @batch_id, @tenant_id, @invoice_count, @size_in_bytes, @file_name, @storage_path,
    @sha256, @first_invoice_id, @last_invoice_id, @status, @retry_count, @error_message,
    @created_at, @completed_at
)
ON CONFLICT (batch_id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    invoice_count = EXCLUDED.invoice_count,
    size_in_bytes = EXCLUDED.size_in_bytes,
    file_name = EXCLUDED.file_name,
    storage_path = EXCLUDED.storage_path,
    sha256 = EXCLUDED.sha256,
    first_invoice_id = EXCLUDED.first_invoice_id,
    last_invoice_id = EXCLUDED.last_invoice_id,
    status = EXCLUDED.status,
    retry_count = EXCLUDED.retry_count,
    error_message = EXCLUDED.error_message,
    completed_at = EXCLUDED.completed_at;";

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter("batch_id", NpgsqlDbType.Text) { Value = batch.BatchId });
        cmd.Parameters.Add(new NpgsqlParameter("tenant_id", NpgsqlDbType.Text) { Value = (object?)batch.TenantId ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("invoice_count", NpgsqlDbType.Integer) { Value = batch.InvoiceCount });
        cmd.Parameters.Add(new NpgsqlParameter("size_in_bytes", NpgsqlDbType.Bigint) { Value = batch.SizeInBytes });
        cmd.Parameters.Add(new NpgsqlParameter("file_name", NpgsqlDbType.Text) { Value = (object?)batch.FileName ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("storage_path", NpgsqlDbType.Text) { Value = (object?)batch.StoragePath ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("sha256", NpgsqlDbType.Text) { Value = (object?)batch.Sha256 ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("first_invoice_id", NpgsqlDbType.Text) { Value = (object?)batch.FirstInvoiceId ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("last_invoice_id", NpgsqlDbType.Text) { Value = (object?)batch.LastInvoiceId ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("status", NpgsqlDbType.Smallint) { Value = (short)batch.Status });
        cmd.Parameters.Add(new NpgsqlParameter("retry_count", NpgsqlDbType.Integer) { Value = batch.RetryCount });
        cmd.Parameters.Add(new NpgsqlParameter("error_message", NpgsqlDbType.Text) { Value = (object?)batch.ErrorMessage ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter("created_at", NpgsqlDbType.TimestampTz) { Value = DateTime.SpecifyKind(batch.CreatedAt, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter("completed_at", NpgsqlDbType.TimestampTz)
        {
            Value = batch.CompletedAt.HasValue
                ? DateTime.SpecifyKind(batch.CompletedAt.Value, DateTimeKind.Utc)
                : DBNull.Value
        });

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<ArchiveBatch?> FindAsync(string batchId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
SELECT batch_id, tenant_id, invoice_count, size_in_bytes, file_name, storage_path,
       sha256, first_invoice_id, last_invoice_id, status, retry_count, error_message,
       created_at, completed_at
FROM {QualifiedTable}
WHERE batch_id = @batch_id;";

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter("batch_id", NpgsqlDbType.Text) { Value = batchId });

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return new ArchiveBatch
        {
            BatchId = reader.GetString(0),
            TenantId = reader.IsDBNull(1) ? null : reader.GetString(1),
            InvoiceCount = reader.GetInt32(2),
            SizeInBytes = reader.GetInt64(3),
            FileName = reader.IsDBNull(4) ? default! : reader.GetString(4),
            StoragePath = reader.IsDBNull(5) ? default! : reader.GetString(5),
            Sha256 = reader.IsDBNull(6) ? null : reader.GetString(6),
            FirstInvoiceId = reader.IsDBNull(7) ? null : reader.GetString(7),
            LastInvoiceId = reader.IsDBNull(8) ? null : reader.GetString(8),
            Status = (ArchiveStatus)reader.GetInt16(9),
            RetryCount = reader.GetInt32(10),
            ErrorMessage = reader.IsDBNull(11) ? null : reader.GetString(11),
            CreatedAt = DateTime.SpecifyKind(reader.GetDateTime(12), DateTimeKind.Utc),
            CompletedAt = reader.IsDBNull(13) ? null : DateTime.SpecifyKind(reader.GetDateTime(13), DateTimeKind.Utc)
        };
    }

    private string QualifiedTable => $"\"{_options.SchemaName}\".\"{_options.TableName}\"";

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized || !_options.AutoMigrate)
        {
            _initialized = true;
            return;
        }

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return;
            }

            var ddl = $@"
CREATE SCHEMA IF NOT EXISTS ""{_options.SchemaName}"";
CREATE TABLE IF NOT EXISTS {QualifiedTable} (
    batch_id          TEXT PRIMARY KEY,
    tenant_id         TEXT NULL,
    invoice_count     INTEGER NOT NULL DEFAULT 0,
    size_in_bytes     BIGINT NOT NULL DEFAULT 0,
    file_name         TEXT NULL,
    storage_path      TEXT NULL,
    sha256            TEXT NULL,
    first_invoice_id  TEXT NULL,
    last_invoice_id   TEXT NULL,
    status            SMALLINT NOT NULL DEFAULT 0,
    retry_count       INTEGER NOT NULL DEFAULT 0,
    error_message     TEXT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    completed_at      TIMESTAMPTZ NULL
);
CREATE INDEX IF NOT EXISTS ix_{_options.TableName}_status
    ON {QualifiedTable} (status);
CREATE INDEX IF NOT EXISTS ix_{_options.TableName}_tenant_created
    ON {QualifiedTable} (tenant_id, created_at DESC);";

            await using var cmd = _dataSource.CreateCommand(ddl);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _initialized = true;
            _logger.LogInformation(
                "Postgres archive_batches schema ensured: {Schema}.{Table}",
                _options.SchemaName, _options.TableName);
        }
        finally
        {
            _initLock.Release();
        }
    }
}
