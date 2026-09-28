-- Schema for InvoiceArchive.Infrastructure.Persistence.PostgresArchiveBatchRepository.
-- The repository also runs this DDL on first use when Persistence:AutoMigrate=true;
-- this file is provided for out-of-band migrations (CI, DBA-managed provisioning).

CREATE SCHEMA IF NOT EXISTS "public";

CREATE TABLE IF NOT EXISTS "public"."archive_batches" (
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

CREATE INDEX IF NOT EXISTS ix_archive_batches_status
    ON "public"."archive_batches" (status);

CREATE INDEX IF NOT EXISTS ix_archive_batches_tenant_created
    ON "public"."archive_batches" (tenant_id, created_at DESC);
