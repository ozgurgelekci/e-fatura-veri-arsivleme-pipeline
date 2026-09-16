using InvoiceArchive.Application.Services;

namespace InvoiceArchive.Tests;

public class DefaultStorageKeyBuilderTests
{
    [Fact]
    public void Uses_yyyy_mm_dd_layout_without_tenant()
    {
        var builder = new DefaultStorageKeyBuilder();
        var key = builder.Build(new DateTime(2026, 8, 11, 10, 30, 0, DateTimeKind.Utc), "20260811-000001", null);
        Assert.Equal("2026/08/11/batch-20260811-000001.zip", key);
    }

    [Fact]
    public void Prefixes_tenant_when_provided()
    {
        var builder = new DefaultStorageKeyBuilder();
        var key = builder.Build(new DateTime(2026, 8, 11, 10, 30, 0, DateTimeKind.Utc), "20260811-000001", "tenant-42");
        Assert.Equal("tenant-42/2026/08/11/batch-20260811-000001.zip", key);
    }
}
