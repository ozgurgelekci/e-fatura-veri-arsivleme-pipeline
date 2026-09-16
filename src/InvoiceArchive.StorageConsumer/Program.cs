using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Infrastructure;
using InvoiceArchive.StorageConsumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateBootstrapLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);

    builder.Services.AddSerilog((services, cfg) => cfg
        .ReadFrom.Configuration(builder.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext()
        .WriteTo.Console());

    builder.Services.AddOptions<ArchiveOptions>()
        .Bind(builder.Configuration.GetSection(ArchiveOptions.SectionName))
        .ValidateOnStart();

    builder.Services.AddInvoiceArchiveInfrastructure(builder.Configuration);
    builder.Services.AddHostedService<StorageStatusWorker>();

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "StorageConsumer terminated unexpectedly");
    throw;
}
finally
{
    Log.CloseAndFlush();
}
