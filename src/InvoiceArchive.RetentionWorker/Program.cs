using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Infrastructure;
using InvoiceArchive.Infrastructure.Health;
using InvoiceArchive.RetentionWorker;
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

    builder.Services.AddOptions<RetentionOptions>()
        .Bind(builder.Configuration.GetSection(RetentionOptions.SectionName))
        .ValidateDataAnnotations()
        .ValidateOnStart();

    builder.Services.AddInvoiceArchiveInfrastructure(builder.Configuration);

    var healthPort = builder.Configuration.GetValue<int?>("Health:Port") ?? 8082;
    builder.Services.AddInvoiceArchiveHealthChecks(builder.Configuration, healthPort);

    builder.Services.AddHostedService<RetentionWorker>();

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "RetentionWorker terminated unexpectedly");
    throw;
}
finally
{
    Log.CloseAndFlush();
}
