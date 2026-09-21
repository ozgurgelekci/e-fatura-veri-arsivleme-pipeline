using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Infrastructure;
using InvoiceArchive.Worker;
using InvoiceArchive.Worker.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext} {Message:lj} {Properties:j}{NewLine}{Exception}")
    .CreateBootstrapLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);

    builder.Services.AddSerilog((services, cfg) => cfg
        .ReadFrom.Configuration(builder.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext()
        .WriteTo.Console(outputTemplate:
            "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext} {Message:lj} {Properties:j}{NewLine}{Exception}"));

    builder.Services.AddOptions<ArchiveOptions>()
        .Bind(builder.Configuration.GetSection(ArchiveOptions.SectionName))
        .ValidateOnStart();

    builder.Services.AddInvoiceArchiveInfrastructure(builder.Configuration);
    builder.Services.Replace(ServiceDescriptor.Singleton<IArchiveMetrics, PrometheusArchiveMetrics>());

    builder.Services.AddHostedService<MetricServerHostedService>();
    builder.Services.AddHostedService<ArchiveWorker>();

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Worker terminated unexpectedly");
    throw;
}
finally
{
    Log.CloseAndFlush();
}
