FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY InvoiceArchive.slnx ./
COPY src/InvoiceArchive.Domain/ src/InvoiceArchive.Domain/
COPY src/InvoiceArchive.Contracts/ src/InvoiceArchive.Contracts/
COPY src/InvoiceArchive.Application/ src/InvoiceArchive.Application/
COPY src/InvoiceArchive.Infrastructure/ src/InvoiceArchive.Infrastructure/
COPY src/InvoiceArchive.StorageConsumer/ src/InvoiceArchive.StorageConsumer/

RUN dotnet restore src/InvoiceArchive.StorageConsumer/InvoiceArchive.StorageConsumer.csproj
RUN dotnet publish src/InvoiceArchive.StorageConsumer/InvoiceArchive.StorageConsumer.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENV DOTNET_ENVIRONMENT=Production
ENTRYPOINT ["dotnet", "InvoiceArchive.StorageConsumer.dll"]
