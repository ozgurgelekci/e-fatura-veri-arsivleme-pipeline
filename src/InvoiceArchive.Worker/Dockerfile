FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY InvoiceArchive.slnx ./
COPY src/InvoiceArchive.Domain/ src/InvoiceArchive.Domain/
COPY src/InvoiceArchive.Contracts/ src/InvoiceArchive.Contracts/
COPY src/InvoiceArchive.Application/ src/InvoiceArchive.Application/
COPY src/InvoiceArchive.Infrastructure/ src/InvoiceArchive.Infrastructure/
COPY src/InvoiceArchive.Worker/ src/InvoiceArchive.Worker/

RUN dotnet restore src/InvoiceArchive.Worker/InvoiceArchive.Worker.csproj
RUN dotnet publish src/InvoiceArchive.Worker/InvoiceArchive.Worker.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENV DOTNET_ENVIRONMENT=Production
EXPOSE 9464
ENTRYPOINT ["dotnet", "InvoiceArchive.Worker.dll"]
