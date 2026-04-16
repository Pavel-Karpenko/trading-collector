# Build stage
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /source

# Copy solution and project files first for layer caching
COPY TradingCollector.sln .
COPY src/TradingCollector.Core/TradingCollector.Core.csproj                       src/TradingCollector.Core/
COPY src/TradingCollector.Application/TradingCollector.Application.csproj         src/TradingCollector.Application/
COPY src/TradingCollector.Infrastructure/TradingCollector.Infrastructure.csproj   src/TradingCollector.Infrastructure/
COPY src/TradingCollector.Host/TradingCollector.Host.csproj                       src/TradingCollector.Host/
COPY tests/TradingCollector.Tests/TradingCollector.Tests.csproj                   tests/TradingCollector.Tests/

RUN dotnet restore

# Copy remaining sources
COPY . .

RUN dotnet publish src/TradingCollector.Host/TradingCollector.Host.csproj \
    -c Release -o /app

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS runtime
WORKDIR /app

COPY --from=build /app .

ENTRYPOINT ["dotnet", "TradingCollector.Host.dll"]
