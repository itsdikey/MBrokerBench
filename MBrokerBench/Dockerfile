# Base runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app

# Expose metrics port
EXPOSE 1234

# Build image
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

# Copy csproj and restore
COPY ["MBrokerBench.csproj", "./"]

# Copy everything else
COPY . .

COPY simulation_config.txt ./simulation_config.json

RUN dotnet restore MBrokerBench.csproj

# Publish
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish MBrokerBench.csproj -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# Final runtime image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MBrokerBench.dll"]
