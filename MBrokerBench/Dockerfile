FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy projects and restore dependencies
COPY ["MBrokerBench/MBrokerBench.csproj", "MBrokerBench/"]
COPY ["MSodaClient/MSodaClient.csproj", "MSodaClient/"]
RUN dotnet restore "MBrokerBench/MBrokerBench.csproj"

# Copy source code and build
COPY . .
WORKDIR "/src/MBrokerBench"
RUN dotnet build "MBrokerBench.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "MBrokerBench.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Use ASP.NET runtime because the controller hosts a Prometheus web server
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .

# Expose metrics port
EXPOSE 1234
ENV NO_TUI=true

ENTRYPOINT ["dotnet", "MBrokerBench.dll"]
