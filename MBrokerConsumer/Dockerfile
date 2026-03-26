FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["MBrokerConsumer/MBrokerConsumer.csproj", "MBrokerConsumer/"]
RUN dotnet restore "MBrokerConsumer/MBrokerConsumer.csproj"
COPY . .
WORKDIR "/src/MBrokerConsumer"
RUN dotnet build "MBrokerConsumer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "MBrokerConsumer.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MBrokerConsumer.dll"]
