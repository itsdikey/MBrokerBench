# MBrokerBench: Kafka Autoscaling Benchmark

MBrokerBench is a simulation and benchmarking tool for evaluating partition assignment and autoscaling strategies in heterogeneous Kafka consumer groups. It implements the **Cost-Centric Modified Worst-Fit (CC-MWF)** algorithm, which builds and adjusts a consumer fleet based on workload demand and instance cost. The simulator can run against a live Kafka cluster while a terminal dashboard shows system state in real time, and it supports comparison with other assignment strategies and the Kafka default assignors.

## Components

- `MBrokerBench/`: Core simulation and benchmarking engine with a terminal dashboard. Contains the assignment strategies (`Strategies/`), the data providers (`DataProviders/`), and the CC-MWF implementation.
- `MBrokerConsumer/`: Consumer agent service that consumes from Kafka and applies the assigned partition layout.
- `MSodaClient/`: Client library for SODA v3 data sources, referenced by the simulator.
- `MBrokerConsumer.Tests/`: Unit tests for the consumer agent (xUnit).
- `MBrokerBench.Phase2Tests/`: End-to-end test harness that exercises the scaling flow against a running environment.

## Prerequisites

- **.NET SDK**: The solution targets `net8.0`, and `MBrokerConsumer.Tests` targets `net10.0`. The .NET 10 SDK is required to build and test the full solution.
- **just**: Command runner used for the automation recipes in the `justfile`.
- **Docker Desktop** (running), **k3d**, **kubectl**, and **helm**: Required for the local Kafka environment used by the simulation mode.

The recipes in the `justfile` are configured for PowerShell. Run `just check` to verify that the required CLI tools are installed.

## Build and test

Build the whole solution:

```powershell
dotnet build MBrokerBench.sln
```

Run the unit tests:

```powershell
dotnet test MBrokerConsumer.Tests/MBrokerConsumer.Tests.csproj
```

## Local simulation

The recommended way to run a simulation is through the recipes in the `justfile`. They start a local Kafka stack (k3d, Strimzi, and an observability stack) and run the simulator against it.

1. Start the environment. This creates the `mbroker-dev` k3d cluster with three agents, installs the Strimzi Kafka operator, deploys a Kafka cluster with metrics, and installs Seq (logs) and Prometheus (metrics):

```powershell
just up
```

2. In a separate terminal, forward the Kafka bootstrap port to `localhost:9092`:

```powershell
just forward-kafka
```

3. Create a topic with multiple partitions (for example, 12):

```powershell
just create-topic test-1 12
```

4. Generate workload on the topic:

```powershell
just stress-topic test-1 1000000
```

5. In another terminal, run the simulator against the local Kafka cluster. This launches the terminal dashboard:

```powershell
just run-kafka-sim
```

The simulator connects to Kafka on `localhost:9092`, reads metrics from Prometheus on `localhost:9090`, and uses the topic `test-1` with consumer group `test-group`.

Observability dashboards:

- Seq (logs): `just forward-seq`, opens at http://localhost:8080
- Prometheus (metrics): `just forward-prom`, opens at http://localhost:9090

Tear down the environment:

```powershell
just down
```

## Terminal dashboard

- **Status window**: step count, total system lag, production and consumption rates, and total system cost.
- **Partitions window**: all Kafka partitions, their current lag, and production rate.
- **Consumers window**: virtual consumer state (Booting, Syncing, Running) and utilization.
- **Logs window**: algorithm decisions and system alerts.
- **Exit**: press `Q` to stop the simulation and close the dashboard.

## Public mirror

This repository is mirrored to a public repository by an automated workflow. The public mirror contains only the files listed in [PUBLIC_EXPORT.md](PUBLIC_EXPORT.md). The Kubernetes manifests used by some `justfile` recipes are not part of the mirror, so recipes such as `just up` require the full source tree. Direct commits to the public repository are overwritten by the next sync.

## License

MIT. See [LICENSE](LICENSE).
