# MBrokerBench: Kafka Autoscaling Benchmark

`MBrokerBench` is a simulation and benchmarking tool designed to evaluate partition assignment and autoscaling strategies for heterogeneous Kafka consumer groups. It features the **Cost-Centric Modified Worst-Fit (CC-MWF)** algorithm, which optimizes fleet composition based on workload and instance costs.

## Prerequisites
- **Docker Desktop** (Running)
- **k3d**, **kubectl**, **helm**
- **.NET 8.0 SDK**
- **just** (Command runner)

## Quick Start: Phase 1 (Hybrid Simulation)

Follow these steps to run the simulation against a real local Kafka cluster with virtual consumers.

### 1. Initialize the Environment
Spin up the k3d cluster, Kafka (via Strimzi), and the observability stack (Seq/Prometheus):
```powershell
just up
```

### 2. Prepare Kafka Connectivity
In a **separate terminal**, start the port-forwarder to allow the local simulator to communicate with the Kubernetes-hosted Kafka:
```powershell
just forward-kafka
```

### 3. Create the Benchmarking Topic
Create a topic with multiple partitions (e.g., 12) to test the assignment strategy:
```powershell
just create-topic test-1 12
```

### 4. Generate Workload
Start a producer stress test to generate a continuous flow of messages:
```powershell
just stress-topic test-1 1000000
```

### 5. Run the Simulation
In a **third terminal**, run the simulator in "Kafka" mode. This will launch the TUI Dashboard:
```powershell
just run-kafka-sim
```

## TUI Dashboard Controls
- **Status Window:** Shows real-time step count, total system lag, production/consumption rates, and total system cost.
- **Partitions Window:** Lists all Kafka partitions, their current lag, and production rate.
- **Consumers Window:** Tracks virtual consumer state (Booting, Syncing, Running) and their utilization.
- **Logs Window:** Displays algorithm decisions and system alerts.
- **Exit:** Press **'Q'** to stop the simulation and close the dashboard.

## Project Structure
- `/MBrokerBench`: Core simulation engine and TUI logic.
- `/Strategies`: Implementation of assignment algorithms (CC-MWF, Worst-Fit, etc.).
- `/DataProviders`: Logic for fetching data from static configs, mathematical models, or real Kafka clusters.
- `/k8s`: Kubernetes manifests for Kafka and observability.
- `justfile`: Automation for cluster management and testing.

## Observability
While the TUI provides immediate feedback, you can also view deep metrics:
- **Seq (Logs):** `just forward-seq` -> [http://localhost:8080](http://localhost:8080)
- **Prometheus (Metrics):** `just forward-prom` -> [http://localhost:9090](http://localhost:9090)
