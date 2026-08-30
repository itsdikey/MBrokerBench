set shell := ["powershell.exe", "-NoProfile", "-Command"]

# MBrokerBench Phase 0 Automation
STRIMZI_IMAGE := "quay.io/strimzi/kafka:0.50.1-kafka-4.1.1"

# Build the thesis PDF and refresh its bibliography
build-pdf:
	Set-Location FCUP_thesis; xelatex -interaction=nonstopmode -file-line-error main.tex; bibtex main; xelatex -interaction=nonstopmode -file-line-error main.tex; xelatex -interaction=nonstopmode -file-line-error main.tex

# Run the full setup from scratch
up: cluster-up strimzi-up kafka-up obs-up

# Create k3d cluster with 3 agents
cluster-up:
	@echo "Creating k3d cluster 'mbroker-dev'..."
	k3d cluster create mbroker-dev --agents 3 --port "8080:80@loadbalancer" --port "9092:9092@loadbalancer"

# Delete k3d cluster
down:
	@echo "Deleting k3d cluster 'mbroker-dev'..."
	k3d cluster delete mbroker-dev

# Install Strimzi Operator
strimzi-up:
	@echo "Installing Strimzi Operator..."
	helm repo add strimzi https://strimzi.io/charts/
	helm repo update
	helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator

# Deploy Kafka with Metrics (Clean Stack)
kafka-up:
	@echo "Deploying Kafka Stack (ConfigMap + Cluster)..."
	kubectl apply -f k8s/init-stack.yaml
	@echo "Waiting for Kafka to be ready..."
	kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s

# Deploy Observability (Seq and Prometheus)
obs-up:
	@echo "Deploying Seq..."
	helm repo add datalust https://helm.datalust.co
	helm repo update
	helm upgrade --install seq datalust/seq \
	--set acceptEula=Y \
	--set service.type=ClusterIP \
	--set firstRunAdminPassword=StrongPassword123!

	@echo "Deploying Prometheus..."
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	helm upgrade --install prometheus prometheus-community/prometheus \
	--set server.service.type=NodePort \
	-f k8s/prometheus-kafka-scrape.yaml

# Port Forwarding for UIs
forward-seq:
	@echo "Opening Seq on http://localhost:8080..."
	kubectl port-forward svc/seq 8080:80

forward-prom:
	@echo "Opening Prometheus on http://localhost:9090..."
	kubectl port-forward svc/prometheus-server 9090:80

forward-kafka:
	@echo "Forwarding Kafka bootstrap to localhost:9092..."
	kubectl port-forward svc/my-cluster-kafka-external-bootstrap 9092:9092

# Kafka Interaction
create-topic topic partitions="6":
	-kubectl delete pod kafka-admin --ignore-not-found=true
	kubectl run kafka-admin -ti -q --restart='Never' --image={{STRIMZI_IMAGE}} --rm=true -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap:9093 --create --topic {{topic}} --partitions {{partitions}} --replication-factor 1

delete-topic topic:
	-kubectl delete pod kafka-admin --ignore-not-found=true
	kubectl run kafka-admin -ti -q --restart='Never' --image={{STRIMZI_IMAGE}} --rm=true -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap:9093 --delete --topic {{topic}}

list-topics:
	-kubectl delete pod kafka-admin --ignore-not-found=true
	kubectl run kafka-admin -ti -q --restart='Never' --image={{STRIMZI_IMAGE}} --rm=true -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap:9093 --list

produce topic:
	-kubectl delete pod kafka-producer --ignore-not-found=true
	kubectl run kafka-producer -ti --image={{STRIMZI_IMAGE}} --rm=true --restart='Never' -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9093 --topic {{topic}}

consume topic:
	-kubectl delete pod kafka-consumer --ignore-not-found=true
	kubectl run kafka-consumer -ti --image={{STRIMZI_IMAGE}} --rm=true --restart='Never' -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9093 --topic {{topic}} --from-beginning

# Stress test a topic with random data to simulate production rate
stress-topic topic messages="1000000" throughput="5000":
	-kubectl delete pod kafka-stress --ignore-not-found=true
	kubectl run kafka-stress -ti --image={{STRIMZI_IMAGE}} --rm=true --restart='Never' -- /opt/kafka/bin/kafka-producer-perf-test.sh --topic {{topic}} --num-records {{messages}} --record-size 100 --throughput {{throughput}} --producer-props bootstrap.servers=my-cluster-kafka-bootstrap:9093

# Run Phase 1 Simulation (Real Metrics, Virtual Consumers)
run-kafka-sim:
	$env:DATA_PROVIDER="Kafka"; \
	$env:KAFKA_BOOTSTRAP="localhost:9092"; \
	$env:PROMETHEUS_URL="http://localhost:9090"; \
	$env:KAFKA_TOPIC="test-1"; \
	$env:KAFKA_GROUP="test-group"; \
	dotnet run --project MBrokerBench/MBrokerBench.csproj

# ============================================================
# Phase 2: Real Infrastructure Deployments
# ============================================================
# Deploy MBrokerBench Controller with RBAC (ServiceAccount, Role, RoleBinding)
deploy-controller:
	@echo "Deploying MBrokerBench Controller with RBAC..."
	kubectl apply -f k8s/mbroker-deployment.yaml
	@echo "Waiting for controller to be ready..."
	kubectl rollout status deployment/mbrokerbench --timeout=120s

# Deploy consumer Deployments (small + large, scaled to 0 by default)
deploy-consumers:
	@echo "Deploying consumer ConfigMap..."
	kubectl apply -f k8s/mbroker-consumer-config.yaml
	@echo "Deploying consumer Deployments (small + large)..."
	kubectl apply -f k8s/consumer-deployments.yaml
	@echo "Removing stale legacy assignment env vars from consumer Deployments..."
	kubectl set env deployment/mbroker-consumer-small MANUAL_PARTITION_ASSIGNMENT_ENABLED- ASSIGNMENT_CONFIG_MAP_NAME- ASSIGNMENT_POLL_INTERVAL_SECONDS- PARTITION_ASSIGNMENT_STRATEGY- KAFKA_ASSIGNOR-
	kubectl set env deployment/mbroker-consumer-medium MANUAL_PARTITION_ASSIGNMENT_ENABLED- ASSIGNMENT_CONFIG_MAP_NAME- ASSIGNMENT_POLL_INTERVAL_SECONDS- PARTITION_ASSIGNMENT_STRATEGY- KAFKA_ASSIGNOR-
	kubectl set env deployment/mbroker-consumer-large MANUAL_PARTITION_ASSIGNMENT_ENABLED- ASSIGNMENT_CONFIG_MAP_NAME- ASSIGNMENT_POLL_INTERVAL_SECONDS- PARTITION_ASSIGNMENT_STRATEGY- KAFKA_ASSIGNOR-

# Full Phase 2 stack deploy (run after Phase 0 is up)
phase2-up: deploy-controller deploy-consumers
	@echo "Phase 2 stack deployed. Consumers start at 0 replicas."
	@echo "MBrokerBench will scale them up/down based on Kafka lag."

# Deploy all Phase 0 + Phase 2 infrastructure
full-up: up deploy-controller deploy-consumers

# Scale all consumer Deployments to 0
scale-down-consumers:
	kubectl scale deployment/mbroker-consumer-small --replicas=0
	kubectl scale deployment/mbroker-consumer-large --replicas=0

# Watch consumer pod status
watch-consumers:
	kubectl get pods -l app=mbroker-consumer -w

# Watch controller logs
logs-controller:
	kubectl logs -l app=mbrokerbench --tail=100 -f
# Phase 2: Real Infrastructure Transition

# Build the Controller Image
build-controller:
	docker build -t mbrokerbench:latest -f MBrokerBench/Dockerfile .

# Load the image into k3d
load-controller:
	k3d image import mbrokerbench:latest -c mbroker-dev

# Build the Consumer Agent Image
build-consumer:
	docker build -t mbroker-consumer:latest -f MBrokerConsumer/Dockerfile .

# Load the image into k3d
load-consumer:
	k3d image import mbroker-consumer:latest -c mbroker-dev

# Deploy Real Infrastructure (Consumers + RBAC)
deploy-infra:
	kubectl apply -f k8s/controller-rbac.yaml
	kubectl apply -f k8s/consumer-deployments.yaml

# Run Phase 2 Controller (Real Scaling Mode)
# This requires port-forwarding for Kafka and K8s context to be set correctly
run-phase2:
	$env:DATA_PROVIDER="Kafka"; \
	$env:SCALING_MODE="Real"; \
	$env:KAFKA_BOOTSTRAP="localhost:9092"; \
	$env:PROMETHEUS_URL="http://localhost:9090"; \
	$env:KAFKA_TOPIC="test-1"; \
	$env:KAFKA_GROUP="test-group"; \
	dotnet run --project MBrokerBench/MBrokerBench.csproj

# Run Phase 2 E2E automated test harness (C#)
phase2-test *args:
	dotnet run --project MBrokerBench.Phase2Tests/MBrokerBench.Phase2Tests.csproj -- {{args}}

# Utility to clean stuck pods
clean-pods:
	kubectl delete pod kafka-admin kafka-producer kafka-consumer kafka-stress --ignore-not-found=true

# Check dependencies
check:
	@if (-not (Get-Command k3d -ErrorAction SilentlyContinue)) { throw "k3d not found. Please install it: https://k3d.io" }
	@if (-not (Get-Command helm -ErrorAction SilentlyContinue)) { throw "helm not found. Please install it." }
	@if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) { throw "kubectl not found. Please install it." }
	@echo "All tools found."
