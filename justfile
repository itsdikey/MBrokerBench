set shell := ["powershell.exe", "-NoProfile", "-Command"]

# MBrokerBench Phase 0 Automation

# Run the full setup
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

# Deploy Kafka KRaft using Strimzi
kafka-up:
	@echo "Deploying Kafka KRaft cluster..."
	kubectl apply -f https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/main/examples/kafka/kafka-single-node.yaml
	@echo "Waiting for Kafka to be ready (this may take a few minutes)..."
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
	--set server.service.type=ClusterIP

# Port Forwarding for UIs
forward-seq:
	@echo "Opening Seq on http://localhost:8080..."
	kubectl port-forward svc/seq 8080:80

forward-prom:
	@echo "Opening Prometheus on http://localhost:9090..."
	kubectl port-forward svc/prometheus-server 9090:80

forward-kafka:
	@echo "Forwarding Kafka bootstrap to localhost:9092..."
	kubectl port-forward svc/my-cluster-kafka-bootstrap 9092:9092

# Kafka Interaction
create-topic topic partitions="6":
	-kubectl delete pod kafka-admin --ignore-not-found=true
	kubectl run kafka-admin -ti -q --restart='Never' --image='quay.io/strimzi/kafka:0.41.0-kafka-3.7.0' --rm=true -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --create --topic {{topic}} --partitions {{partitions}} --replication-factor 1

list-topics:
	-kubectl delete pod kafka-admin --ignore-not-found=true
	kubectl run kafka-admin -ti -q --restart='Never' --image='quay.io/strimzi/kafka:0.41.0-kafka-3.7.0' --rm=true -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --list

produce topic:
	-kubectl delete pod kafka-producer --ignore-not-found=true
	kubectl run kafka-producer -ti --image='quay.io/strimzi/kafka:0.41.0-kafka-3.7.0' --rm=true --restart='Never' -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic {{topic}}

consume topic:
	-kubectl delete pod kafka-consumer --ignore-not-found=true
	kubectl run kafka-consumer -ti --image='quay.io/strimzi/kafka:0.41.0-kafka-3.7.0' --rm=true --restart='Never' -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic {{topic}} --from-beginning

# Utility to clean stuck pods
clean-pods:
	kubectl delete pod kafka-admin kafka-producer kafka-consumer --ignore-not-found=true

# Check dependencies
check:
	@if (-not (Get-Command k3d -ErrorAction SilentlyContinue)) { throw "k3d not found. Please install it: https://k3d.io" }
	@if (-not (Get-Command helm -ErrorAction SilentlyContinue)) { throw "helm not found. Please install it." }
	@if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) { throw "kubectl not found. Please install it." }
	@echo "All tools found."
