terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

# Shared bridge network for all PulseOps services
resource "docker_network" "pulseops_net" {
  name = var.network_name
}

# Postgres data directory (core banking / CDC source database)
resource "docker_volume" "postgres_data" {
  name = var.volume_names["postgres_data"]
}

# MinIO data lake storage
resource "docker_volume" "minio_data" {
  name = var.volume_names["minio_data"]
}

# Zookeeper state storage
resource "docker_volume" "zookeeper_data" {
  name = var.volume_names["zookeeper_data"]
}

# Kafka broker log storage
resource "docker_volume" "kafka_data" {
  name = var.volume_names["kafka_data"]
}
