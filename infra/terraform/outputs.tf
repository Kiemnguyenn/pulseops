output "network_id" {
  description = "ID of the Docker network created by Terraform"
  value       = docker_network.pulseops_net.id
}

output "network_name" {
  description = "Name of the Docker network created by Terraform"
  value       = docker_network.pulseops_net.name
}

output "volume_names" {
  description = "Names of all Docker volumes provisioned by Terraform"
  value = {
    postgres_data  = docker_volume.postgres_data.name
    minio_data     = docker_volume.minio_data.name
    zookeeper_data = docker_volume.zookeeper_data.name
    kafka_data     = docker_volume.kafka_data.name
  }
}
