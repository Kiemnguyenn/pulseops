variable "network_name" {
  description = "Name of the shared Docker network used by the entire PulseOps stack"
  type        = string
  default     = "pulseops-net"
}

variable "volume_names" {
  description = "Map of logical name -> actual Docker volume name, matching the keys in docker-compose.yml"
  type        = map(string)
  default = {
    postgres_data  = "postgres_data"
    minio_data     = "minio_data"
    zookeeper_data = "zookeeper_data"
    kafka_data     = "kafka_data"
  }
}
