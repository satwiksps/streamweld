output "cluster_name" {
  description = "GKE cluster name."
  value       = module.demo.cluster_name
}

output "cluster_location" {
  description = "GKE cluster zone."
  value       = module.demo.cluster_location
}

output "cluster_endpoint" {
  description = "GKE control-plane endpoint."
  value       = module.demo.cluster_endpoint
  sensitive   = true
}

output "get_credentials_command" {
  description = "Command that writes this cluster context into the current kubeconfig."
  value       = module.demo.get_credentials_command
}

output "gpu_node_pools" {
  description = "On-demand and Spot GPU pool topology."
  value       = module.demo.gpu_node_pools
}

output "network_name" {
  description = "Dedicated VPC network name."
  value       = module.demo.network_name
}

output "node_service_account_email" {
  description = "Least-privilege service account attached to all GKE nodes."
  value       = module.demo.node_service_account_email
}

output "spot_workload_scheduling" {
  description = "Node selector and toleration required to target the Spot pool explicitly."
  value       = module.demo.spot_workload_scheduling
}
