output "cluster_name" {
  description = "GKE cluster name."
  value       = module.streamweld_demo.cluster_name
}

output "gpu_node_pools" {
  description = "The example's on-demand and Spot GPU pools."
  value       = module.streamweld_demo.gpu_node_pools
}
