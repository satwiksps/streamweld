output "cluster_name" {
  description = "GKE cluster name."
  value       = google_container_cluster.this.name
}

output "cluster_location" {
  description = "GKE cluster zone."
  value       = google_container_cluster.this.location
}

output "cluster_endpoint" {
  description = "GKE control-plane endpoint."
  value       = google_container_cluster.this.endpoint
  sensitive   = true
}

output "get_credentials_command" {
  description = "Command that writes this cluster context into the current kubeconfig."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --zone ${var.zone} --project ${var.project_id}"
}

output "gpu_node_pools" {
  description = "On-demand and Spot GPU pool topology."
  value = {
    on_demand = {
      name         = google_container_node_pool.gpu_on_demand.name
      spot         = google_container_node_pool.gpu_on_demand.node_config[0].spot
      machine_type = var.gpu_machine_type
      gpu_type     = var.gpu_type
      gpu_count    = var.gpu_count
      min_nodes    = var.on_demand_gpu_min_nodes
      max_nodes    = var.on_demand_gpu_max_nodes
    }
    spot = {
      name         = google_container_node_pool.gpu_spot.name
      spot         = google_container_node_pool.gpu_spot.node_config[0].spot
      machine_type = var.gpu_machine_type
      gpu_type     = var.gpu_type
      gpu_count    = var.gpu_count
      min_nodes    = var.spot_gpu_min_nodes
      max_nodes    = var.spot_gpu_max_nodes
    }
  }
}

output "network_name" {
  description = "Dedicated VPC network name."
  value       = google_compute_network.this.name
}

output "node_service_account_email" {
  description = "Service account attached to all persistent node pools."
  value       = google_service_account.nodes.email
}

output "spot_workload_scheduling" {
  description = "Node selector and toleration required to target the Spot pool explicitly."
  value = {
    node_selector = {
      "cloud.google.com/gke-spot" = "true"
    }
    toleration = {
      key      = "cloud.google.com/gke-spot"
      operator = "Equal"
      value    = "true"
      effect   = "NoSchedule"
    }
  }
}
