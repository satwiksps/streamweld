module "demo" {
  source = "./modules/gke-gpu-cluster"

  project_id = var.project_id
  region     = var.region
  zone       = var.zone
  name       = var.name
  labels     = var.labels

  network_cidr  = var.network_cidr
  pods_cidr     = var.pods_cidr
  services_cidr = var.services_cidr

  system_machine_type = var.system_machine_type
  system_min_nodes    = var.system_min_nodes
  system_max_nodes    = var.system_max_nodes

  gpu_type                = var.gpu_type
  gpu_count               = var.gpu_count
  gpu_machine_type        = var.gpu_machine_type
  gpu_driver_version      = var.gpu_driver_version
  on_demand_gpu_min_nodes = var.on_demand_gpu_min_nodes
  on_demand_gpu_max_nodes = var.on_demand_gpu_max_nodes
  spot_gpu_min_nodes      = var.spot_gpu_min_nodes
  spot_gpu_max_nodes      = var.spot_gpu_max_nodes
}
