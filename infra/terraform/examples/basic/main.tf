module "streamweld_demo" {
  source = "../../modules/gke-gpu-cluster"

  project_id = var.project_id
  region     = "us-central1"
  zone       = "us-central1-a"
  name       = "streamweld"

  network_cidr  = "10.20.0.0/20"
  pods_cidr     = "10.24.0.0/14"
  services_cidr = "10.28.0.0/20"

  system_machine_type = "e2-standard-2"
  system_min_nodes    = 1
  system_max_nodes    = 2

  gpu_type                = "nvidia-tesla-t4"
  gpu_count               = 1
  gpu_machine_type        = "n1-standard-4"
  gpu_driver_version      = "DEFAULT"
  on_demand_gpu_min_nodes = 1
  on_demand_gpu_max_nodes = 2
  spot_gpu_min_nodes      = 1
  spot_gpu_max_nodes      = 2
}
