mock_provider "google" {}

run "default_topology" {
  command = plan

  variables {
    project_id = "streamweld-test-project"
  }

  assert {
    condition     = output.cluster_name == "streamweld-demo"
    error_message = "The default cluster name changed unexpectedly."
  }

  assert {
    condition     = output.gpu_node_pools.on_demand.spot == false
    error_message = "The fallback GPU pool must use on-demand capacity."
  }

  assert {
    condition     = output.gpu_node_pools.spot.spot == true
    error_message = "The failure-injection GPU pool must use real Spot VMs."
  }

  assert {
    condition     = output.gpu_node_pools.on_demand.min_nodes == 1 && output.gpu_node_pools.spot.min_nodes == 1
    error_message = "The default topology must start one node in each GPU pool."
  }

  assert {
    condition     = output.spot_workload_scheduling.node_selector["cloud.google.com/gke-spot"] == "true"
    error_message = "The module must expose deterministic Spot workload scheduling."
  }
}

run "rejects_mismatched_region_and_zone" {
  command = plan

  variables {
    project_id = "streamweld-test-project"
    region     = "us-central1"
    zone       = "europe-west1-b"
  }

  expect_failures = [var.zone]
}

run "rejects_inverted_spot_bounds" {
  command = plan

  variables {
    project_id         = "streamweld-test-project"
    spot_gpu_min_nodes = 2
    spot_gpu_max_nodes = 1
  }

  expect_failures = [var.spot_gpu_max_nodes]
}

run "module_resource_contract" {
  command = plan

  module {
    source = "./modules/gke-gpu-cluster"
  }

  variables {
    project_id = "streamweld-test-project"
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

  assert {
    condition     = google_container_cluster.this.deletion_protection == false
    error_message = "The demo cluster must remain destroyable by Terraform."
  }

  assert {
    condition     = google_container_node_pool.gpu_on_demand.deletion_policy == "DELETE" && google_container_node_pool.gpu_spot.deletion_policy == "DELETE"
    error_message = "Both GPU pools must delete their underlying instances during destroy."
  }

  assert {
    condition     = google_container_node_pool.gpu_on_demand.node_config[0].spot == false
    error_message = "The fallback GPU pool must use standard capacity."
  }

  assert {
    condition     = google_container_node_pool.gpu_spot.node_config[0].spot == true
    error_message = "The failure-injection GPU pool must create real Spot VMs."
  }

  assert {
    condition     = google_container_node_pool.gpu_spot.node_config[0].guest_accelerator[0].type == "nvidia-tesla-t4" && google_container_node_pool.gpu_spot.node_config[0].guest_accelerator[0].count == 1
    error_message = "The Spot pool must attach the configured GPU hardware."
  }

  assert {
    condition     = google_container_node_pool.gpu_spot.node_config[0].taint[0].key == "cloud.google.com/gke-spot" && google_container_node_pool.gpu_spot.node_config[0].taint[0].effect == "NO_SCHEDULE"
    error_message = "The Spot pool must reject workloads that do not opt into interruption risk."
  }
}
