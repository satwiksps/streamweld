locals {
  common_labels = merge({
    application = "streamweld"
    environment = "demo"
    managed_by  = "terraform"
  }, var.labels)

  node_oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
}

resource "google_compute_network" "this" {
  project                 = var.project_id
  name                    = "${var.name}-vpc"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "this" {
  project       = var.project_id
  name          = "${var.name}-gke"
  region        = var.region
  network       = google_compute_network.this.id
  ip_cidr_range = var.network_cidr

  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "${var.name}-pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "${var.name}-services"
    ip_cidr_range = var.services_cidr
  }
}

resource "google_service_account" "nodes" {
  project      = var.project_id
  account_id   = "${var.name}-gke-nodes"
  display_name = "${var.name} GKE node service account"
  description  = "Least-privilege node identity for the Streamweld GKE demo cluster."
}

resource "google_project_iam_member" "nodes" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = "serviceAccount:${google_service_account.nodes.email}"
}

resource "google_container_cluster" "this" {
  project  = var.project_id
  name     = "${var.name}-demo"
  location = var.zone

  network    = google_compute_network.this.id
  subnetwork = google_compute_subnetwork.this.id

  deletion_protection      = false
  remove_default_node_pool = true
  initial_node_count       = 1

  enable_shielded_nodes = true
  resource_labels       = local.common_labels

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "${var.name}-pods"
    services_secondary_range_name = "${var.name}-services"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # This config is used only by the temporary default pool that GKE creates and
  # removes during cluster creation. All lasting pools are separate resources.
  node_config {
    service_account = google_service_account.nodes.email
    oauth_scopes    = local.node_oauth_scopes

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  depends_on = [google_project_iam_member.nodes]
}

resource "google_container_node_pool" "system" {
  project  = var.project_id
  name     = "${var.name}-system"
  location = var.zone
  cluster  = google_container_cluster.this.name

  initial_node_count = var.system_min_nodes
  deletion_policy    = "DELETE"

  autoscaling {
    total_min_node_count = var.system_min_nodes
    total_max_node_count = var.system_max_nodes
    location_policy      = "BALANCED"
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  node_config {
    machine_type = var.system_machine_type
    image_type   = "COS_CONTAINERD"
    disk_type    = "pd-balanced"
    disk_size_gb = 50

    service_account = google_service_account.nodes.email
    oauth_scopes    = local.node_oauth_scopes

    labels = {
      "streamweld.io/role"          = "system"
      "streamweld.io/capacity-type" = "on-demand"
    }

    resource_labels = local.common_labels

    metadata = {
      disable-legacy-endpoints = "true"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  lifecycle {
    # Autoscaling changes the observed initial size. Do not replace the pool to
    # reconcile that creation-only field on a later plan.
    ignore_changes = [initial_node_count]

    precondition {
      condition     = var.system_max_nodes >= var.system_min_nodes
      error_message = "system_max_nodes must be greater than or equal to system_min_nodes."
    }
  }
}

resource "google_container_node_pool" "gpu_on_demand" {
  project  = var.project_id
  name     = "${var.name}-gpu"
  location = var.zone
  cluster  = google_container_cluster.this.name

  initial_node_count = var.on_demand_gpu_min_nodes
  deletion_policy    = "DELETE"

  autoscaling {
    total_min_node_count = var.on_demand_gpu_min_nodes
    total_max_node_count = var.on_demand_gpu_max_nodes
    location_policy      = "BALANCED"
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  upgrade_settings {
    # Avoid requiring an extra scarce GPU during an upgrade. The other GPU pool
    # remains available while this single-node demo pool is replaced.
    max_surge       = 0
    max_unavailable = 1
  }

  node_config {
    spot         = false
    machine_type = var.gpu_machine_type
    image_type   = "COS_CONTAINERD"
    disk_type    = "pd-balanced"
    disk_size_gb = 100

    service_account = google_service_account.nodes.email
    oauth_scopes    = local.node_oauth_scopes

    guest_accelerator {
      type  = var.gpu_type
      count = var.gpu_count

      gpu_driver_installation_config {
        gpu_driver_version = var.gpu_driver_version
      }
    }

    labels = {
      "streamweld.io/role"          = "inference"
      "streamweld.io/capacity-type" = "on-demand"
    }

    resource_labels = local.common_labels

    metadata = {
      disable-legacy-endpoints = "true"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  lifecycle {
    # Autoscaling changes the observed initial size. Do not replace the pool to
    # reconcile that creation-only field on a later plan.
    ignore_changes = [initial_node_count]

    precondition {
      condition     = var.on_demand_gpu_max_nodes >= var.on_demand_gpu_min_nodes
      error_message = "on_demand_gpu_max_nodes must be greater than or equal to on_demand_gpu_min_nodes."
    }
  }
}

resource "google_container_node_pool" "gpu_spot" {
  project  = var.project_id
  name     = "${var.name}-gpu-spot"
  location = var.zone
  cluster  = google_container_cluster.this.name

  initial_node_count = var.spot_gpu_min_nodes
  deletion_policy    = "DELETE"

  autoscaling {
    total_min_node_count = var.spot_gpu_min_nodes
    total_max_node_count = var.spot_gpu_max_nodes
    location_policy      = "ANY"
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  upgrade_settings {
    # Avoid requiring a second Spot GPU allocation during an upgrade.
    max_surge       = 0
    max_unavailable = 1
  }

  node_config {
    spot         = true
    machine_type = var.gpu_machine_type
    image_type   = "COS_CONTAINERD"
    disk_type    = "pd-balanced"
    disk_size_gb = 100

    service_account = google_service_account.nodes.email
    oauth_scopes    = local.node_oauth_scopes

    guest_accelerator {
      type  = var.gpu_type
      count = var.gpu_count

      gpu_driver_installation_config {
        gpu_driver_version = var.gpu_driver_version
      }
    }

    labels = {
      "streamweld.io/role"          = "inference"
      "streamweld.io/capacity-type" = "spot"
    }

    taint {
      key    = "cloud.google.com/gke-spot"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    resource_labels = local.common_labels

    metadata = {
      disable-legacy-endpoints = "true"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    kubelet_config {
      shutdown_grace_period_seconds               = 60
      shutdown_grace_period_critical_pods_seconds = 20
    }
  }

  lifecycle {
    # Autoscaling changes the observed initial size. Do not replace the pool to
    # reconcile that creation-only field on a later plan.
    ignore_changes = [initial_node_count]

    precondition {
      condition     = var.spot_gpu_max_nodes >= var.spot_gpu_min_nodes
      error_message = "spot_gpu_max_nodes must be greater than or equal to spot_gpu_min_nodes."
    }
  }
}
