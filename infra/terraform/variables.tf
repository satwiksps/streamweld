variable "project_id" {
  description = "Google Cloud project ID in which to create the demo cluster."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid 6-30 character Google Cloud project ID."
  }
}

variable "region" {
  description = "Google Cloud region that contains the subnet and selected zone."
  type        = string
  default     = "us-central1"

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+$", var.region))
    error_message = "region must look like a Google Cloud region, for example us-central1."
  }
}

variable "zone" {
  description = "Single GKE zone. Confirm that the selected GPU type has quota and capacity here."
  type        = string
  default     = "us-central1-a"

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+-[a-z]$", var.zone))
    error_message = "zone must look like a Google Cloud zone, for example us-central1-a."
  }

  validation {
    condition     = startswith(var.zone, "${var.region}-")
    error_message = "zone must belong to region."
  }
}

variable "name" {
  description = "Short prefix used for the cluster and all supporting resources."
  type        = string
  default     = "streamweld"

  validation {
    condition     = length(var.name) >= 2 && length(var.name) <= 20 && can(regex("^[a-z][a-z0-9-]*[a-z0-9]$", var.name))
    error_message = "name must be 2-20 lowercase letters, digits, or hyphens, starting with a letter and ending with a letter or digit."
  }
}

variable "labels" {
  description = "Additional Google Cloud resource labels."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for key, value in var.labels :
      can(regex("^[a-z][a-z0-9_-]{0,62}$", key)) &&
      (value == "" || can(regex("^[a-z0-9][a-z0-9_-]{0,62}$", value)))
    ])
    error_message = "labels must use Google Cloud-compatible lowercase keys and values of at most 63 characters."
  }
}

variable "network_cidr" {
  description = "Primary IPv4 range for GKE nodes. Must not overlap the pod or service ranges."
  type        = string
  default     = "10.20.0.0/20"

  validation {
    condition     = can(cidrnetmask(var.network_cidr))
    error_message = "network_cidr must be a valid IPv4 CIDR."
  }
}

variable "pods_cidr" {
  description = "Secondary IPv4 range for VPC-native GKE pods."
  type        = string
  default     = "10.24.0.0/14"

  validation {
    condition     = can(cidrnetmask(var.pods_cidr))
    error_message = "pods_cidr must be a valid IPv4 CIDR."
  }
}

variable "services_cidr" {
  description = "Secondary IPv4 range for VPC-native GKE services."
  type        = string
  default     = "10.28.0.0/20"

  validation {
    condition     = can(cidrnetmask(var.services_cidr))
    error_message = "services_cidr must be a valid IPv4 CIDR."
  }
}

variable "system_machine_type" {
  description = "Machine type for the non-GPU system node pool."
  type        = string
  default     = "e2-standard-2"
}

variable "system_min_nodes" {
  description = "Minimum standard nodes reserved for GKE system workloads. Keep at least one."
  type        = number
  default     = 1

  validation {
    condition     = var.system_min_nodes >= 1 && floor(var.system_min_nodes) == var.system_min_nodes
    error_message = "system_min_nodes must be an integer of at least 1."
  }
}

variable "system_max_nodes" {
  description = "Maximum standard nodes in the system pool."
  type        = number
  default     = 2

  validation {
    condition     = var.system_max_nodes >= 1 && floor(var.system_max_nodes) == var.system_max_nodes
    error_message = "system_max_nodes must be a positive integer."
  }

  validation {
    condition     = var.system_max_nodes >= var.system_min_nodes
    error_message = "system_max_nodes must be greater than or equal to system_min_nodes."
  }
}

variable "gpu_type" {
  description = "Compute Engine accelerator type attached to both GPU pools."
  type        = string
  default     = "nvidia-tesla-t4"
}

variable "gpu_count" {
  description = "GPUs attached to each GPU node. Must match the selected machine family."
  type        = number
  default     = 1

  validation {
    condition     = var.gpu_count >= 1 && floor(var.gpu_count) == var.gpu_count
    error_message = "gpu_count must be a positive integer."
  }
}

variable "gpu_machine_type" {
  description = "Machine type for both GPU pools. The default supports one attached T4."
  type        = string
  default     = "n1-standard-4"
}

variable "gpu_driver_version" {
  description = "GKE-managed NVIDIA driver channel."
  type        = string
  default     = "DEFAULT"

  validation {
    condition     = contains(["DEFAULT", "LATEST"], var.gpu_driver_version)
    error_message = "gpu_driver_version must be DEFAULT or LATEST."
  }
}

variable "on_demand_gpu_min_nodes" {
  description = "Minimum on-demand GPU nodes. The default keeps the failure fallback immediately available."
  type        = number
  default     = 1

  validation {
    condition     = var.on_demand_gpu_min_nodes >= 0 && floor(var.on_demand_gpu_min_nodes) == var.on_demand_gpu_min_nodes
    error_message = "on_demand_gpu_min_nodes must be a non-negative integer."
  }
}

variable "on_demand_gpu_max_nodes" {
  description = "Maximum on-demand GPU nodes."
  type        = number
  default     = 2

  validation {
    condition     = var.on_demand_gpu_max_nodes >= 1 && floor(var.on_demand_gpu_max_nodes) == var.on_demand_gpu_max_nodes
    error_message = "on_demand_gpu_max_nodes must be a positive integer."
  }

  validation {
    condition     = var.on_demand_gpu_max_nodes >= var.on_demand_gpu_min_nodes
    error_message = "on_demand_gpu_max_nodes must be greater than or equal to on_demand_gpu_min_nodes."
  }
}

variable "spot_gpu_min_nodes" {
  description = "Minimum Spot GPU nodes. The default of one makes reclaim behavior real after apply."
  type        = number
  default     = 1

  validation {
    condition     = var.spot_gpu_min_nodes >= 0 && floor(var.spot_gpu_min_nodes) == var.spot_gpu_min_nodes
    error_message = "spot_gpu_min_nodes must be a non-negative integer."
  }
}

variable "spot_gpu_max_nodes" {
  description = "Maximum Spot GPU nodes."
  type        = number
  default     = 2

  validation {
    condition     = var.spot_gpu_max_nodes >= 1 && floor(var.spot_gpu_max_nodes) == var.spot_gpu_max_nodes
    error_message = "spot_gpu_max_nodes must be a positive integer."
  }

  validation {
    condition     = var.spot_gpu_max_nodes >= var.spot_gpu_min_nodes
    error_message = "spot_gpu_max_nodes must be greater than or equal to spot_gpu_min_nodes."
  }
}
