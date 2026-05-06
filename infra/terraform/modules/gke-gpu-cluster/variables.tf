variable "project_id" {
  description = "Google Cloud project ID."
  type        = string
}

variable "region" {
  description = "Region for the VPC subnet."
  type        = string
}

variable "zone" {
  description = "Zone for the GKE control plane and GPU nodes."
  type        = string

  validation {
    condition     = startswith(var.zone, "${var.region}-")
    error_message = "zone must belong to region."
  }
}

variable "name" {
  description = "Short resource-name prefix."
  type        = string
}

variable "labels" {
  description = "Additional Google Cloud resource labels."
  type        = map(string)
  default     = {}
}

variable "network_cidr" {
  description = "Primary subnet range for GKE nodes."
  type        = string
}

variable "pods_cidr" {
  description = "Secondary subnet range for VPC-native pods."
  type        = string
}

variable "services_cidr" {
  description = "Secondary subnet range for VPC-native services."
  type        = string
}

variable "system_machine_type" {
  description = "Machine type for the reliable non-GPU system pool."
  type        = string
}

variable "system_min_nodes" {
  description = "Minimum system-pool nodes."
  type        = number
}

variable "system_max_nodes" {
  description = "Maximum system-pool nodes."
  type        = number

  validation {
    condition     = var.system_max_nodes >= var.system_min_nodes
    error_message = "system_max_nodes must be greater than or equal to system_min_nodes."
  }
}

variable "gpu_type" {
  description = "Accelerator type for both GPU pools."
  type        = string
}

variable "gpu_count" {
  description = "Accelerator count per GPU node."
  type        = number
}

variable "gpu_machine_type" {
  description = "Machine type for both GPU pools."
  type        = string
}

variable "gpu_driver_version" {
  description = "GKE-managed NVIDIA driver channel."
  type        = string
}

variable "on_demand_gpu_min_nodes" {
  description = "Minimum on-demand GPU nodes."
  type        = number
}

variable "on_demand_gpu_max_nodes" {
  description = "Maximum on-demand GPU nodes."
  type        = number

  validation {
    condition     = var.on_demand_gpu_max_nodes >= var.on_demand_gpu_min_nodes
    error_message = "on_demand_gpu_max_nodes must be greater than or equal to on_demand_gpu_min_nodes."
  }
}

variable "spot_gpu_min_nodes" {
  description = "Minimum Spot GPU nodes."
  type        = number
}

variable "spot_gpu_max_nodes" {
  description = "Maximum Spot GPU nodes."
  type        = number

  validation {
    condition     = var.spot_gpu_max_nodes >= var.spot_gpu_min_nodes
    error_message = "spot_gpu_max_nodes must be greater than or equal to spot_gpu_min_nodes."
  }
}
