# Streamweld GKE GPU demo

This Terraform root deploys a zonal GKE Standard cluster for Streamweld's real Spot-reclaim scenario. The reusable module lives in `modules/gke-gpu-cluster`; `examples/basic` shows a direct module call.

The default topology is deliberately explicit:

| Pool | Capacity | Default size | Purpose |
|---|---|---:|---|
| `streamweld-system` | standard CPU | 1-2 | reliable DNS, metrics, and cluster services |
| `streamweld-gpu` | on-demand T4 | 1-2 | stable inference fallback |
| `streamweld-gpu-spot` | Spot T4 | 1-2 | interruptible inference target for real reclaim tests |

GKE recommends keeping standard non-GPU capacity for system workloads when using Spot GPU nodes. Both GPU pools use separate `google_container_node_pool` resources, Container-Optimized OS, GKE-managed NVIDIA drivers, autoscaling, auto-repair, and auto-upgrade.

This is a disposable demo topology, not a production security baseline. It uses GKE's public control-plane endpoint and public node networking to avoid a NAT dependency; IAM still protects cluster authentication. A production deployment should evaluate private nodes, control-plane authorized networks, organization policy, and a regional control plane separately.

## Prerequisites

- Terraform `>= 1.9, < 2.0`
- A Google Cloud project with billing enabled
- `container.googleapis.com`, `compute.googleapis.com`, and `iam.googleapis.com` enabled
- Application Default Credentials with permission to create GKE, Compute Engine, service-account, and project IAM resources
- GPU quota for two `nvidia-tesla-t4` accelerators in `us-central1-a`, or matching overrides for another available zone and accelerator
- A pre-existing versioned GCS bucket when using remote state

The module does not enable project APIs or create GPU quota. Those are project-level prerequisites and are intentionally outside the destroy boundary.

## Remote state

The root has an empty `backend "gcs"` block. Copy the example and initialize with a partial backend configuration; do not place credentials in the file.

```sh
cp backend.gcs.hcl.example backend.gcs.hcl
terraform init -backend-config=backend.gcs.hcl
```

The GCS bucket is external to this stack, remains after destroy, and should have Object Versioning enabled. Terraform uses Application Default Credentials for the backend when no credential path is supplied.

For local validation without contacting a backend:

```sh
terraform init -backend=false
```

## Deploy

```sh
cp terraform.tfvars.example terraform.tfvars
# Edit project_id and confirm zone quota/capacity.
terraform fmt -check -recursive
terraform init -backend-config=backend.gcs.hcl
terraform validate
terraform plan -out=streamweld.tfplan
terraform apply streamweld.tfplan
terraform output -raw get_credentials_command
```

Run the printed `gcloud container clusters get-credentials` command, then verify the topology:

```sh
kubectl get nodes -L cloud.google.com/gke-nodepool,cloud.google.com/gke-spot
kubectl apply -f examples/spot-workload.yaml
kubectl get pod spot-gpu-proof -o wide
kubectl logs spot-gpu-proof
```

The example Pod uses both the `cloud.google.com/gke-spot: "true"` selector and matching taint toleration, requests one GPU, and runs `nvidia-smi`. It proves that the workload reached Spot GPU hardware; reclaim timing remains controlled by Google Cloud rather than Terraform.

## Cost note

The defaults start three billable worker VMs: one `e2-standard-2` system node, one on-demand `n1-standard-4` node with a T4, and one Spot `n1-standard-4` node with a T4. Also account for the GKE management fee, persistent disks, network egress, logging/monitoring ingestion, and the remote-state bucket. Spot capacity is discounted but can be reclaimed at any time and its price or availability is not guaranteed.

No fixed total is quoted because Google Cloud prices, free-tier eligibility, taxes, and Spot rates vary by account and location. Before apply, use the [Google Cloud Pricing Calculator](https://cloud.google.com/products/calculator) with the exact variable values and review [GKE pricing](https://cloud.google.com/kubernetes-engine/pricing), [Compute Engine pricing](https://cloud.google.com/compute/all-pricing), and [Spot VM pricing](https://cloud.google.com/spot-vms/pricing).

For a cheaper idle environment, set both GPU minimums to zero. That reduces idle GPU spend but means the Spot-reclaim scenario is not immediately runnable and scale-up depends on capacity.

## Destroy and verify cleanup

The GKE cluster sets `deletion_protection = false`; every node pool sets `deletion_policy = "DELETE"`. The dedicated VPC, subnet, node service account, and its project IAM membership are all in the same state and are removed by:

```sh
terraform plan -destroy -out=destroy.tfplan
terraform apply destroy.tfplan
terraform state list
```

The final command should print no managed resources. The pre-existing GCS state bucket and project APIs remain because this configuration did not create them. If an apply is interrupted, rerun `terraform destroy`; do not delete state while resources remain.

## Inputs and outputs

Run `terraform providers schema -json` or inspect `variables.tf` for the full input contract. The most important knobs are the region/zone, GPU type and machine type, GPU count, and min/max sizes for each pool. Outputs include the credentials command, cluster identity, VPC name, node service account, complete GPU pool topology, and the Spot scheduling selector/toleration.

## Validation

The checked-in topology tests use Terraform's mock provider, so they require no Google Cloud credentials:

```sh
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
terraform test
tflint --init --config=.tflint.hcl
tflint --recursive

terraform -chdir=examples/basic init -backend=false
terraform -chdir=examples/basic validate
```

Validation checks provider schema and module wiring but cannot prove regional GPU quota or live Spot capacity. Always inspect `terraform plan` against the target project before apply.

## Design references

- [Run GPUs in GKE Standard node pools](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus)
- [GKE Spot VMs](https://cloud.google.com/kubernetes-engine/docs/concepts/spot-vms)
- [`google_container_cluster`](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster)
- [`google_container_node_pool`](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_node_pool)
- [Terraform GCS backend](https://developer.hashicorp.com/terraform/language/backend/gcs)
- [Terraform partial backend configuration](https://developer.hashicorp.com/terraform/language/backend#partial-configuration)
