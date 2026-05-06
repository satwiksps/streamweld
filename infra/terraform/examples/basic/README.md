# Basic example

This example calls the reusable `gke-gpu-cluster` module directly with the same cost-conscious topology as the root deployment: one small standard system pool, one on-demand T4 pool, and one Spot T4 pool.

```sh
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
terraform destroy
```

The example intentionally uses local state. The deployable root configuration demonstrates a partially configured GCS backend.
