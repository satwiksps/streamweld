terraform {
  # Supply bucket and prefix at init time. Keeping this block empty prevents
  # backend locations and credentials from being committed with the module.
  backend "gcs" {}
}
