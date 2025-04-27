terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}
provider "google" {
  credentials = "../docker/mage/google-cred.json"
  project     = "arctic-surf-456413-f0"

}

resource "google_storage_bucket" "gcp-storage" {
  name          = "swe5003"
  location = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }

    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "bigquery-dataset" {
  dataset_id = var.bq_dataset_name
  location = var.location
}
