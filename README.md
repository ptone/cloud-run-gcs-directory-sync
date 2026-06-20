# Google Cloud Run GCS Directory Sync Sidecar

A lightweight, robust, and highly optimized Google Cloud Run sidecar container that synchronizes an ephemeral disk shared volume (`emptyDir`) with Google Cloud Storage (GCS). This is a prototype of a persistent filesystem with full POSIX compliance that delivers the exact same performance characteristics as a local disk (since it reads/writes directly to local ephemeral `/data` mounts).

> [!WARNING]
> * **Strict Single-Instance Scaling (Max Scale = 1)**: The Cloud Run resource attaching to this disk **MUST** be configured with a maximum of **1 active instance** (the service can scale between `0` and `1` instance; `maxScale: "1"`). This system is designed as a single-writer/single-reader environment and is **not** engineered for multi-reader or multi-writer operations.
> * **Best-Effort Synchronization (No Guaranteed Persistence)**: Data persistence is **not** absolutely guaranteed. Synchronization runs on a **best-effort basis**. Edge cases (such as ungraceful container crashes, synchronization API failures, or network splits) can occur, leading to instances where local files are not synced up to Google Cloud Storage.

This sidecar implements a highly responsive **bidirectional and event-driven** synchronization pattern designed for stateless containers that need access to persistent, shared file systems (e.g., file assets, static sites, or simple file databases) with high-speed local ephemeral disk reads/writes:
1. **On Startup**: It downloads all files from a specified GCS bucket/prefix into the shared directory. It blocks the main application's startup until the initial download is 100% complete.
2. **On File System Changes (Real-time Watcher)**: It uses an event-driven `fsnotify` file system watcher to track file creation, modification, deletion, and renaming events in real time, executing a debounced upload sync within 2 seconds of filesystem inactivity.
3. **Periodically (Configurable)**: It periodically scans the local shared directory as a robust fallback and uploads new or modified files to GCS. It uses file size and MD5 hash comparisons to perform high-performance **delta uploads**, avoiding redundant uploads and minimizing GCS egress costs and API write fees.
4. **On Shutdown (SIGTERM)**: It traps termination signals sent by Cloud Run, pauses active ticker runs, and executes a final, comprehensive upload sync before gracefully exiting.

---

## Architecture Overview

```
                      +---------------------------------------+
                      |          Cloud Run Instance           |
                      |                                       |
  [ HTTP Traffic ] ------> [ main-app ]                       |
                      |        |                              |
                      |        | (Reads/Writes)               |
                      |        v                              |
                      |   +===============================+   |
                      |   |  Shared Ephemeral Disk Vol    |   |
                      |   |  (/data, ephemeral emptyDir)  |   |
                      |   +===============================+   |
                      |        ^                              |
                      |        | (Syncs Dir)                  |
                      |        v                              |
                      |   [ gcs-sidecar ]                     |
                      |        |                              |
                      +--------|------------------------------+
                               |
                               | (HTTPS API)
                               v
                     +--------------------+
                     |    Google Cloud    |
                     |      Storage       |
                     +--------------------+
```

---

## Configuration Reference

The sidecar is configured entirely via environment variables. Configure these under the `gcs-sidecar` container spec inside your `service.yaml`.

| Environment Variable | Description | Default | Required? |
|----------------------|-------------|---------|-----------|
| `GCS_BUCKET` | The GCS bucket name to sync files with. | — | **Yes** |
| `GCS_PREFIX` | Prefix (folder) in GCS under which files are stored. | `shared-data/` | No |
| `SHARED_DIR` | The local mount path of the shared volume. | `/data` | No |
| `SYNC_INTERVAL` | Frequency of periodic upload checks. Parseable as Go duration (e.g., `10s`, `1m`, `5m`). | `1m` | No |
| `READY_PORT` | Port where readiness and liveness HTTP probes are exposed. | `8080` | No |

---

## Quick Start & Deployment Guide

### 1. Set Environment Variables

Define your deployment parameters in your terminal. These variables will be used for both building the container image and generating the deployment manifest:

```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export REPOSITORY="your-artifact-repo"
export GCS_BUCKET="your-gcs-bucket-name"
```

### 2. Build and Push the Sidecar Image

Compile, package, and push the sidecar image using Google Cloud Build (which uses our multi-stage `Dockerfile` and runs as the non-root `sidecar-user`):

```bash
gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/gcs-sidecar:latest .
```

### 3. Build and Push the Main Application Image (Example)

This step builds and pushes the provided Node.js web dashboard as the `main-app`. In a real deployment, you should replace the `example/` directory with your own application directory. Your custom application is expected to read from and write data to the `/data` directory (which is mapped to the shared ephemeral volume).

```bash
gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/main-app:latest example/
```

### 4. Deploy to Cloud Run

1. Render the final `service.yaml` deployment manifest from the template (using `sed` which is built-in on macOS and Linux):
   ```bash
   sed -e "s/\${PROJECT_ID}/${PROJECT_ID}/g" \
       -e "s/\${REGION}/${REGION}/g" \
       -e "s/\${REPOSITORY}/${REPOSITORY}/g" \
       -e "s/\${GCS_BUCKET}/${GCS_BUCKET}/g" \
       service.template.yaml > service.yaml
   ```

2. Apply and deploy the generated configuration using `gcloud beta`:
   ```bash
    gcloud beta run services replace service.yaml
    ```

---

## IAM Permissions & Security

For the sidecar to operate, the service account assigned to the Cloud Run service must have permissions to read, write, and delete objects in your target GCS bucket.

1. **Recommended Role**: Assign the **Storage Object Admin** (`roles/storage.objectAdmin`) or **Storage Object User** (`roles/storage.objectUser`) role to the Cloud Run service account on the specific bucket. 
2. **Deletion Permission**: Since the sidecar defaults to deleting GCS objects that have been removed locally, ensure the service account has the `storage.objects.delete` permission (which is included in the roles above).

---

## Key Design Optimizations

- **Real-Time Event-Driven Sync**: Uses `fsnotify` to listen for directory writes, creation, and deletions, and automatically registers watches recursively on subdirectories. File modifications are debounced by 2 seconds of filesystem inactivity to prevent redundant GCS writes during active burst periods, ensuring instant and resource-efficient synchronization.
- **Highly Efficient Diffing**: Instead of calling GCS metadata endpoints for *every* local file recursively (which causes excessive HTTP overhead and is slow), the sidecar listings are loaded into a memory cache map once at the beginning of each sync cycle. Local file sizes and MD5 hashes are compared against the cache map to only write modified files.
- **Atomic File Writing**: File writes to the local file system on download are done cleanly. Directories are resolved dynamically.
- **Safe Signal Handling**: Once SIGTERM is received, the sidecar suspends its periodic execution ticker, shuts down incoming probe servers, and executes a final upload sync. Cloud Run holds the container alive during this period up to the configured container timeout, ensuring complete data replication on instance termination.

---

## Container Startup Ordering

Cloud Run multi-container services support specifying startup dependencies using the `run.googleapis.com/container-dependencies` annotation. 

The sidecar exposes two endpoints on its `READY_PORT`:
* `/healthz`: Always returns `200 OK` (liveness probe).
* `/ready`: Returns `503 Service Unavailable` on startup. Once the initial download from GCS completes successfully, it transitions to returning `200 OK` (readiness probe).

By defining a `startupProbe` pointing to `/ready` on the sidecar and specifying that `main-app` depends on `gcs-sidecar` (see `service.yaml`), Cloud Run guarantees that **the sidecar will download all files from GCS before the main application starts running**.
