// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsnotify/fsnotify"
)

func main() {
	log.Println("Initializing Cloud Run GCS Sidecar...")

	// 1. Load configuration from environment variables
	bucketName := os.Getenv("GCS_BUCKET")
	if bucketName == "" {
		log.Fatal("FATAL: GCS_BUCKET environment variable is required")
	}

	gcsPrefix := os.Getenv("GCS_PREFIX")
	if gcsPrefix == "" {
		gcsPrefix = "shared-data/"
	}

	sharedDir := os.Getenv("SHARED_DIR")
	if sharedDir == "" {
		sharedDir = os.Getenv("DIR")
	}
	if sharedDir == "" {
		sharedDir = "/data"
	}

	syncIntervalStr := os.Getenv("SYNC_INTERVAL")
	if syncIntervalStr == "" {
		syncIntervalStr = "1m"
	}
	syncInterval, err := time.ParseDuration(syncIntervalStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid SYNC_INTERVAL duration '%s': %v", syncIntervalStr, err)
	}

	readyPort := os.Getenv("READY_PORT")
	if readyPort == "" {
		readyPort = "8080"
	}

	log.Printf("Configuration:")
	log.Printf("  GCS_BUCKET:     %s", bucketName)
	log.Printf("  GCS_PREFIX:     %s", gcsPrefix)
	log.Printf("  SHARED_DIR:     %s", sharedDir)
	log.Printf("  SYNC_INTERVAL:  %v", syncInterval)
	log.Printf("  READY_PORT:     %s", readyPort)

	// 2. Setup health HTTP server
	var startupDone atomic.Bool

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if startupDone.Load() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ready"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("sync in progress"))
		}
	})

	server := &http.Server{
		Addr: ":" + readyPort,
	}

	go func() {
		log.Printf("Starting HTTP server on port %s...", readyPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("FATAL: HTTP server failed: %v", err)
		}
	}()

	// Create storage client
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("FATAL: Failed to create GCS client: %v", err)
	}
	defer storageClient.Close()

	// 3. Perform initial startup download from Cloud Storage with a standard retry loop
	log.Println("Executing initial startup download from Cloud Storage...")
	var initErr error
	delay := 2 * time.Second
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		initErr = DownloadDirectory(ctx, storageClient, bucketName, gcsPrefix, sharedDir)
		if initErr == nil {
			break
		}
		if attempt < maxAttempts {
			log.Printf("Warning: initial download attempt %d failed: %v. Retrying in %v...", attempt, initErr, delay)
			time.Sleep(delay)
			delay *= 2
		}
	}
	if initErr != nil {
		log.Fatalf("FATAL: Initial startup download failed after %d attempts: %v", maxAttempts, initErr)
	}

	// Signal readiness
	log.Println("Initial download completed successfully. Signaling readiness.")
	startupDone.Store(true)

	// 4. Setup fsnotify watcher for real-time changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("FATAL: Failed to create fsnotify watcher: %v", err)
	}
	defer watcher.Close()

	// Recursively register watches under the shared volume directory
	if err := watchDirRecursive(watcher, sharedDir); err != nil {
		log.Printf("Warning: failed to watch shared directory %s recursively: %v", sharedDir, err)
	}

	// 5. Setup ticker and signal handlers
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Single-threaded coordinator trigger helper
	triggerUpload := func(reason string) {
		log.Printf("Sync upload triggered by %s...", reason)
		syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if err := UploadDirectory(syncCtx, storageClient, bucketName, gcsPrefix, sharedDir); err != nil {
			log.Printf("Error during upload sync: %v", err)
		}
		cancel()
	}

	var debounceTimer *time.Timer
	var debounceChan <-chan time.Time

	log.Println("Entering main synchronization loop.")
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				break
			}

			// Ignore hidden/metadata temporary files (such as those starting with ".")
			base := filepath.Base(event.Name)
			if strings.HasPrefix(base, ".") || strings.HasSuffix(base, "~") {
				continue
			}

			// Process writes, creations, deletions, and renamings
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				log.Printf("Real-time file system change detected: %v", event)

				// If a new directory is created, register a watch on it recursively
				if event.Has(fsnotify.Create) {
					info, err := os.Stat(event.Name)
					if err == nil && info.IsDir() {
						_ = watchDirRecursive(watcher, event.Name)
					}
				}

				// Reset or initialize debounce timer (debounce uploads by 2 seconds of inactivity)
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.NewTimer(2 * time.Second)
				debounceChan = debounceTimer.C
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				break
			}
			log.Printf("fsnotify watcher encountered error: %v", err)

		case <-debounceChan:
			// Timer fired, execute upload sync
			debounceChan = nil
			debounceTimer = nil
			triggerUpload("real-time change events (debounced)")

		case <-ticker.C:
			// Regular periodic check fallback
			triggerUpload("periodic interval ticker")

		case sig := <-sigs:
			log.Printf("Received termination signal (%v). Commencing graceful shutdown.", sig)
			ticker.Stop()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}

			// Shutdown HTTP server
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = server.Shutdown(shutdownCtx)
			shutdownCancel()

			// Perform final sync upload
			log.Println("Executing final upload sync before exit...")
			finalSyncCtx, finalCancel := context.WithTimeout(context.Background(), 10*time.Minute)
			if err := UploadDirectory(finalSyncCtx, storageClient, bucketName, gcsPrefix, sharedDir); err != nil {
				log.Printf("Error during final upload sync: %v", err)
				finalCancel()
				os.Exit(1)
			}
			finalCancel()

			log.Println("Graceful shutdown completed successfully. Exiting.")
			os.Exit(0)
		}
	}
}

// watchDirRecursive walks root and adds any subdirectories to the watcher recursively.
func watchDirRecursive(watcher *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// Skip lost+found or any permission-restricted/system directories gracefully
			if d != nil && d.IsDir() {
				log.Printf("Warning: skipping unreadable directory during watch registration: %s: %v", path, err)
				return filepath.SkipDir
			}
			if strings.HasSuffix(path, "lost+found") || os.IsPermission(err) {
				log.Printf("Warning: skipping restricted path during watch registration: %s: %v", path, err)
				return filepath.SkipDir
			}
			return err
		}
		if d.IsDir() {
			// Skip lost+found explicitly even if no error was reported yet
			if d.Name() == "lost+found" {
				return filepath.SkipDir
			}
			log.Printf("Watching shared subdirectory: %s", path)
			if err := watcher.Add(path); err != nil {
				return fmt.Errorf("failed to watch %s: %w", path, err)
			}
		}
		return nil
	})
}
