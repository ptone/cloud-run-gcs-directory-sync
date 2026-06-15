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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func TestUploadDirectory_TransientFiles(t *testing.T) {
	// Create a temporary directory for local files
	tempDir := t.TempDir()

	// 1. Create a normal file
	normalPath := filepath.Join(tempDir, "normal.txt")
	normalContent := "hello normal file"
	if err := os.WriteFile(normalPath, []byte(normalContent), 0644); err != nil {
		t.Fatalf("failed to create normal file: %v", err)
	}

	// 2. Create an unreadable file (permission 0000)
	unreadablePath := filepath.Join(tempDir, "unreadable.txt")
	if err := os.WriteFile(unreadablePath, []byte("hello unreadable"), 0644); err != nil {
		t.Fatalf("failed to create unreadable file: %v", err)
	}
	if err := os.Chmod(unreadablePath, 0000); err != nil {
		t.Fatalf("failed to chmod unreadable file: %v", err)
	}
	// Make sure we restore permissions during cleanup so t.TempDir() can clean it up successfully
	t.Cleanup(func() {
		_ = os.Chmod(unreadablePath, 0644)
	})

	// 3. Create a broken symlink (points to a non-existent file)
	brokenSymlinkPath := filepath.Join(tempDir, "broken_symlink.txt")
	if err := os.Symlink("non_existent_file_target.txt", brokenSymlinkPath); err != nil {
		t.Fatalf("failed to create broken symlink: %v", err)
	}

	// 4. Create a file that we will delete concurrently to simulate a vanishing file
	vanishedInfoPath := filepath.Join(tempDir, "vanished_info.txt")
	if err := os.WriteFile(vanishedInfoPath, []byte("hello vanished info"), 0644); err != nil {
		t.Fatalf("failed to create vanished_info file: %v", err)
	}

	// Setup a mock GCS server to intercept calls
	var mu sync.Mutex
	uploadedFiles := make(map[string][]byte)
	listCalled := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		t.Logf("Mock GCS received request: %s %s", r.Method, r.URL.Path)

		// 1. Handle listing objects
		if r.Method == "GET" && strings.Contains(r.URL.Path, "/b/test-bucket/o") {
			listCalled = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// Return empty objects list
			_, _ = w.Write([]byte(`{"kind":"storage#objects"}`))
			return
		}

		// 2. Handle upload requests (POST and PUT)
		if (r.Method == "POST" || r.Method == "PUT") && (strings.Contains(r.URL.Path, "/b/test-bucket/o") || strings.Contains(r.URL.Path, "/upload/")) {
			name := r.URL.Query().Get("name")
			if name == "" {
				// Fallback to path extraction if name is not in query params
				parts := strings.Split(r.URL.Path, "/upload/test-bucket/o/")
				if len(parts) == 2 {
					name = parts[1]
				}
			}

			if name != "" {
				body, err := io.ReadAll(r.Body)
				if err == nil {
					uploadedFiles[name] = body
				}
			}

			// Provide Location header in case the SDK is performing a resumable upload sequence
			w.Header().Set("Location", "http://"+r.Host+r.URL.Path+"?name="+name)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"kind":"storage#object"}`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	// Concurrently delete the vanished_info.txt file shortly after listing starts
	go func() {
		// Wait a small duration to let list request start or run
		time.Sleep(5 * time.Millisecond)
		_ = os.Remove(vanishedInfoPath)
	}()

	// Initialize GCS client pointing to our mock server
	ctx := context.Background()
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(ts.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create storage client: %v", err)
	}
	defer client.Close()

	// Call UploadDirectory
	err = UploadDirectory(ctx, client, "test-bucket", "shared-data", tempDir)
	if err != nil {
		t.Fatalf("UploadDirectory failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify listing was called
	if !listCalled {
		t.Error("Expected GCS list objects endpoint to be called, but it wasn't")
	}

	// Verify that normal.txt was successfully uploaded
	normalUploadedContent, exists := uploadedFiles["shared-data/normal.txt"]
	if !exists {
		t.Error("Expected normal.txt to be uploaded to GCS, but it wasn't found in uploaded files")
	} else if !strings.Contains(string(normalUploadedContent), normalContent) {
		t.Errorf("Expected uploaded content to contain %q, got %q", normalContent, string(normalUploadedContent))
	}

	// Verify that unreadable.txt, broken_symlink.txt, and vanished_info.txt were NOT uploaded
	if _, exists := uploadedFiles["shared-data/unreadable.txt"]; exists {
		t.Error("unreadable.txt was uploaded to GCS, but it should have been skipped")
	}
	if _, exists := uploadedFiles["shared-data/broken_symlink.txt"]; exists {
		t.Error("broken_symlink.txt was uploaded to GCS, but it should have been skipped")
	}
	if _, exists := uploadedFiles["shared-data/vanished_info.txt"]; exists {
		t.Error("vanished_info.txt was uploaded to GCS, but it should have been skipped")
	}
}
