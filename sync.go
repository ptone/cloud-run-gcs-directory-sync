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
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// LocalFileState caches local file properties to avoid redundant MD5 calculations.
type LocalFileState struct {
	Size  int64
	Mtime time.Time
	MD5   []byte
}

var (
	localStateMutex sync.RWMutex
	localStateCache = make(map[string]LocalFileState)
)

// DownloadJob represents a file queued for download.
type DownloadJob struct {
	GCSKey    string
	LocalPath string
	Size      int64
	Updated   time.Time
	MD5       []byte
}

// SyncConfig defines configuration parameters for directory synchronization.
type SyncConfig struct {
	BucketName string
	GCSPrefix  string
	LocalDir   string
	FileMode   os.FileMode
	DirMode    os.FileMode
	FileUID    int // -1 to skip chown
	FileGID    int // -1 to skip chown
}

// DefaultSyncConfig returns a SyncConfig with standard default permissions.
func DefaultSyncConfig(bucketName, gcsPrefix, localDir string) SyncConfig {
	return SyncConfig{
		BucketName: bucketName,
		GCSPrefix:  gcsPrefix,
		LocalDir:   localDir,
		FileMode:   0644,
		DirMode:    0755,
		FileUID:    -1,
		FileGID:    -1,
	}
}

var chownWarnOnce sync.Once

func logChownWarningOnce(err error) {
	chownWarnOnce.Do(func() {
		log.Printf("Warning: failed to chown file/dir (container may not be running as root): %v", err)
	})
}

func ensureDir(dirPath string, dirMode os.FileMode, fileUID, fileGID int) error {
	if err := os.MkdirAll(dirPath, dirMode); err != nil {
		return err
	}
	_ = os.Chmod(dirPath, dirMode)
	if fileUID >= 0 || fileGID >= 0 {
		if err := os.Chown(dirPath, fileUID, fileGID); err != nil {
			logChownWarningOnce(err)
		}
	}
	return nil
}

// DownloadDirectory downloads all files from GCS bucket under gcsPrefix to localDir using default settings.
func DownloadDirectory(ctx context.Context, client *storage.Client, bucketName, gcsPrefix, localDir string) error {
	return DownloadDirectoryWithConfig(ctx, client, DefaultSyncConfig(bucketName, gcsPrefix, localDir))
}

// DownloadDirectoryWithConfig downloads all files from GCS bucket using the provided SyncConfig.
func DownloadDirectoryWithConfig(ctx context.Context, client *storage.Client, cfg SyncConfig) error {
	bucket := client.Bucket(cfg.BucketName)
	prefix := normalizePrefix(cfg.GCSPrefix)

	fileMode := cfg.FileMode
	if fileMode == 0 {
		fileMode = 0644
	}
	dirMode := cfg.DirMode
	if dirMode == 0 {
		dirMode = 0755
	}

	log.Printf("Starting initial download from gs://%s/%s to %s (fileMode: %04o, dirMode: %04o, uid: %d, gid: %d)",
		cfg.BucketName, prefix, cfg.LocalDir, fileMode, dirMode, cfg.FileUID, cfg.FileGID)

	// Ensure local directory exists
	if err := ensureDir(cfg.LocalDir, dirMode, cfg.FileUID, cfg.FileGID); err != nil {
		return fmt.Errorf("failed to create local directory %s: %w", cfg.LocalDir, err)
	}

	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)

	var jobsToDownload []DownloadJob
	skippedCount := 0

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to iterate GCS objects: %w", err)
		}

		// Skip "directories" (objects ending with trailing slash in GCS representation)
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}

		// Calculate relative path from GCS prefix
		relPath := strings.TrimPrefix(attrs.Name, prefix)
		relPath = strings.TrimPrefix(relPath, "/")
		if relPath == "" {
			continue
		}

		localPath := filepath.Join(cfg.LocalDir, filepath.FromSlash(relPath))

		// Check if local file exists and matches size and modification time to avoid redundant download
		skip := false
		if fileInfo, err := os.Stat(localPath); err == nil {
			if fileInfo.Size() == attrs.Size {
				// If local modification time matches remote updated time, we can skip download and hash check!
				if fileInfo.ModTime().Equal(attrs.Updated) {
					skip = true
				} else {
					// Otherwise fall back to MD5 comparison
					localMD5, err := calculateMD5(localPath)
					if err == nil && matchesMD5(localMD5, attrs.MD5) {
						skip = true
						// Repair mtime to match GCS so we skip MD5 check next time
						_ = os.Chtimes(localPath, attrs.Updated, attrs.Updated)
					}
				}
			}
		}

		if skip {
			// Populate cache immediately so future uploads skip MD5 recalculation
			localStateMutex.Lock()
			localStateCache[localPath] = LocalFileState{
				Size:  attrs.Size,
				Mtime: attrs.Updated,
				MD5:   attrs.MD5,
			}
			localStateMutex.Unlock()

			skippedCount++
			continue
		}

		jobsToDownload = append(jobsToDownload, DownloadJob{
			GCSKey:    attrs.Name,
			LocalPath: localPath,
			Size:      attrs.Size,
			Updated:   attrs.Updated,
			MD5:       attrs.MD5,
		})
	}

	if len(jobsToDownload) == 0 {
		log.Printf("Initial download complete. No files to download. Skipped: %d files.", skippedCount)
		return nil
	}

	log.Printf("Queued %d files for concurrent download (skipped %d matches)...", len(jobsToDownload), skippedCount)

	// Run concurrent download worker pool
	const numWorkers = 5
	jobsChan := make(chan DownloadJob, len(jobsToDownload))
	errsChan := make(chan error, len(jobsToDownload))

	for w := 0; w < numWorkers; w++ {
		go func() {
			for job := range jobsChan {
				errsChan <- downloadFileWithRetry(ctx, bucket, job.GCSKey, job.LocalPath, job.Updated, job.MD5, job.Size, fileMode, cfg.FileUID, cfg.FileGID, dirMode)
			}
		}()
	}

	for _, job := range jobsToDownload {
		jobsChan <- job
	}
	close(jobsChan)

	var downloadErrs []error
	downloadedCount := 0
	for i := 0; i < len(jobsToDownload); i++ {
		err := <-errsChan
		if err != nil {
			downloadErrs = append(downloadErrs, err)
		} else {
			downloadedCount++
		}
	}

	if len(downloadErrs) > 0 {
		return fmt.Errorf("failed to complete initial download: %w", errors.Join(downloadErrs...))
	}

	log.Printf("Initial download complete. Downloaded: %d files, Skipped: %d files.", downloadedCount, skippedCount)
	return nil
}

// UploadJob represents a file queued for upload.
type UploadJob struct {
	LocalPath string
	GCSKey    string
	Size      int64
	Mtime     time.Time
	MD5       []byte
}

// UploadDirectory uploads new or modified files from localDir to GCS bucket under gcsPrefix, and deletes removed files.
func UploadDirectory(ctx context.Context, client *storage.Client, bucketName, gcsPrefix, localDir string) error {
	bucket := client.Bucket(bucketName)
	prefix := normalizePrefix(gcsPrefix)

	log.Printf("Starting directory upload sync from %s to gs://%s/%s", localDir, bucketName, prefix)

	// Step 1: List all existing GCS objects under the prefix to build a metadata cache
	gcsFiles := make(map[string]*storage.ObjectAttrs)
	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list existing GCS objects for diff: %w", err)
		}
		if !strings.HasSuffix(attrs.Name, "/") {
			gcsFiles[attrs.Name] = attrs
		}
	}

	var jobsToUpload []UploadJob
	skippedCount := 0
	encounteredGCSKeys := make(map[string]bool)

	// Step 2: Walk the local directory recursively to identify modified files
	err := filepath.WalkDir(localDir, func(localPath string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				log.Printf("Warning: skipping vanished file during walk: %s: %v", localPath, err)
				return nil
			}
			// Skip lost+found or any permission-restricted/system directories gracefully
			if d != nil && d.IsDir() {
				log.Printf("Warning: skipping unreadable directory during walk: %s: %v", localPath, err)
				return filepath.SkipDir
			}
			if strings.HasSuffix(localPath, "lost+found") || os.IsPermission(err) {
				log.Printf("Warning: skipping restricted path during walk: %s: %v", localPath, err)
				return filepath.SkipDir
			}
			return err
		}
		if d.IsDir() {
			// Skip lost+found explicitly even if no error was reported yet
			if d.Name() == "lost+found" {
				return filepath.SkipDir
			}
			return nil
		}

		// Get relative path using forward slashes
		relPath, err := filepath.Rel(localDir, localPath)
		if err != nil {
			return fmt.Errorf("failed to compute relative path for %s: %w", localPath, err)
		}
		gcsKey := path.Join(prefix, filepath.ToSlash(relPath))
		encounteredGCSKeys[gcsKey] = true

		fileInfo, err := d.Info()
		if err != nil {
			log.Printf("Warning: skipping vanished file %s: %v", localPath, err)
			return nil
		}

		size := fileInfo.Size()
		mtime := fileInfo.ModTime()
		var md5Bytes []byte

		// Look up in-memory cache to see if size and mtime match
		localStateMutex.RLock()
		cached, exists := localStateCache[localPath]
		localStateMutex.RUnlock()

		if exists && cached.Size == size && cached.Mtime.Equal(mtime) {
			md5Bytes = cached.MD5
		} else {
			// Compute MD5 and update cache
			calculatedMD5, err := calculateMD5(localPath)
			if err != nil {
				log.Printf("Warning: skipping file with unreadable content %s: %v", localPath, err)
				return nil
			}
			md5Bytes = calculatedMD5

			localStateMutex.Lock()
			localStateCache[localPath] = LocalFileState{
				Size:  size,
				Mtime: mtime,
				MD5:   md5Bytes,
			}
			localStateMutex.Unlock()
		}

		// Check if remote GCS object exists and matches size & MD5 hash
		if gcsAttrs, exists := gcsFiles[gcsKey]; exists {
			if size == gcsAttrs.Size && matchesMD5(md5Bytes, gcsAttrs.MD5) {
				skippedCount++
				return nil
			}
		}

		jobsToUpload = append(jobsToUpload, UploadJob{
			LocalPath: localPath,
			GCSKey:    gcsKey,
			Size:      size,
			Mtime:     mtime,
			MD5:       md5Bytes,
		})
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed walking local directory: %w", err)
	}

	// Step 3: Handle deletion of removed files
	deletedCount := 0
	var deletionErrs []error
	for gcsKey := range gcsFiles {
		if !encounteredGCSKeys[gcsKey] {
			log.Printf("Deleting removed file from GCS: gs://%s/%s", bucketName, gcsKey)
			var delErr error
			delay := 500 * time.Millisecond
			for attempt := 1; attempt <= 3; attempt++ {
				delErr = bucket.Object(gcsKey).Delete(ctx)
				if delErr == nil {
					break
				}
				log.Printf("Warning: delete attempt %d for gs://%s/%s failed: %v", attempt, bucketName, gcsKey, delErr)
				time.Sleep(delay)
				delay *= 2
			}
			if delErr != nil {
				deletionErrs = append(deletionErrs, fmt.Errorf("failed to delete gs://%s/%s: %w", bucketName, gcsKey, delErr))
			} else {
				deletedCount++
			}
		}
	}

	// Step 4: Run concurrent upload worker pool
	uploadedCount := 0
	var uploadErrs []error

	if len(jobsToUpload) > 0 {
		log.Printf("Queuing %d files for concurrent upload...", len(jobsToUpload))
		const numWorkers = 5
		jobsChan := make(chan UploadJob, len(jobsToUpload))
		errsChan := make(chan error, len(jobsToUpload))

		for w := 0; w < numWorkers; w++ {
			go func() {
				for job := range jobsChan {
					errsChan <- uploadFileWithRetry(ctx, bucket, job.LocalPath, job.GCSKey, job.Size, job.Mtime, job.MD5)
				}
			}()
		}

		for _, job := range jobsToUpload {
			jobsChan <- job
		}
		close(jobsChan)

		for i := 0; i < len(jobsToUpload); i++ {
			err := <-errsChan
			if err != nil {
				uploadErrs = append(uploadErrs, err)
			} else {
				uploadedCount++
			}
		}
	}

	// Combine all errors from uploads and deletions
	var allErrs []error
	allErrs = append(allErrs, uploadErrs...)
	allErrs = append(allErrs, deletionErrs...)

	if len(allErrs) > 0 {
		return fmt.Errorf("upload sync completed with errors: %w", errors.Join(allErrs...))
	}

	log.Printf("Upload sync complete. Uploaded: %d, Skipped: %d, Deleted from GCS: %d.", uploadedCount, skippedCount, deletedCount)
	return nil
}

// Helpers

func normalizePrefix(p string) string {
	p = strings.TrimPrefix(p, "/")
	if p != "" && !strings.HasSuffix(p, "/") {
		p = p + "/"
	}
	return p
}

func calculateMD5(filePath string) ([]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func matchesMD5(local []byte, remote []byte) bool {
	if len(local) != len(remote) {
		return false
	}
	for i := range local {
		if local[i] != remote[i] {
			return false
		}
	}
	return true
}

func downloadFile(ctx context.Context, bucket *storage.BucketHandle, gcsKey, localPath string, fileMode os.FileMode, fileUID, fileGID int) error {
	reader, err := bucket.Object(gcsKey).NewReader(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()

	writer, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	_ = os.Chmod(localPath, fileMode)
	if fileUID >= 0 || fileGID >= 0 {
		if err := os.Chown(localPath, fileUID, fileGID); err != nil {
			logChownWarningOnce(err)
		}
	}
	return nil
}

func uploadFile(ctx context.Context, bucket *storage.BucketHandle, localPath, gcsKey string) error {
	reader, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	writer := bucket.Object(gcsKey).NewWriter(ctx)
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		return err
	}

	return writer.Close()
}

func downloadFileWithRetry(ctx context.Context, bucket *storage.BucketHandle, gcsKey, localPath string, updatedTime time.Time, md5Bytes []byte, size int64, fileMode os.FileMode, fileUID, fileGID int, dirMode os.FileMode) error {
	var err error
	delay := 1 * time.Second
	maxAttempts := 3

	// Ensure parent directory exists before downloading
	if errDir := ensureDir(filepath.Dir(localPath), dirMode, fileUID, fileGID); errDir != nil {
		return fmt.Errorf("failed to create parent directory for %s: %w", localPath, errDir)
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = downloadFile(ctx, bucket, gcsKey, localPath, fileMode, fileUID, fileGID)
		if err == nil {
			// Preserving remote modification time locally
			if errCht := os.Chtimes(localPath, updatedTime, updatedTime); errCht != nil {
				log.Printf("Warning: failed to set mtime for %s: %v", localPath, errCht)
			}

			// Populate cache immediately so future uploads skip MD5 calculation
			localStateMutex.Lock()
			localStateCache[localPath] = LocalFileState{
				Size:  size,
				Mtime: updatedTime,
				MD5:   md5Bytes,
			}
			localStateMutex.Unlock()

			log.Printf("Successfully downloaded gs://%s -> %s (%d bytes)", gcsKey, localPath, size)
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("Download attempt %d for %s failed: %v. Retrying in %v...", attempt, gcsKey, err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("failed to download %s after %d attempts: %w", gcsKey, maxAttempts, err)
}

func uploadFileWithRetry(ctx context.Context, bucket *storage.BucketHandle, localPath, gcsKey string, size int64, mtime time.Time, md5Bytes []byte) error {
	var err error
	delay := 1 * time.Second
	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = uploadFile(ctx, bucket, localPath, gcsKey)
		if err == nil {
			// Update local cache state with mtime and MD5 on successful upload
			localStateMutex.Lock()
			localStateCache[localPath] = LocalFileState{
				Size:  size,
				Mtime: mtime,
				MD5:   md5Bytes,
			}
			localStateMutex.Unlock()

			log.Printf("Successfully uploaded %s -> gs://%s (%d bytes)", localPath, gcsKey, size)
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("Upload attempt %d for %s failed: %v. Retrying in %v...", attempt, localPath, err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("failed to upload %s after %d attempts: %w", localPath, maxAttempts, err)
}
