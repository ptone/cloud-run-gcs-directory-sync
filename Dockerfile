# Build stage
FROM golang:1.26-alpine AS builder

# Install certificates and timezone data
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY *.go ./

# Build statically-linked, highly optimized production binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /gcs-sidecar .

# Final stage: ultra-minimal, secure execution environment
FROM alpine:3.19.1

ARG UID=1000
ARG GID=1000

# Copy CA certs and timezones for secure HTTPS and local time configurations
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy static compiled binary from builder
COPY --from=builder /gcs-sidecar /gcs-sidecar

# Create a standard non-root user and group (default UID/GID 1000 to match standard non-root container users)
RUN addgroup -g ${GID} -S sidecar-group && adduser -u ${UID} -S sidecar-user -G sidecar-group

# Ensure shared directory exists and is writable by non-root user
RUN mkdir -p /data && chown -R sidecar-user:sidecar-group /data

# Run container as non-root (UID 1000:1000 by default)
USER ${UID}:${GID}

# Set default directory to the shared mount point
WORKDIR /data

# Expose default readiness port
EXPOSE 8080

# Execute binary
ENTRYPOINT ["/gcs-sidecar"]
