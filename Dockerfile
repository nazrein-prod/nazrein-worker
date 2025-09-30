
FROM golang:1.23-alpine AS builder
WORKDIR /build

# Install Doppler CLI in the builder stage
RUN apk add --no-cache curl sudo gnupg
RUN (curl -Ls --tlsv1.2 --proto "=https" --retry 3 https://cli.doppler.com/install.sh || wget -t 3 -qO- https://cli.doppler.com/install.sh) | sh

COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=linux go build -o worker

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app

# Copy the Doppler binary from builder stage
COPY --from=builder /usr/local/bin/doppler /usr/local/bin/doppler
COPY --from=builder /build/worker .

USER nonroot:nonroot
CMD ["doppler", "run", "--", "/app/worker"]
