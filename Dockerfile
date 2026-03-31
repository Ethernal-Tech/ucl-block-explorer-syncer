# Build requires Go version from go.mod (toolchain auto-downloads when needed).
FROM golang:1.24-bookworm AS builder

WORKDIR /src

ENV GOTOOLCHAIN=auto

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/syncer .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/syncer /syncer

USER nonroot:nonroot

ENTRYPOINT ["/syncer"]
