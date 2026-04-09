# Match go.mod; GOTOOLCHAIN=auto can still fetch a newer patch toolchain if go.mod pins one.
FROM golang:1.26-bookworm AS builder

WORKDIR /src

ENV GOTOOLCHAIN=auto

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/syncer .

# Alpine (not distroless) so Kurtosis can run `sh -c` one-shots with this image to copy
# scripts/init.sql into a files artifact — same pattern as Bedrock + faucet migrations.
# Schema stays only in this repo under scripts/init.sql; Bedrock never vendors a copy.
FROM alpine:3.19

RUN apk add --no-cache ca-certificates \
    && addgroup -g 65532 -S nonroot \
    && adduser -u 65532 -S -G nonroot -H -D nonroot

COPY --from=builder --chown=65532:65532 /out/syncer /syncer
COPY --chown=65532:65532 scripts/init.sql /init.sql

USER nonroot:nonroot

ENTRYPOINT ["/syncer"]
