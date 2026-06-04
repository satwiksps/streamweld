# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.5
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine3.23 AS build

WORKDIR /src
RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

FROM build AS proxy-build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath \
      -ldflags="-s -w -X github.com/streamweld/streamweld/internal/version.Version=${VERSION} -X github.com/streamweld/streamweld/internal/version.Commit=${COMMIT} -X github.com/streamweld/streamweld/internal/version.Date=${DATE}" \
      -o /out/streamweld-proxy ./cmd/streamweld-proxy

FROM build AS operator-build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath \
      -ldflags="-s -w -X github.com/streamweld/streamweld/internal/version.Version=${VERSION} -X github.com/streamweld/streamweld/internal/version.Commit=${COMMIT} -X github.com/streamweld/streamweld/internal/version.Date=${DATE}" \
      -o /out/streamweld-operator ./cmd/streamweld-operator

FROM scratch AS proxy
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
LABEL org.opencontainers.image.title="Streamweld proxy" \
      org.opencontainers.image.description="Durable OpenAI-compatible inference stream proxy" \
      org.opencontainers.image.source="https://github.com/streamweld/streamweld" \
      org.opencontainers.image.url="https://github.com/streamweld/streamweld" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${DATE}"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=proxy-build /out/streamweld-proxy /streamweld-proxy
USER 65532:65532
EXPOSE 8080 8081
ENTRYPOINT ["/streamweld-proxy"]

FROM scratch AS operator
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
LABEL org.opencontainers.image.title="Streamweld operator" \
      org.opencontainers.image.description="Kubernetes control plane for durable inference streams" \
      org.opencontainers.image.source="https://github.com/streamweld/streamweld" \
      org.opencontainers.image.url="https://github.com/streamweld/streamweld" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${DATE}"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=operator-build /out/streamweld-operator /streamweld-operator
USER 65532:65532
EXPOSE 8080 8081 8082 9443
ENTRYPOINT ["/streamweld-operator"]
