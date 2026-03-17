# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM build AS proxy-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/streamweld-proxy ./cmd/streamweld-proxy

FROM build AS operator-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/streamweld-operator ./cmd/streamweld-operator

FROM gcr.io/distroless/static-debian12:nonroot AS proxy
COPY --from=proxy-build /out/streamweld-proxy /streamweld-proxy
USER 65532:65532
ENTRYPOINT ["/streamweld-proxy"]

FROM gcr.io/distroless/static-debian12:nonroot AS operator
COPY --from=operator-build /out/streamweld-operator /streamweld-operator
USER 65532:65532
ENTRYPOINT ["/streamweld-operator"]
