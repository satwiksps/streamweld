# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/streamweld-chaos-backend ./test/chaos/backend

FROM alpine:3.21
RUN addgroup -g 65532 streamweld && adduser -D -H -u 65532 -G streamweld streamweld
COPY --from=build /out/streamweld-chaos-backend /streamweld-chaos-backend
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["/streamweld-chaos-backend"]
