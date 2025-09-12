# ---- build stage ----
FROM golang:1.23-alpine AS build
WORKDIR /src

# 의존성 먼저 캐시
COPY go.mod go.sum ./
RUN go mod download

# 소스 복사 후 빌드 (리눅스 바이너리)
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /out/sigma_router -ldflags="-s -w" ./main.go

# ---- runtime stage ----
FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=build /out/sigma_router /app/sigma_router
USER nonroot:nonroot
ENTRYPOINT ["/app/sigma_router"]
