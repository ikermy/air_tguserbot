# Stage 1: Build
FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Собираем статический бинарь без CGO
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /app/tguserbot ./cmd/

# Сжимаем бинарь UPX
RUN apt-get update && apt-get install -y --no-install-recommends upx-ucl \
    && rm -rf /var/lib/apt/lists/*
RUN upx --best --lzma /app/tguserbot

# Stage 2: Only the additional native runtime dependency required by
# slim_libntgcalls.so. The distroless C/C++ image already provides glibc,
# libgcc and libstdc++.
FROM debian:bookworm-slim AS native-runtime
RUN apt-get update && apt-get install -y --no-install-recommends libz1 \
    && rm -rf /var/lib/apt/lists/*

# Stage 3: Minimal glibc runtime.
# distroless/cc contains glibc and the C/C++ runtime required by ntgcalls,
# without the Debian package manager, shell and other unused files.
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

COPY --from=builder /app/tguserbot /app/tguserbot
COPY slim_libntgcalls.so /app/slim_libntgcalls.so
COPY --from=native-runtime /lib/x86_64-linux-gnu/libz.so.1* /lib/x86_64-linux-gnu/

ENV TZ=Europe/Amsterdam
ENV LD_LIBRARY_PATH=/app

ENTRYPOINT ["/app/tguserbot"]
