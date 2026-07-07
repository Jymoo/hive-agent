FROM golang:1.24-bookworm AS build

WORKDIR /src

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/hive-agent \
    ./cmd/agent

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/hive-agent /usr/local/bin/hive-agent

# USER nonroot:nonroot
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/hive-agent"]






# FROM golang:1.24-bookworm AS build
# WORKDIR /src
# COPY go.mod go.sum* ./
# RUN go mod download
# COPY . .
# RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/hive-agent ./cmd/agent

# FROM gcr.io/distroless/static-debian12:nonroot
# COPY --from=build /out/hive-agent /usr/local/bin/hive-agent
# USER nonroot:nonroot
# ENTRYPOINT ["/usr/local/bin/hive-agent"]