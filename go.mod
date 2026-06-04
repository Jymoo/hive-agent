module github.com/hive-sre/hive-agent

go 1.24

require (
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/go-resty/resty/v2 v2.16.5
	github.com/gorilla/websocket v1.5.3
	github.com/prometheus/client_golang v1.23.2
	github.com/spf13/cobra v1.10.1
	github.com/spf13/viper v1.21.0
	go.opentelemetry.io/otel v1.38.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.38.0
	go.opentelemetry.io/otel/sdk v1.38.0
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.75.0
	k8s.io/api v0.33.4
	k8s.io/apimachinery v0.33.4
	k8s.io/client-go v0.33.4
)
