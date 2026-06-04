package registration

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-resty/resty/v2"
	"github.com/hive-sre/hive-agent/internal/auth"
	"github.com/hive-sre/hive-agent/internal/capabilities"
	"github.com/hive-sre/hive-agent/internal/config"
	"go.uber.org/zap"
)

var Capabilities = capabilities.All()

type Request struct {
	ClusterName       string   `json:"clusterName"`
	Provider          string   `json:"provider"`
	AgentVersion      string   `json:"agentVersion"`
	KubernetesVersion string   `json:"kubernetesVersion"`
	ClusterUID        string   `json:"clusterUID"`
	NodeCount         int      `json:"nodeCount"`
	APIKey            string   `json:"apiKey"`
	Capabilities      []string `json:"capabilities"`
	State             string   `json:"state,omitempty"`
}

type Response struct {
	ClusterID    string `json:"clusterId"`
	JWT          string `json:"jwt"`
	WSEndpoint   string `json:"wsEndpoint,omitempty"`
	WebSocketURL string `json:"websocketUrl,omitempty"`
}

type NodeHealth struct {
	Ready    int `json:"ready"`
	NotReady int `json:"notReady"`
}

type PodHealth struct {
	Running int `json:"running"`
	Pending int `json:"pending"`
	Failed  int `json:"failed"`
}

type WorkloadHealth struct {
	Deployments  int `json:"deployments"`
	StatefulSets int `json:"statefulsets"`
	DaemonSets   int `json:"daemonsets"`
}

type CapabilitiesPayload struct {
	ClusterID    string   `json:"clusterId"`
	Capabilities []string `json:"capabilities"`
}

type HeartbeatPayload struct {
	ClusterID     string         `json:"clusterId"`
	State         string         `json:"state"`
	ClusterHealth string         `json:"clusterHealth"`
	Nodes         NodeHealth     `json:"nodes"`
	Pods          PodHealth      `json:"pods"`
	Workloads     WorkloadHealth `json:"workloads"`
	Timestamp     time.Time      `json:"timestamp"`
}

type InventoryEvent struct {
	ClusterID       string         `json:"clusterId"`
	Type            string         `json:"type"`
	Operation       string         `json:"operation"`
	ResourceVersion string         `json:"resourceVersion"`
	Object          map[string]any `json:"object,omitempty"`
	Timestamp       time.Time      `json:"timestamp"`
}

type InventoryDeltaPayload struct {
	ClusterID string           `json:"clusterId"`
	Events    []InventoryEvent `json:"events"`
}

type CredentialRotation struct {
	JWT        string `json:"jwt,omitempty"`
	APIKey     string `json:"apiKey,omitempty"`
	WSEndpoint string `json:"wsEndpoint,omitempty"`
}

type Client struct {
	cfg    config.Config
	http   *resty.Client
	tokens *auth.TokenStore
	log    *zap.Logger
}

func NewClient(cfg config.Config, tokens *auth.TokenStore, log *zap.Logger) *Client {
	return &Client{cfg: cfg, tokens: tokens, log: log, http: resty.New().SetTLSClientConfig(cfg.TLSConfig()).SetTimeout(30 * time.Second).SetRetryCount(0)}
}
func (c *Client) TokenStore() *auth.TokenStore { return c.tokens }

func (c *Client) Register(ctx context.Context, req Request) (Response, error) {
	if len(req.Capabilities) == 0 {
		req.Capabilities = Capabilities
	}
	var out Response
	err := c.retry(ctx, func() error {
		resp, err := c.http.R().SetContext(ctx).SetBody(req).SetResult(&out).Post(c.cfg.APIEndpoint + "/api/v1/agents/register")
		if err != nil {
			return err
		}
		if resp.StatusCode() >= 300 {
			return fmt.Errorf("register status %d: %s", resp.StatusCode(), resp.String())
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	if out.WebSocketURL == "" {
		out.WebSocketURL = out.WSEndpoint
	}
	c.tokens.Set(out.JWT)
	return out, nil
}

func (c *Client) RegisterCapabilities(ctx context.Context, clusterID string, capabilities []string) error {
	return c.postJWT(ctx, "/api/v1/agents/capabilities", CapabilitiesPayload{ClusterID: clusterID, Capabilities: capabilities})
}

func (c *Client) Heartbeat(ctx context.Context, hb HeartbeatPayload) error {
	return c.postJWT(ctx, "/api/v1/agents/heartbeat", hb)
}

func (c *Client) InventoryDelta(ctx context.Context, delta InventoryDeltaPayload) error {
	return c.postJWT(ctx, "/api/v1/agents/inventory/delta", delta)
}

func (c *Client) Refresh(ctx context.Context) (Response, error) {
	var out Response
	err := c.retry(ctx, func() error {
		resp, err := c.http.R().SetContext(ctx).SetHeader("Authorization", "Bearer "+c.tokens.Get()).SetResult(&out).Post(c.cfg.APIEndpoint + "/api/v1/agents/token/refresh")
		if err != nil {
			return err
		}
		if resp.StatusCode() == http.StatusUnauthorized || resp.StatusCode() == http.StatusForbidden {
			return fmt.Errorf("token refresh unauthorized")
		}
		if resp.StatusCode() >= 300 {
			return fmt.Errorf("refresh status %d", resp.StatusCode())
		}
		return nil
	})
	if err == nil && out.JWT != "" {
		c.tokens.Set(out.JWT)
	}
	return out, err
}

func (r Response) TunnelURL() string {
	if r.WebSocketURL != "" {
		return r.WebSocketURL
	}
	return r.WSEndpoint
}

func (c *Client) ApplyRotation(rotation CredentialRotation) {
	if rotation.JWT != "" {
		c.tokens.Set(rotation.JWT)
	}
}

func (c *Client) postJWT(ctx context.Context, path string, body any) error {
	return c.retry(ctx, func() error {
		resp, err := c.http.R().SetContext(ctx).SetHeader("Authorization", "Bearer "+c.tokens.Get()).SetBody(body).Post(c.cfg.APIEndpoint + path)
		if err != nil {
			return err
		}
		if resp.StatusCode() >= 300 {
			return fmt.Errorf("%s status %d", path, resp.StatusCode())
		}
		return nil
	})
}

func (c *Client) retry(ctx context.Context, op func() error) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 2 * time.Minute
	return backoff.Retry(func() error {
		select {
		case <-ctx.Done():
			return backoff.Permanent(ctx.Err())
		default:
			return op()
		}
	}, b)
}
