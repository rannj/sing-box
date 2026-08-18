package observability

import (
	"net/http"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

const (
	DefaultRecentConnections = 1000
	DefaultRecentTTL         = 30 * time.Minute
	DefaultTopKSize          = 100
	MaxRecentConnections     = 100000
	MaxTopKSize              = 1000
)

type Service interface {
	adapter.LifecycleService
	Handler() http.Handler
}

type Connection struct {
	ID              string    `json:"id"`
	Network         string    `json:"network"`
	Inbound         string    `json:"inbound,omitempty"`
	SourceIP        string    `json:"sourceIP,omitempty"`
	SourcePort      uint16    `json:"sourcePort,omitempty"`
	SourceMAC       string    `json:"sourceMAC,omitempty"`
	SourceHostname  string    `json:"sourceHostname,omitempty"`
	DestinationIP   string    `json:"destinationIP,omitempty"`
	DestinationPort uint16    `json:"destinationPort,omitempty"`
	Domain          string    `json:"domain,omitempty"`
	Process         string    `json:"process,omitempty"`
	User            string    `json:"user,omitempty"`
	Outbound        string    `json:"outbound,omitempty"`
	OutboundType    string    `json:"outboundType,omitempty"`
	Chain           []string  `json:"chain,omitempty"`
	Rule            string    `json:"rule,omitempty"`
	StartedAt       time.Time `json:"startedAt"`
	ClosedAt        time.Time `json:"closedAt,omitempty"`
	Upload          int64     `json:"upload"`
	Download        int64     `json:"download"`
}

type ConnectionPage struct {
	Data  []Connection `json:"data"`
	Total int          `json:"total"`
}

type Dimension struct {
	Value       string `json:"value"`
	Upload      int64  `json:"upload"`
	Download    int64  `json:"download"`
	Connections int64  `json:"connections"`
}

type DimensionPage struct {
	Dimension string      `json:"dimension"`
	Window    string      `json:"window"`
	Data      []Dimension `json:"data"`
	Total     int         `json:"total"`
}

type Status struct {
	Version               string  `json:"version"`
	UptimeSeconds         float64 `json:"uptimeSeconds"`
	MemoryBytes           uint64  `json:"memoryBytes"`
	Goroutines            int     `json:"goroutines"`
	ActiveConnections     int     `json:"activeConnections"`
	RecentConnections     int     `json:"recentConnections"`
	ConnectionsTotal      uint64  `json:"connectionsTotal"`
	UploadBytesTotal      int64   `json:"uploadBytesTotal"`
	DownloadBytesTotal    int64   `json:"downloadBytesTotal"`
	RecentConnectionLimit int     `json:"recentConnectionLimit"`
	RecentTTL             string  `json:"recentTTL"`
	TopKSize              int     `json:"topKSize"`
	ExposeSensitive       bool    `json:"exposeSensitive"`
}

type Event struct {
	Type       string     `json:"type"`
	Connection Connection `json:"connection"`
}
