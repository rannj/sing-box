package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type httpHandler struct {
	manager *Manager
}

func (h *httpHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/observability/v1")
	switch strings.TrimSuffix(path, "/") {
	case "":
		writeJSON(writer, http.StatusOK, map[string]any{
			"name":      "sing-box observability API",
			"version":   1,
			"endpoints": []string{"metrics", "status", "connections/active", "connections/recent", "events", "top"},
		})
	case "/metrics":
		h.metrics(writer)
	case "/status":
		writeJSON(writer, http.StatusOK, h.manager.status())
	case "/connections/active":
		writeJSON(writer, http.StatusOK, h.manager.activeConnections())
	case "/connections/recent":
		h.recentConnections(writer, request)
	case "/top":
		h.top(writer, request)
	case "/events":
		h.events(writer, request)
	default:
		writeError(writer, http.StatusNotFound, "not found")
	}
}

func (h *httpHandler) metrics(writer http.ResponseWriter) {
	var content bytes.Buffer
	if err := h.manager.writePrometheus(&content); err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(content.Bytes())
}

func (h *httpHandler) recentConnections(writer http.ResponseWriter, request *http.Request) {
	offset, err := queryInteger(request, "offset", 0)
	if err != nil || offset < 0 {
		writeError(writer, http.StatusBadRequest, "offset must be zero or greater")
		return
	}
	limit, err := queryInteger(request, "limit", 100)
	if err != nil || limit < 1 || limit > h.manager.recentConnections {
		writeError(writer, http.StatusBadRequest, fmt.Sprintf("limit must be between 1 and %d", h.manager.recentConnections))
		return
	}
	window, err := queryDuration(request, "window", h.manager.recentTTL)
	if err != nil || window <= 0 || window > h.manager.recentTTL {
		writeError(writer, http.StatusBadRequest, "window must be positive and no greater than recent_ttl")
		return
	}
	writeJSON(writer, http.StatusOK, h.manager.recentConnectionPage(offset, limit, window))
}

func (h *httpHandler) top(writer http.ResponseWriter, request *http.Request) {
	dimension := request.URL.Query().Get("dimension")
	if dimension == "" {
		dimension = "outbound"
	}
	limit, err := queryInteger(request, "limit", h.manager.topKSize)
	if err != nil || limit < 1 || limit > h.manager.topKSize {
		writeError(writer, http.StatusBadRequest, fmt.Sprintf("limit must be between 1 and %d", h.manager.topKSize))
		return
	}
	window, err := queryDuration(request, "window", h.manager.recentTTL)
	if err != nil || window <= 0 || window > h.manager.recentTTL {
		writeError(writer, http.StatusBadRequest, "window must be positive and no greater than recent_ttl")
		return
	}
	result, err := h.manager.topDimensions(dimension, window, limit)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (h *httpHandler) events(writer http.ResponseWriter, request *http.Request) {
	flusher, loaded := writer.(http.Flusher)
	if !loaded {
		writeError(writer, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	heartbeat, err := queryDuration(request, "heartbeat", 15*time.Second)
	if err != nil || heartbeat < time.Second || heartbeat > time.Minute {
		writeError(writer, http.StatusBadRequest, "heartbeat must be between 1s and 1m")
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()
	err = h.manager.streamEvents(request.Context(), heartbeat, func(event *Event) error {
		if event == nil {
			_, writeErr := fmt.Fprint(writer, ": keepalive\n\n")
			flusher.Flush()
			return writeErr
		}
		content, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return marshalErr
		}
		if _, writeErr := fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event.Type, content); writeErr != nil {
			return writeErr
		}
		flusher.Flush()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return
	}
}

func queryInteger(request *http.Request, name string, fallback int) (int, error) {
	value := request.URL.Query().Get(name)
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func queryDuration(request *http.Request, name string, fallback time.Duration) (time.Duration, error) {
	value := request.URL.Query().Get(name)
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"message": message})
}
