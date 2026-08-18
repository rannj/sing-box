package trafficcontrol

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/compatible"
	"github.com/sagernet/sing/common/cleanup"
	"github.com/sagernet/sing/common/observable"
	"github.com/sagernet/sing/common/x/list"

	"github.com/gofrs/uuid/v5"
)

type ConnectionEventType int

const (
	ConnectionEventNew ConnectionEventType = iota
	ConnectionEventClosed
)

type ConnectionEvent struct {
	Type     ConnectionEventType
	ID       uuid.UUID
	Metadata *TrackerMetadata
	ClosedAt time.Time
}

type TrafficCounters struct {
	UploadBytes   atomic.Int64
	DownloadBytes atomic.Int64
}

type ConnectionObserver interface {
	TrafficCounters(metadata TrackerMetadata) *TrafficCounters
	ConnectionOpened(metadata TrackerMetadata)
	ConnectionClosed(metadata TrackerMetadata)
}

const defaultClosedConnectionsLimit = 1000

var (
	_ adapter.ConnectionTracker = (*Manager)(nil)
	_ adapter.LifecycleService  = (*Manager)(nil)
)

type Manager struct {
	outbound      adapter.OutboundManager
	uploadTotal   atomic.Int64
	downloadTotal atomic.Int64

	connections             compatible.Map[uuid.UUID, Tracker]
	closedConnectionsAccess sync.Mutex
	closedConnections       list.List[TrackerMetadata]
	closedConnectionsLimit  int

	eventSubscriber *observable.Subscriber[ConnectionEvent]
	eventObserver   *observable.Observer[ConnectionEvent]
	observerAccess  sync.RWMutex
	observer        ConnectionObserver
	cleaner         *cleanup.Cleaner
}

func NewManager(outbound adapter.OutboundManager) *Manager {
	return &Manager{
		outbound:               outbound,
		closedConnectionsLimit: defaultClosedConnectionsLimit,
		eventSubscriber:        observable.NewSubscriber[ConnectionEvent](256),
	}
}

func (m *Manager) Name() string {
	return "traffic manager"
}

func (m *Manager) Start(stage adapter.StartStage) error {
	if stage == adapter.StartStateInitialize {
		m.eventObserver = observable.NewObserver(m.eventSubscriber, 64)
		m.cleaner = cleanup.Add(m.Clear)
	}
	return nil
}

func (m *Manager) Close() error {
	if m.cleaner != nil {
		m.cleaner.Close()
	}
	if m.eventObserver != nil {
		return m.eventObserver.Close()
	}
	return nil
}

func (m *Manager) SubscribeEvents() (observable.Subscription[ConnectionEvent], <-chan struct{}, error) {
	return m.eventObserver.Subscribe()
}

func (m *Manager) UnSubscribeEvents(subscription observable.Subscription[ConnectionEvent]) {
	m.eventObserver.UnSubscribe(subscription)
}

func (m *Manager) SetConnectionObserver(observer ConnectionObserver) {
	m.observerAccess.Lock()
	m.observer = observer
	m.observerAccess.Unlock()
}

func (m *Manager) SetClosedConnectionsLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	m.closedConnectionsAccess.Lock()
	m.closedConnectionsLimit = limit
	for m.closedConnections.Len() > limit {
		m.closedConnections.PopFront()
	}
	m.closedConnectionsAccess.Unlock()
}

func (m *Manager) trafficCounters(metadata TrackerMetadata) *TrafficCounters {
	m.observerAccess.RLock()
	defer m.observerAccess.RUnlock()
	if m.observer == nil {
		return nil
	}
	return m.observer.TrafficCounters(metadata)
}

func (m *Manager) join(tracker Tracker) {
	metadata := tracker.Metadata()
	m.connections.Store(metadata.ID, tracker)
	m.observerAccess.RLock()
	if m.observer != nil {
		m.observer.ConnectionOpened(*metadata)
	}
	m.observerAccess.RUnlock()
	m.eventSubscriber.Emit(ConnectionEvent{
		Type:     ConnectionEventNew,
		ID:       metadata.ID,
		Metadata: metadata,
	})
}

func (m *Manager) leave(tracker Tracker) {
	metadata := tracker.Metadata()
	_, loaded := m.connections.LoadAndDelete(metadata.ID)
	if !loaded {
		return
	}
	closedAt := time.Now()
	metadata.ClosedAt = closedAt
	metadataCopy := *metadata
	m.closedConnectionsAccess.Lock()
	if m.closedConnectionsLimit > 0 && m.closedConnections.Len() >= m.closedConnectionsLimit {
		m.closedConnections.PopFront()
	}
	if m.closedConnectionsLimit > 0 {
		m.closedConnections.PushBack(metadataCopy)
	}
	m.closedConnectionsAccess.Unlock()
	m.observerAccess.RLock()
	if m.observer != nil {
		m.observer.ConnectionClosed(metadataCopy)
	}
	m.observerAccess.RUnlock()
	m.eventSubscriber.Emit(ConnectionEvent{
		Type:     ConnectionEventClosed,
		ID:       metadata.ID,
		Metadata: &metadataCopy,
		ClosedAt: closedAt,
	})
}

func (m *Manager) Total() (uplinkTotal int64, downlinkTotal int64) {
	return m.uploadTotal.Load(), m.downloadTotal.Load()
}

func (m *Manager) ConnectionsLen() int {
	return m.connections.Len()
}

func (m *Manager) Connections() []*TrackerMetadata {
	var connections []*TrackerMetadata
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		connections = append(connections, tracker.Metadata())
		return true
	})
	return connections
}

func (m *Manager) ClosedConnections() []*TrackerMetadata {
	m.closedConnectionsAccess.Lock()
	values := m.closedConnections.Array()
	m.closedConnectionsAccess.Unlock()
	if len(values) == 0 {
		return nil
	}
	connections := make([]*TrackerMetadata, len(values))
	for i := range values {
		connections[i] = &values[i]
	}
	return connections
}

func (m *Manager) Connection(id uuid.UUID) Tracker {
	connection, loaded := m.connections.Load(id)
	if !loaded {
		return nil
	}
	return connection
}

func (m *Manager) CloseAllConnections() {
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		tracker.Close()
		return true
	})
}

func (m *Manager) Clear() {
	m.closedConnectionsAccess.Lock()
	defer m.closedConnectionsAccess.Unlock()
	m.closedConnections.Init()
}
