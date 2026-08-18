package trafficcontrol

import (
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/require"
)

type testTracker struct {
	metadata TrackerMetadata
}

func (t *testTracker) Metadata() *TrackerMetadata {
	return &t.metadata
}

func (t *testTracker) Close() error {
	return nil
}

func TestClosedConnectionsLimit(t *testing.T) {
	manager := NewManager(nil)
	require.NoError(t, manager.Start(adapter.StartStateInitialize))
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	manager.SetClosedConnectionsLimit(2)

	ids := make([]uuid.UUID, 3)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV4())
		tracker := &testTracker{metadata: TrackerMetadata{
			ID:       ids[i],
			Upload:   new(atomic.Int64),
			Download: new(atomic.Int64),
		}}
		manager.join(tracker)
		manager.leave(tracker)
	}

	closed := manager.ClosedConnections()
	require.Len(t, closed, 2)
	require.Equal(t, ids[1], closed[0].ID)
	require.Equal(t, ids[2], closed[1].ID)

	manager.SetClosedConnectionsLimit(1)
	closed = manager.ClosedConnections()
	require.Len(t, closed, 1)
	require.Equal(t, ids[2], closed[0].ID)
}
