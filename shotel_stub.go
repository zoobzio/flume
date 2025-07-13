package plugz

import "github.com/zoobzio/sctx"

// TODO:SHOTEL - Replace with real shotel when ready

type Room interface {
	ID() string
	// Future: ProcessInRoom(ByteProcessor) (ByteProcessor, error)
}

type RoomFactory interface {
	CreateRoom(ctx sctx.Context) (Room, error)
}

// Stub implementations
type stubRoom struct {
	id string
}

func (r *stubRoom) ID() string { return r.id }

type stubRoomFactory struct{}

func (f *stubRoomFactory) CreateRoom(ctx sctx.Context) (Room, error) {
	return &stubRoom{id: "stub-room"}, nil
}

var defaultRoomFactory RoomFactory = &stubRoomFactory{}