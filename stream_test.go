package flume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zoobzio/pipz"
)

// testData implements pipz.Cloner for testing.
type testData struct {
	value string
	mu    sync.Mutex
}

func (t *testData) Clone() *testData {
	t.mu.Lock()
	defer t.mu.Unlock()
	return &testData{value: t.value}
}

func TestChannelFactory(t *testing.T) {
	t.Run("add and get channel", func(t *testing.T) {
		factory := New[*testData]()
		channel := make(chan *testData, 10)

		// Add channel
		factory.AddChannel("test-channel", channel)

		// Check HasChannel
		if !factory.HasChannel("test-channel") {
			t.Error("expected channel to exist")
		}

		// Get channel
		retrieved, exists := factory.GetChannel("test-channel")
		if !exists {
			t.Error("expected channel to exist")
		}
		if retrieved != channel {
			t.Error("expected to get same channel instance")
		}

		// List channels
		channels := factory.ListChannels()
		if len(channels) != 1 || channels[0] != "test-channel" {
			t.Errorf("expected ['test-channel'], got %v", channels)
		}
	})

	t.Run("remove channel", func(t *testing.T) {
		factory := New[*testData]()
		channel := make(chan *testData, 10)

		factory.AddChannel("test-channel", channel)

		// Remove channel
		removed := factory.RemoveChannel("test-channel")
		if !removed {
			t.Error("expected channel to be removed")
		}

		// Verify it's gone
		if factory.HasChannel("test-channel") {
			t.Error("expected channel to not exist")
		}

		// Remove again should return false
		removed = factory.RemoveChannel("test-channel")
		if removed {
			t.Error("expected false for non-existent channel")
		}
	})
}

func TestBuildStream(t *testing.T) {
	factory := New[*testData]()

	// Add processors for testing
	factory.Add(pipz.Transform("processor", func(_ context.Context, data *testData) *testData {
		data.value += "-processed"
		return data
	}))

	t.Run("build stream node", func(t *testing.T) {
		channel := make(chan *testData, 10)
		factory.AddChannel("test-channel", channel)

		node := &Node{
			Stream: "test-channel",
		}

		effect, err := factory.buildStream(node, "root")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Test it works
		ctx := context.Background()
		input := &testData{value: "test"}
		result, err := effect.Process(ctx, input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Should return the original input
		if result != input {
			t.Error("expected effect to return original input")
		}

		// Verify channel received data
		select {
		case received := <-channel:
			if received.value != "test" {
				t.Errorf("expected 'test', got '%s'", received.value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("build stream node errors", func(t *testing.T) {
		tests := []struct {
			name string
			node *Node
			want string
		}{
			{
				name: "empty stream name",
				node: &Node{},
				want: "root: stream node requires a stream name",
			},
			{
				name: "non-existent channel",
				node: &Node{Stream: "missing"},
				want: "root: channel 'missing' not found",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := factory.buildStream(tt.node, "root")
				if err == nil {
					t.Error("expected error")
				} else if err.Error() != tt.want {
					t.Errorf("expected error '%s', got '%v'", tt.want, err)
				}
			})
		}
	})
}

func TestStreamValidation(t *testing.T) {
	factory := New[*testData]()

	t.Run("valid stream", func(t *testing.T) {
		channel := make(chan *testData, 10)
		factory.AddChannel("test-channel", channel)

		schema := Schema{
			Node: Node{
				Stream: "test-channel",
			},
		}

		err := factory.ValidateSchema(schema)
		if err != nil {
			t.Errorf("unexpected validation error: %v", err)
		}
	})

	t.Run("stream validation errors", func(t *testing.T) {
		tests := []struct {
			name   string
			node   Node
			errors []string
		}{
			{
				name:   "empty stream name",
				node:   Node{},
				errors: []string{"empty node - must have either ref, type, or stream"},
			},
			{
				name: "missing channel",
				node: Node{
					Stream: "missing",
				},
				errors: []string{"channel 'missing' not found"},
			},
			{
				name: "stream with invalid child",
				node: Node{
					Stream: "test-channel",
					Child:  &Node{Ref: "missing-processor"},
				},
				errors: []string{"processor 'missing-processor' not found"},
			},
			{
				name: "stream with invalid children",
				node: Node{
					Stream:   "test-channel",
					Children: []Node{{Ref: "missing-processor"}},
				},
				errors: []string{"processor 'missing-processor' not found"},
			},
			{
				name: "stream with type",
				node: Node{
					Stream: "test-channel",
					Type:   "sequence",
				},
				errors: []string{"stream node should not have type or ref fields"},
			},
		}

		// Add a valid channel for tests that need it
		channel := make(chan *testData, 10)
		factory.AddChannel("test-channel", channel)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				schema := Schema{Node: tt.node}
				err := factory.ValidateSchema(schema)
				if err == nil {
					t.Error("expected validation error")
					return
				}

				var validationErr ValidationErrors
				if !errors.As(err, &validationErr) {
					t.Errorf("expected ValidationErrors, got %T", err)
					return
				}

				if len(validationErr) != len(tt.errors) {
					t.Errorf("expected %d errors, got %d: %v", len(tt.errors), len(validationErr), validationErr)
					return
				}

				for i, expectedMsg := range tt.errors {
					if validationErr[i].Message != expectedMsg {
						t.Errorf("error %d: expected '%s', got '%s'", i, expectedMsg, validationErr[i].Message)
					}
				}
			})
		}
	})
}

func TestStreamTimeout(t *testing.T) {
	t.Run("stream with timeout succeeds when channel has capacity", func(t *testing.T) {
		factory := New[*testData]()
		channel := make(chan *testData, 10)
		factory.AddChannel("test-channel", channel)

		schema := Schema{
			Node: Node{
				Stream:        "test-channel",
				StreamTimeout: "1s",
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := &testData{value: "test"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result.value != "test" {
			t.Errorf("expected 'test', got '%s'", result.value)
		}

		// Verify channel received data
		select {
		case received := <-channel:
			if received.value != "test" {
				t.Errorf("expected 'test', got '%s'", received.value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("stream with timeout fails when channel is full", func(t *testing.T) {
		factory := New[*testData]()
		channel := make(chan *testData) // unbuffered, will block
		factory.AddChannel("blocked-channel", channel)

		schema := Schema{
			Node: Node{
				Stream:        "blocked-channel",
				StreamTimeout: "50ms",
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx := context.Background()
		input := &testData{value: "test"}

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected timeout error but got none")
		}
		if !contains(err.Error(), "write timeout") {
			t.Errorf("expected timeout error, got: %v", err)
		}
	})

	t.Run("stream without timeout respects context cancellation", func(t *testing.T) {
		factory := New[*testData]()
		channel := make(chan *testData) // unbuffered, will block
		factory.AddChannel("blocked-channel", channel)

		schema := Schema{
			Node: Node{
				Stream: "blocked-channel",
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		input := &testData{value: "test"}

		_, err = pipeline.Process(ctx, input)
		if err == nil {
			t.Fatal("expected context error but got none")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("stream timeout validation", func(t *testing.T) {
		factory := New[*testData]()
		channel := make(chan *testData, 10)
		factory.AddChannel("test-channel", channel)

		schema := Schema{
			Node: Node{
				Stream:        "test-channel",
				StreamTimeout: "invalid",
			},
		}

		err := factory.ValidateSchema(schema)
		if err == nil {
			t.Fatal("expected validation error but got none")
		}
		if !contains(err.Error(), "invalid stream_timeout") {
			t.Errorf("expected stream_timeout validation error, got: %v", err)
		}
	})
}

// contains checks if s contains substr.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (substr == "" || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestStreamIntegration(t *testing.T) {
	t.Run("pipeline ending with stream", func(t *testing.T) {
		factory := New[*testData]()

		// Add processors
		factory.Add(
			pipz.Apply("validate", func(_ context.Context, data *testData) (*testData, error) {
				if data.value == "" {
					return nil, fmt.Errorf("empty value")
				}
				return data, nil
			}),
			pipz.Transform("enrich", func(_ context.Context, data *testData) *testData {
				data.value = "enriched-" + data.value
				return data
			}),
		)

		// Add channel
		channel := make(chan *testData, 10)
		factory.AddChannel("output-channel", channel)

		// Build pipeline that ends with stream
		schema := Schema{
			Node: Node{
				Type: "sequence",
				Children: []Node{
					{Ref: "validate"},
					{Ref: "enrich"},
					{Stream: "output-channel"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		// Process data
		ctx := context.Background()
		input := &testData{value: "test"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Pipeline should return the enriched result
		if result.value != "enriched-test" {
			t.Errorf("expected 'enriched-test', got '%s'", result.value)
		}

		// Check channel output
		select {
		case received := <-channel:
			if received.value != "enriched-test" {
				t.Errorf("expected 'enriched-test', got '%s'", received.value)
			}
		default:
			t.Error("channel didn't receive expected data")
		}
	})

	t.Run("switch routing to different channels", func(t *testing.T) {
		factory := New[*testData]()

		// Add condition
		factory.AddCondition(Condition[*testData]{
			Name: "route-by-value",
			Condition: func(_ context.Context, data *testData) string {
				if data.value == "high" {
					return "high-priority"
				}
				return "low-priority"
			},
		})

		// Add channels
		highChannel := make(chan *testData, 10)
		lowChannel := make(chan *testData, 10)
		factory.AddChannel("high-channel", highChannel)
		factory.AddChannel("low-channel", lowChannel)

		// Build switch that routes to different channels
		schema := Schema{
			Node: Node{
				Type:      "switch",
				Condition: "route-by-value",
				Routes: map[string]Node{
					"high-priority": {Stream: "high-channel"},
					"low-priority":  {Stream: "low-channel"},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		// Process different values
		ctx := context.Background()

		// Send high priority
		highData := &testData{value: "high"}
		_, err = pipeline.Process(ctx, highData)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Send low priority
		lowData := &testData{value: "low"}
		_, err = pipeline.Process(ctx, lowData)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify routing
		select {
		case received := <-highChannel:
			if received.value != "high" {
				t.Errorf("high channel: expected 'high', got '%s'", received.value)
			}
		default:
			t.Error("high channel didn't receive expected data")
		}

		select {
		case received := <-lowChannel:
			if received.value != "low" {
				t.Errorf("low channel: expected 'low', got '%s'", received.value)
			}
		default:
			t.Error("low channel didn't receive expected data")
		}
	})

	t.Run("stream with children continues processing", func(t *testing.T) {
		factory := New[*testData]()

		// Add processors
		factory.Add(
			pipz.Transform("enrich", func(_ context.Context, data *testData) *testData {
				return &testData{value: "enriched-" + data.value}
			}),
			pipz.Transform("finalize", func(_ context.Context, data *testData) *testData {
				return &testData{value: data.value + "-final"}
			}),
		)

		// Add channel
		midChannel := make(chan *testData, 10)
		factory.AddChannel("mid-channel", midChannel)

		// Build pipeline: enrich -> stream (push to channel) -> finalize
		schema := Schema{
			Node: Node{
				Type: "sequence",
				Children: []Node{
					{Ref: "enrich"},
					{
						Stream: "mid-channel",
						Child:  &Node{Ref: "finalize"},
					},
				},
			},
		}

		pipeline, err := factory.Build(schema)
		if err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}

		// Process data
		ctx := context.Background()
		input := &testData{value: "test"}

		result, err := pipeline.Process(ctx, input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Pipeline should continue and return final result
		if result.value != "enriched-test-final" {
			t.Errorf("expected 'enriched-test-final', got '%s'", result.value)
		}

		// Check channel received intermediate result
		// First drain all items from channel to see what we got
		var channelItems []*testData
		for {
			select {
			case received := <-midChannel:
				channelItems = append(channelItems, received)
			default:
				goto checkResults
			}
		}

	checkResults:
		if len(channelItems) != 1 {
			t.Errorf("expected 1 item in channel, got %d: %v", len(channelItems), channelItems)
			return
		}

		if channelItems[0].value != "enriched-test" {
			t.Errorf("channel: expected 'enriched-test', got '%s'", channelItems[0].value)
		}
	})
}
