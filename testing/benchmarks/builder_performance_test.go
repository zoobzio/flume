//nolint:errcheck // Benchmarks intentionally ignore errors for performance measurement
package benchmarks

import (
	"context"
	"fmt"
	"testing"

	"github.com/zoobzio/flume"
	flumetesting "github.com/zoobzio/flume/testing"
	"github.com/zoobzio/pipz"
)

// BenchmarkBuilderBasic measures basic builder operations.
func BenchmarkBuilderBasic(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 50; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	b.Run("Build_SingleRef", func(b *testing.B) {
		schema := flume.Schema{Node: flume.Node{Ref: "processor-0"}}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Build_Sequence_3", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "processor-0"},
					{Ref: "processor-1"},
					{Ref: "processor-2"},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Build_Sequence_10", func(b *testing.B) {
		children := make([]flume.Node, 10)
		for i := 0; i < 10; i++ {
			children[i] = flume.Node{Ref: fmt.Sprintf("processor-%d", i)}
		}
		schema := flume.Schema{
			Node: flume.Node{
				Type:     "sequence",
				Children: children,
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Build_Concurrent_5", func(b *testing.B) {
		children := make([]flume.Node, 5)
		for i := 0; i < 5; i++ {
			children[i] = flume.Node{Ref: fmt.Sprintf("processor-%d", i)}
		}
		schema := flume.Schema{
			Node: flume.Node{
				Type:     "concurrent",
				Children: children,
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})
}

// BenchmarkBuilderConnectors measures connector-specific build performance.
func BenchmarkBuilderConnectors(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 20; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Register predicate
	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "is-valid",
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.ID > 0
		},
	})

	// Register condition
	factory.AddCondition(flume.Condition[flumetesting.TestData]{
		Name: "get-status",
		Condition: func(_ context.Context, d flumetesting.TestData) string {
			if d.Value > 100 {
				return "high"
			}
			return "low"
		},
	})

	b.Run("Filter", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:      "filter",
				Predicate: "is-valid",
				Then:      &flume.Node{Ref: "processor-0"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Filter_WithElse", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:      "filter",
				Predicate: "is-valid",
				Then:      &flume.Node{Ref: "processor-0"},
				Else:      &flume.Node{Ref: "processor-1"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Switch_3Routes", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:      "switch",
				Condition: "get-status",
				Routes: map[string]flume.Node{
					"high": {Ref: "processor-0"},
					"low":  {Ref: "processor-1"},
				},
				Default: &flume.Node{Ref: "processor-2"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Retry", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:     "retry",
				Attempts: 3,
				Child:    &flume.Node{Ref: "processor-0"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Timeout", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:     "timeout",
				Duration: "30s",
				Child:    &flume.Node{Ref: "processor-0"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Fallback", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "fallback",
				Children: []flume.Node{
					{Ref: "processor-0"},
					{Ref: "processor-1"},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("CircuitBreaker", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:             "circuit-breaker",
				FailureThreshold: 5,
				RecoveryTimeout:  "60s",
				Child:            &flume.Node{Ref: "processor-0"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("RateLimit", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type:              "rate-limit",
				RequestsPerSecond: 100.0,
				BurstSize:         10,
				Child:             &flume.Node{Ref: "processor-0"},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})
}

// BenchmarkBuilderNested measures nested pipeline build performance.
func BenchmarkBuilderNested(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 30; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "pred",
		Predicate: func(_ context.Context, _ flumetesting.TestData) bool {
			return true
		},
	})

	b.Run("Depth_2", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{
						Type: "sequence",
						Children: []flume.Node{
							{Ref: "processor-0"},
							{Ref: "processor-1"},
						},
					},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Depth_4", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{
						Type: "concurrent",
						Children: []flume.Node{
							{
								Type: "sequence",
								Children: []flume.Node{
									{
										Type:      "filter",
										Predicate: "pred",
										Then:      &flume.Node{Ref: "processor-0"},
									},
								},
							},
						},
					},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Wide_3x3", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "concurrent",
				Children: []flume.Node{
					{
						Type: "sequence",
						Children: []flume.Node{
							{Ref: "processor-0"},
							{Ref: "processor-1"},
							{Ref: "processor-2"},
						},
					},
					{
						Type: "sequence",
						Children: []flume.Node{
							{Ref: "processor-3"},
							{Ref: "processor-4"},
							{Ref: "processor-5"},
						},
					},
					{
						Type: "sequence",
						Children: []flume.Node{
							{Ref: "processor-6"},
							{Ref: "processor-7"},
							{Ref: "processor-8"},
						},
					},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("Complex_Mixed", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "processor-0"},
					{
						Type:      "filter",
						Predicate: "pred",
						Then: &flume.Node{
							Type:     "retry",
							Attempts: 3,
							Child: &flume.Node{
								Type:     "timeout",
								Duration: "5s",
								Child:    &flume.Node{Ref: "processor-1"},
							},
						},
					},
					{
						Type: "concurrent",
						Children: []flume.Node{
							{Ref: "processor-2"},
							{Ref: "processor-3"},
						},
					},
					{Ref: "processor-4"},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})
}

// BenchmarkBuilderWithChannels measures build performance with channel operations.
func BenchmarkBuilderWithChannels(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 10; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Register channels
	ch1 := make(chan flumetesting.TestData, 100)
	ch2 := make(chan flumetesting.TestData, 100)
	ch3 := make(chan flumetesting.TestData, 100)

	factory.AddChannel("output-1", ch1)
	factory.AddChannel("output-2", ch2)
	factory.AddChannel("output-3", ch3)

	// Drain channels
	go func() {
		for range ch1 { //nolint:revive // intentionally empty drain loop
		}
	}()
	go func() {
		for range ch2 { //nolint:revive // intentionally empty drain loop
		}
	}()
	go func() {
		for range ch3 { //nolint:revive // intentionally empty drain loop
		}
	}()

	b.Run("SingleStream", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "processor-0"},
					{Stream: "output-1"},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Run("MultipleStreams", func(b *testing.B) {
		schema := flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "processor-0"},
					{
						Type: "concurrent",
						Children: []flume.Node{
							{Stream: "output-1"},
							{Stream: "output-2"},
							{Stream: "output-3"},
						},
					},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Build(schema)
		}
	})

	b.Cleanup(func() {
		close(ch1)
		close(ch2)
		close(ch3)
	})
}

// BenchmarkPipelineExecution measures built pipeline execution performance.
func BenchmarkPipelineExecution(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register lightweight processors
	factory.Add(
		pipz.Transform("increment", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.ID++
			return d
		}),
		pipz.Transform("double", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Value *= 2
			return d
		}),
		pipz.Transform("format", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			d.Name = fmt.Sprintf("item-%d", d.ID)
			return d
		}),
	)

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "non-zero",
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.ID > 0
		},
	})

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1, Name: "test", Value: 10.0}

	b.Run("SingleProcessor", func(b *testing.B) {
		pipeline, _ := factory.Build(flume.Schema{Node: flume.Node{Ref: "increment"}})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})

	b.Run("Sequence_3", func(b *testing.B) {
		pipeline, _ := factory.Build(flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "increment"},
					{Ref: "double"},
					{Ref: "format"},
				},
			},
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})

	b.Run("Filter_Pass", func(b *testing.B) {
		pipeline, _ := factory.Build(flume.Schema{
			Node: flume.Node{
				Type:      "filter",
				Predicate: "non-zero",
				Then:      &flume.Node{Ref: "double"},
			},
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})

	b.Run("Concurrent_3", func(b *testing.B) {
		pipeline, _ := factory.Build(flume.Schema{
			Node: flume.Node{
				Type: "concurrent",
				Children: []flume.Node{
					{Ref: "increment"},
					{Ref: "double"},
					{Ref: "format"},
				},
			},
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})

	b.Run("Complex_Pipeline", func(b *testing.B) {
		pipeline, _ := factory.Build(flume.Schema{
			Node: flume.Node{
				Type: "sequence",
				Children: []flume.Node{
					{Ref: "increment"},
					{
						Type:      "filter",
						Predicate: "non-zero",
						Then: &flume.Node{
							Type: "sequence",
							Children: []flume.Node{
								{Ref: "double"},
								{Ref: "format"},
							},
						},
					},
				},
			},
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})
}

// BenchmarkConcurrentBuilds measures build performance under concurrent access.
func BenchmarkConcurrentBuilds(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 20; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	schema := flume.Schema{
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "processor-0"},
				{Ref: "processor-1"},
				{Ref: "processor-2"},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(4)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = factory.Build(schema)
		}
	})
}

// BenchmarkBuildAndExecute measures combined build and execution overhead.
func BenchmarkBuildAndExecute(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	factory.Add(pipz.Transform("process", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
		d.ID++
		return d
	}))

	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}

	b.Run("Build_Then_Execute", func(b *testing.B) {
		schema := flume.Schema{Node: flume.Node{Ref: "process"}}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pipeline, _ := factory.Build(schema)
			_, _ = pipeline.Process(ctx, input)
		}
	})

	b.Run("Reuse_Built_Pipeline", func(b *testing.B) {
		schema := flume.Schema{Node: flume.Node{Ref: "process"}}
		pipeline, _ := factory.Build(schema)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = pipeline.Process(ctx, input)
		}
	})
}
