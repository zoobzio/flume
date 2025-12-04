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

// BenchmarkFactoryCreation measures factory instantiation overhead.
func BenchmarkFactoryCreation(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		factory := flume.New[flumetesting.TestData]()
		_ = factory
	}
}

// BenchmarkProcessorRegistration measures processor registration performance.
func BenchmarkProcessorRegistration(b *testing.B) {
	b.Run("Single_Add", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			factory := flume.New[flumetesting.TestData]()
			processor := pipz.Transform("test", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
				return d
			})
			factory.Add(processor)
		}
	})

	b.Run("Batch_10", func(b *testing.B) {
		processors := make([]pipz.Chainable[flumetesting.TestData], 10)
		for i := 0; i < 10; i++ {
			name := pipz.Name(fmt.Sprintf("processor-%d", i))
			processors[i] = pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
				return d
			})
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			factory := flume.New[flumetesting.TestData]()
			factory.Add(processors...)
		}
	})

	b.Run("Batch_100", func(b *testing.B) {
		processors := make([]pipz.Chainable[flumetesting.TestData], 100)
		for i := 0; i < 100; i++ {
			name := pipz.Name(fmt.Sprintf("processor-%d", i))
			processors[i] = pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
				return d
			})
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			factory := flume.New[flumetesting.TestData]()
			factory.Add(processors...)
		}
	})

	b.Run("WithMeta", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			factory := flume.New[flumetesting.TestData]()
			meta := flume.ProcessorMeta[flumetesting.TestData]{
				Processor: pipz.Transform("test", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
					return d
				}),
				Description: "A test processor for benchmarking",
				Tags:        []string{"benchmark", "test"},
			}
			factory.AddWithMeta(meta)
		}
	})
}

// BenchmarkPredicateRegistration measures predicate registration performance.
func BenchmarkPredicateRegistration(b *testing.B) {
	b.Run("Single", func(b *testing.B) {
		predicate := flume.Predicate[flumetesting.TestData]{
			Name: "test-predicate",
			Predicate: func(_ context.Context, d flumetesting.TestData) bool {
				return d.ID > 0
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			factory := flume.New[flumetesting.TestData]()
			factory.AddPredicate(predicate)
		}
	})

	b.Run("Batch_10", func(b *testing.B) {
		predicates := make([]flume.Predicate[flumetesting.TestData], 10)
		for i := 0; i < 10; i++ {
			predicates[i] = flume.Predicate[flumetesting.TestData]{
				Name: pipz.Name(fmt.Sprintf("pred-%d", i)),
				Predicate: func(_ context.Context, d flumetesting.TestData) bool {
					return d.ID > 0
				},
			}
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			factory := flume.New[flumetesting.TestData]()
			factory.AddPredicate(predicates...)
		}
	})
}

// BenchmarkConditionRegistration measures condition registration performance.
func BenchmarkConditionRegistration(b *testing.B) {
	condition := flume.Condition[flumetesting.TestData]{
		Name: "test-condition",
		Condition: func(_ context.Context, d flumetesting.TestData) string {
			if d.Value > 100 {
				return "high"
			}
			return "low"
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		factory := flume.New[flumetesting.TestData]()
		factory.AddCondition(condition)
	}
}

// BenchmarkChannelRegistration measures channel registration performance.
func BenchmarkChannelRegistration(b *testing.B) {
	ch := make(chan flumetesting.TestData, 100)
	defer close(ch)

	// Drain channel in background
	go func() {
		for range ch { //nolint:revive // intentionally empty drain loop
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		factory := flume.New[flumetesting.TestData]()
		factory.AddChannel("output", ch)
	}
}

// BenchmarkLookups measures lookup performance for registered components.
func BenchmarkLookups(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register 100 processors
	for i := 0; i < 100; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		processor := pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		})
		factory.Add(processor)
	}

	// Register 50 predicates
	for i := 0; i < 50; i++ {
		factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
			Name: pipz.Name(fmt.Sprintf("predicate-%d", i)),
			Predicate: func(_ context.Context, _ flumetesting.TestData) bool {
				return true
			},
		})
	}

	// Register 25 conditions
	for i := 0; i < 25; i++ {
		factory.AddCondition(flume.Condition[flumetesting.TestData]{
			Name: pipz.Name(fmt.Sprintf("condition-%d", i)),
			Condition: func(_ context.Context, _ flumetesting.TestData) string {
				return "default"
			},
		})
	}

	b.Run("HasProcessor_Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.HasProcessor("processor-50")
		}
	})

	b.Run("HasProcessor_Miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.HasProcessor("nonexistent")
		}
	})

	b.Run("HasPredicate_Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.HasPredicate("predicate-25")
		}
	})

	b.Run("HasCondition_Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.HasCondition("condition-10")
		}
	})

	b.Run("ListProcessors", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.ListProcessors()
		}
	})
}

// BenchmarkRemoval measures component removal performance.
func BenchmarkRemoval(b *testing.B) {
	b.Run("Remove_Single", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			factory := flume.New[flumetesting.TestData]()
			processor := pipz.Transform("to-remove", func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
				return d
			})
			factory.Add(processor)
			b.StartTimer()

			factory.Remove("to-remove")
		}
	})

	b.Run("Remove_From_Large_Registry", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			factory := flume.New[flumetesting.TestData]()

			// Add 100 processors
			for j := 0; j < 100; j++ {
				name := pipz.Name(fmt.Sprintf("processor-%d", j))
				processor := pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
					return d
				})
				factory.Add(processor)
			}
			b.StartTimer()

			factory.Remove("processor-50")
		}
	})
}

// BenchmarkSpec measures factory specification generation performance.
func BenchmarkSpec(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register components
	for i := 0; i < 50; i++ {
		factory.AddWithMeta(flume.ProcessorMeta[flumetesting.TestData]{
			Processor: pipz.Transform(pipz.Name(fmt.Sprintf("proc-%d", i)), func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
				return d
			}),
			Description: fmt.Sprintf("Processor %d description", i),
			Tags:        []string{"benchmark", "test"},
		})
	}

	for i := 0; i < 20; i++ {
		factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
			Name:        pipz.Name(fmt.Sprintf("pred-%d", i)),
			Description: fmt.Sprintf("Predicate %d description", i),
			Predicate: func(_ context.Context, _ flumetesting.TestData) bool {
				return true
			},
		})
	}

	b.Run("Spec", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.Spec()
		}
	})

	b.Run("SpecJSON", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.SpecJSON()
		}
	})
}

// BenchmarkConcurrentRegistration measures registration under concurrent access.
func BenchmarkConcurrentRegistration(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Pre-register some processors
	for i := 0; i < 50; i++ {
		name := pipz.Name(fmt.Sprintf("existing-%d", i))
		processor := pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		})
		factory.Add(processor)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(4)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Mix of reads and writes
			if i%10 == 0 {
				// Write: register new processor
				name := pipz.Name(fmt.Sprintf("parallel-%d", i))
				processor := pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
					return d
				})
				factory.Add(processor)
			} else {
				// Read: check existence
				_ = factory.HasProcessor("existing-25")
			}
			i++
		}
	})
}
