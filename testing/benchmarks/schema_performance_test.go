//nolint:errcheck // Benchmarks intentionally ignore errors for performance measurement
package benchmarks

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/zoobzio/flume"
	flumetesting "github.com/zoobzio/flume/testing"
	"github.com/zoobzio/pipz"
)

// BenchmarkSchemaParsingYAML measures YAML schema parsing performance.
func BenchmarkSchemaParsingYAML(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 100; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	b.Run("Simple_Ref", func(b *testing.B) {
		schema := "ref: processor-0"

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Sequence_5", func(b *testing.B) {
		schema := `
type: sequence
children:
  - ref: processor-0
  - ref: processor-1
  - ref: processor-2
  - ref: processor-3
  - ref: processor-4
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Sequence_20", func(b *testing.B) {
		var sb strings.Builder
		sb.WriteString("type: sequence\nchildren:\n")
		for i := 0; i < 20; i++ {
			sb.WriteString(fmt.Sprintf("  - ref: processor-%d\n", i))
		}
		schema := sb.String()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Sequence_50", func(b *testing.B) {
		var sb strings.Builder
		sb.WriteString("type: sequence\nchildren:\n")
		for i := 0; i < 50; i++ {
			sb.WriteString(fmt.Sprintf("  - ref: processor-%d\n", i))
		}
		schema := sb.String()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})
}

// BenchmarkSchemaParsingJSON measures JSON schema parsing performance.
func BenchmarkSchemaParsingJSON(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 100; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	b.Run("Simple_Ref", func(b *testing.B) {
		schema := `{"ref": "processor-0"}`

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromJSON(schema)
		}
	})

	b.Run("Sequence_5", func(b *testing.B) {
		schema := `{
			"type": "sequence",
			"children": [
				{"ref": "processor-0"},
				{"ref": "processor-1"},
				{"ref": "processor-2"},
				{"ref": "processor-3"},
				{"ref": "processor-4"}
			]
		}`

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromJSON(schema)
		}
	})

	b.Run("Sequence_20", func(b *testing.B) {
		var sb strings.Builder
		sb.WriteString(`{"type": "sequence", "children": [`)
		for i := 0; i < 20; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(fmt.Sprintf(`{"ref": "processor-%d"}`, i))
		}
		sb.WriteString("]}")
		schema := sb.String()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromJSON(schema)
		}
	})
}

// BenchmarkComplexSchemas measures parsing of complex nested schemas.
func BenchmarkComplexSchemas(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors and predicates
	for i := 0; i < 20; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Identity: factory.Identity("is-valid", "Validates data ID is positive"),
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.ID > 0
		},
	})

	factory.AddCondition(flume.Condition[flumetesting.TestData]{
		Identity: factory.Identity("get-type", "Returns high/low based on value"),
		Condition: func(_ context.Context, d flumetesting.TestData) string {
			if d.Value > 100 {
				return "high"
			}
			return "low"
		},
	})

	b.Run("Filter_Simple", func(b *testing.B) {
		schema := `
type: filter
predicate: is-valid
then:
  ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Filter_With_Else", func(b *testing.B) {
		schema := `
type: filter
predicate: is-valid
then:
  ref: processor-0
else:
  ref: processor-1
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Switch_3_Routes", func(b *testing.B) {
		schema := `
type: switch
condition: get-type
routes:
  high:
    ref: processor-0
  low:
    ref: processor-1
default:
  ref: processor-2
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Retry", func(b *testing.B) {
		schema := `
type: retry
attempts: 3
child:
  ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Timeout", func(b *testing.B) {
		schema := `
type: timeout
duration: "30s"
child:
  ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Fallback", func(b *testing.B) {
		schema := `
type: fallback
children:
  - ref: processor-0
  - ref: processor-1
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Circuit_Breaker", func(b *testing.B) {
		schema := `
type: circuit-breaker
failure_threshold: 5
recovery_timeout: "60s"
child:
  ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Rate_Limit", func(b *testing.B) {
		schema := `
type: rate-limit
requests_per_second: 100.0
burst_size: 10
child:
  ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})
}

// BenchmarkNestedSchemas measures parsing of deeply nested schemas.
func BenchmarkNestedSchemas(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 50; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Identity: factory.Identity("predicate", "Always-true benchmark predicate"),
		Predicate: func(_ context.Context, _ flumetesting.TestData) bool {
			return true
		},
	})

	b.Run("Depth_3", func(b *testing.B) {
		schema := `
type: sequence
children:
  - type: sequence
    children:
      - type: sequence
        children:
          - ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Depth_5", func(b *testing.B) {
		schema := `
type: sequence
children:
  - type: concurrent
    children:
      - type: sequence
        children:
          - type: filter
            predicate: predicate
            then:
              type: sequence
              children:
                - ref: processor-0
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})

	b.Run("Wide_And_Deep", func(b *testing.B) {
		schema := `
type: sequence
children:
  - type: concurrent
    children:
      - ref: processor-0
      - ref: processor-1
      - ref: processor-2
  - type: sequence
    children:
      - ref: processor-3
      - type: filter
        predicate: predicate
        then:
          type: sequence
          children:
            - ref: processor-4
            - ref: processor-5
      - ref: processor-6
  - ref: processor-7
`
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(schema)
		}
	})
}

// BenchmarkSchemaValidation measures schema validation performance.
func BenchmarkSchemaValidation(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 100; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Create schema struct directly
	schema := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Type: "sequence",
			Children: []flume.Node{
				{Ref: "processor-0"},
				{Ref: "processor-1"},
				{Ref: "processor-2"},
			},
		},
	}

	b.Run("ValidateSchema", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.ValidateSchema(schema)
		}
	})

	b.Run("Bind", func(b *testing.B) {
		// Pre-register schema for binding
		_ = factory.SetSchema("bench-schema", schema)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := factory.Identity(fmt.Sprintf("pipeline-%d", i), fmt.Sprintf("Benchmark pipeline %d", i))
			_, _ = factory.Bind(id, "bench-schema")
		}
	})
}

// BenchmarkBindingRetrieval measures binding retrieval performance.
func BenchmarkBindingRetrieval(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 10; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Register schemas and 100 bindings
	bindings := make([]*flume.Binding[flumetesting.TestData], 100)
	for i := 0; i < 100; i++ {
		schema := flume.Schema{
			Version: fmt.Sprintf("%d.0.0", i),
			Node: flume.Node{
				Ref: fmt.Sprintf("processor-%d", i%10),
			},
		}
		schemaID := fmt.Sprintf("bench-schema-%d", i)
		_ = factory.SetSchema(schemaID, schema)
		id := factory.Identity(fmt.Sprintf("schema-%d", i), fmt.Sprintf("Benchmark schema %d", i))
		bindings[i], _ = factory.Bind(id, schemaID)
	}

	id50 := factory.Identity("schema-50", "Benchmark schema 50")
	idMissing := factory.Identity("nonexistent", "Non-existent binding for benchmark")

	b.Run("Get_Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.Get(id50)
		}
	})

	b.Run("Get_Miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.Get(idMissing)
		}
	})

	b.Run("Binding_Process", func(b *testing.B) {
		binding := bindings[50]
		ctx := context.Background()
		input := flumetesting.TestData{ID: 1}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = binding.Process(ctx, input)
		}
	})
}

// BenchmarkYAMLvsJSON compares YAML and JSON parsing performance.
func BenchmarkYAMLvsJSON(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	for i := 0; i < 10; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	yamlSchema := `
type: sequence
children:
  - ref: processor-0
  - ref: processor-1
  - ref: processor-2
  - ref: processor-3
  - ref: processor-4
`

	jsonSchema := `{
		"type": "sequence",
		"children": [
			{"ref": "processor-0"},
			{"ref": "processor-1"},
			{"ref": "processor-2"},
			{"ref": "processor-3"},
			{"ref": "processor-4"}
		]
	}`

	b.Run("YAML", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromYAML(yamlSchema)
		}
	})

	b.Run("JSON", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.BuildFromJSON(jsonSchema)
		}
	})
}

// BenchmarkConcurrentBindingAccess measures concurrent binding operations.
func BenchmarkConcurrentBindingAccess(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	for i := 0; i < 10; i++ {
		id := factory.Identity(fmt.Sprintf("processor-%d", i), fmt.Sprintf("Benchmark processor %d", i))
		factory.Add(pipz.Transform(id, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Pre-register schemas and bindings
	bindings := make([]*flume.Binding[flumetesting.TestData], 50)
	for i := 0; i < 50; i++ {
		schema := flume.Schema{
			Node: flume.Node{Ref: fmt.Sprintf("processor-%d", i%10)},
		}
		schemaID := fmt.Sprintf("parallel-base-schema-%d", i)
		_ = factory.SetSchema(schemaID, schema)
		id := factory.Identity(fmt.Sprintf("schema-%d", i), fmt.Sprintf("Benchmark schema %d", i))
		bindings[i], _ = factory.Bind(id, schemaID)
	}

	// Pre-register a schema for parallel bindings
	parallelSchema := flume.Schema{
		Node: flume.Node{Ref: "processor-0"},
	}
	_ = factory.SetSchema("parallel-shared-schema", parallelSchema)

	id25 := factory.Identity("schema-25", "Benchmark schema 25")
	ctx := context.Background()
	input := flumetesting.TestData{ID: 1}

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(4)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch {
			case i%10 == 0:
				// Write: bind new schema
				id := factory.Identity(fmt.Sprintf("parallel-schema-%d", i), fmt.Sprintf("Parallel schema %d", i))
				_, _ = factory.Bind(id, "parallel-shared-schema")
			case i%5 == 0:
				// Read: get binding
				_ = factory.Get(id25)
			default:
				// Process: use binding
				_, _ = bindings[25].Process(ctx, input)
			}
			i++
		}
	})
}
