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
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
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
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
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
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "is-valid",
		Predicate: func(_ context.Context, d flumetesting.TestData) bool {
			return d.ID > 0
		},
	})

	factory.AddCondition(flume.Condition[flumetesting.TestData]{
		Name: "get-type",
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
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	factory.AddPredicate(flume.Predicate[flumetesting.TestData]{
		Name: "predicate",
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
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
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

	b.Run("SetSchema", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.SetSchema(fmt.Sprintf("pipeline-%d", i), schema)
		}
	})
}

// BenchmarkSchemaRetrieval measures schema retrieval performance.
func BenchmarkSchemaRetrieval(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	// Register processors
	for i := 0; i < 10; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Register 100 schemas
	for i := 0; i < 100; i++ {
		schema := flume.Schema{
			Version: fmt.Sprintf("%d.0.0", i),
			Node: flume.Node{
				Ref: fmt.Sprintf("processor-%d", i%10),
			},
		}
		_ = factory.SetSchema(fmt.Sprintf("schema-%d", i), schema)
	}

	b.Run("GetSchema_Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.GetSchema("schema-50")
		}
	})

	b.Run("GetSchema_Miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.GetSchema("nonexistent")
		}
	})

	b.Run("Pipeline_Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Pipeline("schema-50")
		}
	})

	b.Run("ListSchemas", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = factory.ListSchemas()
		}
	})
}

// BenchmarkYAMLvsJSON compares YAML and JSON parsing performance.
func BenchmarkYAMLvsJSON(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	for i := 0; i < 10; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
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

// BenchmarkConcurrentSchemaAccess measures concurrent schema operations.
func BenchmarkConcurrentSchemaAccess(b *testing.B) {
	factory := flume.New[flumetesting.TestData]()

	for i := 0; i < 10; i++ {
		name := pipz.Name(fmt.Sprintf("processor-%d", i))
		factory.Add(pipz.Transform(name, func(_ context.Context, d flumetesting.TestData) flumetesting.TestData {
			return d
		}))
	}

	// Pre-register some schemas
	for i := 0; i < 50; i++ {
		schema := flume.Schema{
			Node: flume.Node{Ref: fmt.Sprintf("processor-%d", i%10)},
		}
		_ = factory.SetSchema(fmt.Sprintf("schema-%d", i), schema)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(4)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch {
			case i%10 == 0:
				// Write: set new schema
				schema := flume.Schema{
					Node: flume.Node{Ref: fmt.Sprintf("processor-%d", i%10)},
				}
				_ = factory.SetSchema(fmt.Sprintf("parallel-schema-%d", i), schema)
			case i%5 == 0:
				// Read: list schemas
				_ = factory.ListSchemas()
			default:
				// Read: get pipeline
				_, _ = factory.Pipeline("schema-25")
			}
			i++
		}
	})
}
