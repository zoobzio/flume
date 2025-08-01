package flume_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/flume"
	"gopkg.in/yaml.v3"
)

func TestSchemaJSONMarshaling(t *testing.T) {
	tests := []struct {
		name   string
		schema flume.Schema
		want   string
	}{
		{
			name: "simple ref schema",
			schema: flume.Schema{
				Node: flume.Node{
					Ref: "test-processor",
				},
			},
			want: `{"ref":"test-processor"}`,
		},
		{
			name: "schema with version",
			schema: flume.Schema{
				Version: "1.0.0",
				Node: flume.Node{
					Ref: "test-processor",
				},
			},
			want: `{"version":"1.0.0","ref":"test-processor"}`,
		},
		{
			name: "complex schema",
			schema: flume.Schema{
				Version: "2.0.0",
				Node: flume.Node{
					Type: "sequence",
					Name: "main",
					Children: []flume.Node{
						{Ref: "step1"},
						{Ref: "step2"},
					},
				},
			},
			want: `{"version":"2.0.0","type":"sequence","name":"main","children":[{"ref":"step1"},{"ref":"step2"}]}`,
		},
		{
			name: "schema with all node fields",
			schema: flume.Schema{
				Node: flume.Node{
					Type:      "filter",
					Name:      "test-filter",
					Predicate: "is-valid",
					Then: &flume.Node{
						Ref: "process",
					},
					Else: &flume.Node{
						Ref: "skip",
					},
				},
			},
			want: `{"type":"filter","name":"test-filter","predicate":"is-valid","then":{"ref":"process"},"else":{"ref":"skip"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.schema)
			if err != nil {
				t.Fatalf("Failed to marshal schema: %v", err)
			}

			if string(data) != tt.want {
				t.Errorf("JSON mismatch\ngot:  %s\nwant: %s", string(data), tt.want)
			}

			// Test unmarshaling back
			var unmarshaled flume.Schema
			if err := json.Unmarshal(data, &unmarshaled); err != nil {
				t.Fatalf("Failed to unmarshal schema: %v", err)
			}

			// Re-marshal to verify round trip
			data2, err := json.Marshal(unmarshaled)
			if err != nil {
				t.Fatalf("Failed to re-marshal schema: %v", err)
			}

			if string(data2) != tt.want {
				t.Errorf("Round trip failed\ngot:  %s\nwant: %s", string(data2), tt.want)
			}
		})
	}
}

func TestSchemaYAMLMarshaling(t *testing.T) {
	tests := []struct {
		name   string
		schema flume.Schema
		want   string
	}{
		{
			name: "simple ref schema",
			schema: flume.Schema{
				Node: flume.Node{
					Ref: "test-processor",
				},
			},
			want: "ref: test-processor\n",
		},
		{
			name: "schema with version",
			schema: flume.Schema{
				Version: "1.0.0",
				Node: flume.Node{
					Ref: "test-processor",
				},
			},
			want: "version: 1.0.0\nref: test-processor\n",
		},
		{
			name: "sequence schema",
			schema: flume.Schema{
				Node: flume.Node{
					Type: "sequence",
					Children: []flume.Node{
						{Ref: "step1"},
						{Ref: "step2"},
					},
				},
			},
			want: `type: sequence
children:
    - ref: step1
    - ref: step2
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(tt.schema)
			if err != nil {
				t.Fatalf("Failed to marshal schema: %v", err)
			}

			if string(data) != tt.want {
				t.Errorf("YAML mismatch\ngot:\n%s\nwant:\n%s", string(data), tt.want)
			}

			// Test unmarshaling back
			var unmarshaled flume.Schema
			if err := yaml.Unmarshal(data, &unmarshaled); err != nil {
				t.Fatalf("Failed to unmarshal schema: %v", err)
			}

			// Verify key fields
			if tt.schema.Version != "" && unmarshaled.Version != tt.schema.Version {
				t.Errorf("Version mismatch: got %s, want %s", unmarshaled.Version, tt.schema.Version)
			}
			if unmarshaled.Node.Ref != tt.schema.Node.Ref {
				t.Errorf("Ref mismatch: got %s, want %s", unmarshaled.Node.Ref, tt.schema.Node.Ref)
			}
			if unmarshaled.Node.Type != tt.schema.Node.Type {
				t.Errorf("Type mismatch: got %s, want %s", unmarshaled.Node.Type, tt.schema.Node.Type)
			}
		})
	}
}

func TestNodeFieldsOmitEmpty(t *testing.T) {
	// Test that optional fields are omitted when empty
	node := flume.Node{
		Ref: "test",
	}

	data, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	// Should only contain ref field
	want := `{"ref":"test"}`
	if string(data) != want {
		t.Errorf("Expected only ref field, got: %s", string(data))
	}

	// Test with type instead
	node = flume.Node{
		Type: "sequence",
		Children: []flume.Node{
			{Ref: "child"},
		},
	}

	data, err = json.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	// Should only contain type and children
	want = `{"type":"sequence","children":[{"ref":"child"}]}`
	if string(data) != want {
		t.Errorf("Expected only type and children fields, got: %s", string(data))
	}
}

func TestSchemaYAMLInline(t *testing.T) {
	// Test that Node fields are inlined in Schema YAML
	schema := flume.Schema{
		Version: "1.0.0",
		Node: flume.Node{
			Type: "sequence",
			Name: "test",
			Children: []flume.Node{
				{Ref: "proc1"},
			},
		},
	}

	data, err := yaml.Marshal(schema)
	if err != nil {
		t.Fatalf("Failed to marshal schema: %v", err)
	}

	// Should have fields at top level, not nested under "node"
	yamlStr := string(data)
	if containsYAML(yamlStr, "node:") {
		t.Errorf("YAML should not contain 'node:' field due to inline tag")
	}
	if !containsYAML(yamlStr, "version:") {
		t.Errorf("YAML should contain 'version:' field")
	}
	if !containsYAML(yamlStr, "type:") {
		t.Errorf("YAML should contain 'type:' field at top level")
	}
	if !containsYAML(yamlStr, "name:") {
		t.Errorf("YAML should contain 'name:' field at top level")
	}
}

func TestNodeRoutes(t *testing.T) {
	// Test switch node with routes
	node := flume.Node{
		Type:      "switch",
		Condition: "router",
		Routes: map[string]flume.Node{
			"a": {Ref: "route-a"},
			"b": {Ref: "route-b"},
		},
		Default: &flume.Node{
			Ref: "default-route",
		},
	}

	// Test JSON marshaling
	jsonData, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	var jsonNode flume.Node
	if err := json.Unmarshal(jsonData, &jsonNode); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if len(jsonNode.Routes) != 2 {
		t.Errorf("Expected 2 routes, got %d", len(jsonNode.Routes))
	}
	if jsonNode.Routes["a"].Ref != "route-a" {
		t.Errorf("Expected route 'a' to have ref 'route-a'")
	}
	if jsonNode.Default == nil || jsonNode.Default.Ref != "default-route" {
		t.Errorf("Expected default route with ref 'default-route'")
	}

	// Test YAML marshaling
	yamlData, err := yaml.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal node to YAML: %v", err)
	}

	var yamlNode flume.Node
	if err := yaml.Unmarshal(yamlData, &yamlNode); err != nil {
		t.Fatalf("Failed to unmarshal YAML: %v", err)
	}

	if len(yamlNode.Routes) != 2 {
		t.Errorf("Expected 2 routes in YAML, got %d", len(yamlNode.Routes))
	}
}

func TestNodeDurationFields(t *testing.T) {
	// Test retry with backoff
	node := flume.Node{
		Type:     "retry",
		Attempts: 3,
		Backoff:  "100ms",
		Child: &flume.Node{
			Ref: "processor",
		},
	}

	// Test JSON round trip
	jsonData, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	var jsonNode flume.Node
	if err := json.Unmarshal(jsonData, &jsonNode); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if jsonNode.Attempts != 3 {
		t.Errorf("Expected attempts=3, got %d", jsonNode.Attempts)
	}
	if jsonNode.Backoff != "100ms" {
		t.Errorf("Expected backoff='100ms', got '%s'", jsonNode.Backoff)
	}

	// Test timeout with duration
	node = flume.Node{
		Type:     "timeout",
		Duration: "5s",
		Child: &flume.Node{
			Ref: "slow-processor",
		},
	}

	jsonData, err = json.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal timeout node: %v", err)
	}

	if err := json.Unmarshal(jsonData, &jsonNode); err != nil {
		t.Fatalf("Failed to unmarshal timeout JSON: %v", err)
	}

	if jsonNode.Duration != "5s" {
		t.Errorf("Expected duration='5s', got '%s'", jsonNode.Duration)
	}
}

// Helper to check if YAML contains a line starting with the given prefix.
func containsYAML(yaml, prefix string) bool {
	lines := splitLines(yaml)
	for _, line := range lines {
		trimmed := trimLeft(line)
		if hasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func trimLeft(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' && s[i] != '\t' {
			return s[i:]
		}
	}
	return ""
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
