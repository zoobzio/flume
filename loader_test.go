package flume_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zoobzio/flume"
	"github.com/zoobzio/pipz"
)

func TestBuildFromFile(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "flume-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	factory := flume.New[TestData]()
	factory.Add(pipz.Transform("test-processor", func(_ context.Context, d TestData) TestData {
		d.Value = "file-processed"
		return d
	}))

	tests := []struct {
		name          string
		fileName      string
		content       string
		expectError   bool
		errorContains string
	}{
		{
			name:     "valid JSON file",
			fileName: "schema.json",
			content:  `{"ref": "test-processor"}`,
		},
		{
			name:     "valid JSON with version",
			fileName: "versioned.json",
			content:  `{"version": "1.0.0", "ref": "test-processor"}`,
		},
		{
			name:     "valid YAML file",
			fileName: "schema.yaml",
			content:  "ref: test-processor",
		},
		{
			name:     "valid YML file",
			fileName: "schema.yml",
			content:  "ref: test-processor",
		},
		{
			name:     "valid YAML with version",
			fileName: "versioned.yaml",
			content: `version: "2.0.0"
ref: test-processor`,
		},
		{
			name:     "complex JSON schema",
			fileName: "complex.json",
			content: `{
				"type": "sequence",
				"children": [
					{"ref": "test-processor"}
				]
			}`,
		},
		{
			name:     "complex YAML schema",
			fileName: "complex.yaml",
			content: `type: sequence
children:
  - ref: test-processor`,
		},
		{
			name:          "invalid JSON",
			fileName:      "invalid.json",
			content:       `{invalid json}`,
			expectError:   true,
			errorContains: "failed to parse JSON",
		},
		{
			name:          "invalid YAML",
			fileName:      "invalid.yaml",
			content:       `[invalid: yaml`,
			expectError:   true,
			errorContains: "failed to parse YAML",
		},
		{
			name:          "unsupported file type",
			fileName:      "schema.txt",
			content:       "ref: test-processor",
			expectError:   true,
			errorContains: "unsupported file format: .txt",
		},
		{
			name:          "non-existent file",
			fileName:      "doesnotexist.json",
			content:       "", // won't be created
			expectError:   true,
			errorContains: "failed to read file",
		},
		{
			name:          "missing processor in JSON",
			fileName:      "missing.json",
			content:       `{"ref": "non-existent"}`,
			expectError:   true,
			errorContains: "processor 'non-existent' not found",
		},
		{
			name:          "missing processor in YAML",
			fileName:      "missing.yaml",
			content:       "ref: non-existent",
			expectError:   true,
			errorContains: "processor 'non-existent' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, tt.fileName)

			// Create file if content is provided (skip for non-existent file test)
			if tt.content != "" {
				if err := os.WriteFile(filePath, []byte(tt.content), 0o600); err != nil {
					t.Fatalf("Failed to create test file: %v", err)
				}
			}

			pipeline, err := factory.BuildFromFile(filePath)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Test the pipeline works
			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Pipeline error: %v", pErr)
			}

			if result.Value != "file-processed" {
				t.Errorf("Expected 'file-processed', got '%s'", result.Value)
			}
		})
	}
}

func TestBuildFromJSON(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("json-proc", func(_ context.Context, d TestData) TestData {
			d.Value = "json-result"
			return d
		}),
		pipz.Transform("step1", func(_ context.Context, d TestData) TestData {
			d.Value += "_1"
			return d
		}),
		pipz.Transform("step2", func(_ context.Context, d TestData) TestData {
			d.Value += "_2"
			return d
		}),
	)

	tests := []struct {
		name          string
		json          string
		expectedValue string
		expectError   bool
		errorContains string
	}{
		{
			name:          "simple processor reference",
			json:          `{"ref": "json-proc"}`,
			expectedValue: "json-result",
		},
		{
			name:          "schema with version",
			json:          `{"version": "1.0.0", "ref": "json-proc"}`,
			expectedValue: "json-result",
		},
		{
			name: "sequence schema",
			json: `{
				"type": "sequence",
				"children": [
					{"ref": "step1"},
					{"ref": "step2"}
				]
			}`,
			expectedValue: "_1_2",
		},
		{
			name: "nested schema",
			json: `{
				"type": "timeout",
				"duration": "1s",
				"child": {
					"type": "sequence",
					"children": [
						{"ref": "json-proc"},
						{"ref": "step1"}
					]
				}
			}`,
			expectedValue: "json-result_1",
		},
		{
			name:          "invalid JSON syntax",
			json:          `{invalid json}`,
			expectError:   true,
			errorContains: "failed to parse JSON",
		},
		{
			name:          "empty JSON",
			json:          `{}`,
			expectError:   true,
			errorContains: "empty node - must have either ref, type, or stream",
		},
		{
			name:          "missing processor",
			json:          `{"ref": "non-existent"}`,
			expectError:   true,
			errorContains: "processor 'non-existent' not found",
		},
		{
			name: "invalid schema structure",
			json: `{
				"type": "sequence"
			}`,
			expectError:   true,
			errorContains: "sequence requires at least one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.BuildFromJSON(tt.json)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Pipeline error: %v", pErr)
			}

			if result.Value != tt.expectedValue {
				t.Errorf("Expected '%s', got '%s'", tt.expectedValue, result.Value)
			}
		})
	}
}

func TestBuildFromYAML(t *testing.T) {
	factory := flume.New[TestData]()

	factory.Add(
		pipz.Transform("yaml-proc", func(_ context.Context, d TestData) TestData {
			d.Value = "yaml-result"
			return d
		}),
		pipz.Transform("step1", func(_ context.Context, d TestData) TestData {
			d.Value += "_1"
			return d
		}),
		pipz.Transform("step2", func(_ context.Context, d TestData) TestData {
			d.Value += "_2"
			return d
		}),
	)

	factory.AddPredicate(flume.Predicate[TestData]{
		Name: "is-empty",
		Predicate: func(_ context.Context, d TestData) bool {
			return d.Value == ""
		},
	})

	tests := []struct {
		name          string
		yaml          string
		expectedValue string
		expectError   bool
		errorContains string
	}{
		{
			name:          "simple processor reference",
			yaml:          "ref: yaml-proc",
			expectedValue: "yaml-result",
		},
		{
			name: "schema with version",
			yaml: `version: "1.0.0"
ref: yaml-proc`,
			expectedValue: "yaml-result",
		},
		{
			name: "sequence schema",
			yaml: `type: sequence
children:
  - ref: step1
  - ref: step2`,
			expectedValue: "_1_2",
		},
		{
			name: "complex nested schema",
			yaml: `type: filter
predicate: is-empty
then:
  ref: yaml-proc
else:
  type: sequence
  children:
    - ref: step1
    - ref: step2`,
			expectedValue: "yaml-result", // is-empty is true with empty input, so then branch
		},
		{
			name: "schema with all node types",
			yaml: `type: sequence
name: main-pipeline
children:
  - ref: yaml-proc
  - type: retry
    attempts: 3
    child:
      ref: step1`,
			expectedValue: "yaml-result_1",
		},
		{
			name:          "invalid YAML syntax",
			yaml:          "[invalid: yaml",
			expectError:   true,
			errorContains: "failed to parse YAML",
		},
		{
			name:          "empty YAML",
			yaml:          "",
			expectError:   true,
			errorContains: "empty node - must have either ref, type, or stream",
		},
		{
			name:          "missing processor",
			yaml:          "ref: non-existent",
			expectError:   true,
			errorContains: "processor 'non-existent' not found",
		},
		{
			name:          "invalid schema structure",
			yaml:          "type: sequence",
			expectError:   true,
			errorContains: "sequence requires at least one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := factory.BuildFromYAML(tt.yaml)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got '%s'", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Pipeline error: %v", pErr)
			}

			if result.Value != tt.expectedValue {
				t.Errorf("Expected '%s', got '%s'", tt.expectedValue, result.Value)
			}
		})
	}
}

func TestLoaderEdgeCases(t *testing.T) {
	factory := flume.New[TestData]()

	t.Run("BuildFromFile with uppercase extensions", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "flume-test-uppercase-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)

		factory.Add(pipz.Transform("test", func(_ context.Context, d TestData) TestData {
			d.Value = "uppercase"
			return d
		}))

		// Test uppercase extensions
		for _, ext := range []string{".JSON", ".YAML", ".YML"} {
			filePath := filepath.Join(tempDir, "schema"+ext)
			content := `{"ref": "test"}`
			if ext != ".JSON" {
				content = "ref: test"
			}

			if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			pipeline, err := factory.BuildFromFile(filePath)
			if err != nil {
				t.Errorf("Failed to build from %s file: %v", ext, err)
				continue
			}

			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}
			if result.Value != "uppercase" {
				t.Errorf("Expected 'uppercase', got '%s'", result.Value)
			}
		}
	})

	t.Run("BuildFromFile with mixed case extensions", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "flume-test-mixedcase-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)

		// Test mixed case
		filePath := filepath.Join(tempDir, "schema.JsOn")
		if err := os.WriteFile(filePath, []byte(`{"ref": "test"}`), 0o600); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		pipeline, err := factory.BuildFromFile(filePath)
		if err != nil {
			t.Errorf("Failed to build from mixed case extension: %v", err)
		} else {
			ctx := context.Background()
			result, pErr := pipeline.Process(ctx, TestData{})
			if pErr != nil {
				t.Fatalf("Process error: %v", pErr)
			}
			if result.Value != "uppercase" {
				t.Errorf("Expected 'uppercase', got '%s'", result.Value)
			}
		}
	})

	t.Run("File read permission error", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "flume-test-perms-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)

		filePath := filepath.Join(tempDir, "noperms.json")
		if err := os.WriteFile(filePath, []byte(`{"ref": "test"}`), 0o000); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		_, err = factory.BuildFromFile(filePath)
		if err == nil {
			t.Error("Expected error for unreadable file")
		} else if !contains(err.Error(), "failed to read file") {
			t.Errorf("Expected 'failed to read file' error, got: %v", err)
		}
	})
}

// Helper function to check if string contains substring.
func contains(s, substr string) bool {
	if substr == "" {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
