package flume

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
	"gopkg.in/yaml.v3"
)

// BuildFromFile loads a schema from a file and builds the pipeline.
// Supports JSON and YAML formats based on file extension.
func (f *Factory[T]) BuildFromFile(path string) (pipz.Chainable[T], error) {
	cleanPath := filepath.Clean(path)
	data, err := os.ReadFile(cleanPath) //nolint:gosec // Path is cleaned; caller is responsible for access control
	if err != nil {
		capitan.Emit(context.Background(), SchemaFileFailed,
			KeyPath.Field(path),
			KeyError.Field(err.Error()))
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	capitan.Emit(context.Background(), SchemaFileLoaded,
		KeyPath.Field(path),
		KeySizeBytes.Field(len(data)))

	var schema Schema
	ext := strings.ToLower(filepath.Ext(cleanPath))

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &schema); err != nil {
			capitan.Emit(context.Background(), SchemaParseFailed,
				KeyPath.Field(path),
				KeyError.Field(err.Error()))
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		fields := []capitan.Field{KeyPath.Field(path)}
		if schema.Version != "" {
			fields = append(fields, KeyVersion.Field(schema.Version))
		}
		capitan.Emit(context.Background(), SchemaJSONParsed, fields...)
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &schema); err != nil {
			capitan.Emit(context.Background(), SchemaParseFailed,
				KeyPath.Field(path),
				KeyError.Field(err.Error()))
			return nil, fmt.Errorf("failed to parse YAML: %w", err)
		}
		fields := []capitan.Field{KeyPath.Field(path)}
		if schema.Version != "" {
			fields = append(fields, KeyVersion.Field(schema.Version))
		}
		capitan.Emit(context.Background(), SchemaYAMLParsed, fields...)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", ext)
	}

	return f.Build(schema)
}

// BuildFromJSON creates a pipeline from a JSON string.
func (f *Factory[T]) BuildFromJSON(jsonStr string) (pipz.Chainable[T], error) {
	var schema Schema
	if err := json.Unmarshal([]byte(jsonStr), &schema); err != nil {
		capitan.Emit(context.Background(), SchemaParseFailed,
			KeyError.Field(err.Error()))
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	fields := []capitan.Field{}
	if schema.Version != "" {
		fields = append(fields, KeyVersion.Field(schema.Version))
	}
	capitan.Emit(context.Background(), SchemaJSONParsed, fields...)
	return f.Build(schema)
}

// BuildFromYAML creates a pipeline from a YAML string.
func (f *Factory[T]) BuildFromYAML(yamlStr string) (pipz.Chainable[T], error) {
	var schema Schema
	if err := yaml.Unmarshal([]byte(yamlStr), &schema); err != nil {
		capitan.Emit(context.Background(), SchemaParseFailed,
			KeyError.Field(err.Error()))
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}
	fields := []capitan.Field{}
	if schema.Version != "" {
		fields = append(fields, KeyVersion.Field(schema.Version))
	}
	capitan.Emit(context.Background(), SchemaYAMLParsed, fields...)
	return f.Build(schema)
}
