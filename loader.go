package flume

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zoobzio/pipz"
	"github.com/zoobzio/zlog"
	"gopkg.in/yaml.v3"
)

// BuildFromFile loads a schema from a file and builds the pipeline.
// Supports JSON and YAML formats based on file extension.
func (f *Factory[T]) BuildFromFile(path string) (pipz.Chainable[T], error) {
	data, err := os.ReadFile(path)
	if err != nil {
		zlog.Emit(SchemaFileFailed, "Failed to read schema file",
			zlog.String("path", path),
			zlog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	zlog.Emit(SchemaFileLoaded, "Schema file loaded",
		zlog.String("path", path),
		zlog.Int("size_bytes", len(data)))

	var schema Schema
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &schema); err != nil {
			zlog.Emit(SchemaParseFailed, "Failed to parse JSON schema",
				zlog.String("path", path),
				zlog.String("error", err.Error()))
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		logFields := []zlog.Field{zlog.String("path", path)}
		if schema.Version != "" {
			logFields = append(logFields, zlog.String("version", schema.Version))
		}
		zlog.Emit(SchemaJSONParsed, "JSON schema parsed", logFields...)
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &schema); err != nil {
			zlog.Emit(SchemaParseFailed, "Failed to parse YAML schema",
				zlog.String("path", path),
				zlog.String("error", err.Error()))
			return nil, fmt.Errorf("failed to parse YAML: %w", err)
		}
		logFields := []zlog.Field{zlog.String("path", path)}
		if schema.Version != "" {
			logFields = append(logFields, zlog.String("version", schema.Version))
		}
		zlog.Emit(SchemaYAMLParsed, "YAML schema parsed", logFields...)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", ext)
	}

	return f.Build(schema)
}

// BuildFromJSON creates a pipeline from a JSON string.
func (f *Factory[T]) BuildFromJSON(jsonStr string) (pipz.Chainable[T], error) {
	var schema Schema
	if err := json.Unmarshal([]byte(jsonStr), &schema); err != nil {
		zlog.Emit(SchemaParseFailed, "Failed to parse JSON string",
			zlog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	logFields := []zlog.Field{}
	if schema.Version != "" {
		logFields = append(logFields, zlog.String("version", schema.Version))
	}
	zlog.Emit(SchemaJSONParsed, "JSON string parsed", logFields...)
	return f.Build(schema)
}

// BuildFromYAML creates a pipeline from a YAML string.
func (f *Factory[T]) BuildFromYAML(yamlStr string) (pipz.Chainable[T], error) {
	var schema Schema
	if err := yaml.Unmarshal([]byte(yamlStr), &schema); err != nil {
		zlog.Emit(SchemaParseFailed, "Failed to parse YAML string",
			zlog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}
	logFields := []zlog.Field{}
	if schema.Version != "" {
		logFields = append(logFields, zlog.String("version", schema.Version))
	}
	zlog.Emit(SchemaYAMLParsed, "YAML string parsed", logFields...)
	return f.Build(schema)
}
