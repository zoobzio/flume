module github.com/zoobzio/flume/examples/data-pipeline/after

go 1.23.1

toolchain go1.24.5

require (
	github.com/zoobzio/flume v0.0.0
	github.com/zoobzio/pipz v0.0.19
)

require (
	github.com/zoobzio/capitan v0.0.9 // indirect
	github.com/zoobzio/clockz v0.0.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/zoobzio/flume => ../../..
