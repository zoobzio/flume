module plugz/benchmarks

go 1.23.1

require (
	plugz v0.0.0
	plugz/examples v0.0.0
)

require (
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)

replace plugz => ../

replace plugz/examples => ../examples
