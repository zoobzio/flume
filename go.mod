module plugz

go 1.23.1

require (
	github.com/vmihailenco/msgpack/v5 v5.4.1
	github.com/zoobzio/pipz v0.0.1
	github.com/zoobzio/sctx v0.0.0
)

require github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect

replace github.com/zoobzio/pipz => ../pipz

replace github.com/zoobzio/sctx => ../sctx
