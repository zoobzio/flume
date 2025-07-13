package plugz

import (
	"fmt"
	"reflect"
	"sync"
)

var typeCache sync.Map // map[reflect.Type]string

func getTypeName(t reflect.Type) string {
	if name, ok := typeCache.Load(t); ok {
		return name.(string)
	}
	name := t.String()
	typeCache.Store(t, name)
	return name
}

func generateSignature[T any, K comparable](key K) string {
	keyType := reflect.TypeOf(key)
	valueType := reflect.TypeOf((*T)(nil)).Elem()
	
	// Cached type names - reflection only on first use
	keyTypeName := getTypeName(keyType)
	valueTypeName := getTypeName(valueType)
	
	return fmt.Sprintf("%s:%v:%s", keyTypeName, key, valueTypeName)
}