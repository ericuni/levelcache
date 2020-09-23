package levelcache

import (
	"reflect"
	"unsafe"
)

// toReadOnlyBytes convert a string to read only bytes
func toReadOnlyBytes(str string) []byte {
	var bs []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	header.Data = (*reflect.StringHeader)(unsafe.Pointer(&str)).Data
	header.Len = len(str)
	header.Cap = len(str)
	return bs
}
