//go:build amd64 && linux

package telegram

import (
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

var (
	ntgLibcOnce sync.Once
	ntgLibcErr  error

	ffiLibcCalloc func(uintptr, uintptr) uintptr
	ffiLibcFree   func(uintptr)
)

func ntgInitNativeAllocator() error {
	ntgLibcOnce.Do(func() {
		candidates := []string{"libc.so.6", "libc.so"}
		for _, name := range candidates {
			h, err := purego.Dlopen(name, purego.RTLD_LAZY)
			if err != nil {
				continue
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						ntgLibcErr = errNativeAllocUnavailable
					}
				}()
				purego.RegisterLibFunc(&ffiLibcCalloc, h, "calloc")
				purego.RegisterLibFunc(&ffiLibcFree, h, "free")
				ntgLibcErr = nil
			}()
			if ntgLibcErr == nil {
				return
			}
			err = purego.Dlclose(h)
			if err != nil {
				ntgLibcErr = errNativeAllocUnavailable
			}
		}
		if ntgLibcErr == nil {
			ntgLibcErr = errNativeAllocUnavailable
		}
	})
	return ntgLibcErr
}

func ntgNativeAllocZero(size uintptr) (unsafe.Pointer, func()) {
	if size == 0 {
		return nil, func() {}
	}
	if err := ntgInitNativeAllocator(); err != nil || ffiLibcCalloc == nil || ffiLibcFree == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0]), func() { _ = b }
	}
	ptr := ffiLibcCalloc(1, size)
	if ptr == 0 {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0]), func() { _ = b }
	}
	return unsafe.Pointer(ptr), func() { ffiLibcFree(ptr) }
}

func ntgNativeAllocBytes(data []byte) (unsafe.Pointer, func()) {
	if len(data) == 0 {
		return nil, func() {}
	}
	ptr, release := ntgNativeAllocZero(uintptr(len(data)))
	copy(unsafe.Slice((*byte)(ptr), len(data)), data)
	return ptr, release
}
