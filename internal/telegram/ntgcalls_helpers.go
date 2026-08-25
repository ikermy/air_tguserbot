//go:build amd64 && linux

package telegram

import (
	"errors"
)

const ptrSizeBytes = 8

var errNativeAllocUnavailable = errors.New("ntgcalls: native allocator unavailable")

func ntgAllocInt32Slot() (*int32, []func()) {
	ptr, release := ntgNativeAllocZero(4)
	return (*int32)(ptr), []func(){release}
}

func ntgAllocBytePtrSlot() (**byte, func()) {
	ptr, release := ntgNativeAllocZero(ptrSizeBytes)
	return (**byte)(ptr), release
}

func ntgAllocBufferCopy(data []byte) (*byte, func()) {
	ptr, release := ntgNativeAllocBytes(data)
	if ptr == nil {
		return nil, release
	}
	return (*byte)(ptr), release
}
