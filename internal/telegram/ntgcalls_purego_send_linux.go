//go:build amd64 && linux

package telegram

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

func ntgResolveSendExternalFrameSymbol(handle uintptr) error {
	sym, err := purego.Dlsym(handle, "ntg_send_external_frame")
	if err != nil {
		return fmt.Errorf("ntg_send_external_frame: %w", err)
	}
	symNtgSendExternalFrame = sym
	return nil
}

func ntgInvokeSendExternalFrame(inst uintptr, userID int64, device uint32, frame *byte, size int32, frameData ntgFrameDataABI, future ntgAsyncABI) int {
	if symNtgSendExternalFrame == 0 {
		panic("ntgcalls: ntg_send_external_frame symbol is not loaded")
	}
	fd0 := *(*uintptr)(unsafe.Pointer(&frameData))
	fd1 := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(&frameData)) + 8))
	fu0 := *(*uintptr)(unsafe.Pointer(&future))
	fu1 := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(&future)) + 8))
	fu2 := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(&future)) + 16))
	fu3 := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(&future)) + 24))
	r1, _, errno := purego.SyscallN(
		symNtgSendExternalFrame,
		inst,
		uintptr(userID),
		uintptr(device),
		uintptr(unsafe.Pointer(frame)),
		uintptr(uint32(size)),
		0, // оставляем R9 пустым: ntg_frame_data_struct должен уходить целиком на stack по SysV ABI.
		fd0,
		fd1,
		fu0,
		fu1,
		fu2,
		fu3,
	)
	runtime.KeepAlive(frame)
	runtime.KeepAlive(frameData)
	runtime.KeepAlive(future)
	ret := int(int32(r1))
	if ret == 0 {
		return 0
	}
	if errno != 0 && ret == 0 {
		return int(int32(errno))
	}
	return ret
}

func ntgValidateSendExternalFramePath() error {
	if symNtgSendExternalFrame == 0 {
		return fmt.Errorf("ntgcalls: ntg_send_external_frame symbol is not loaded")
	}
	return nil
}
