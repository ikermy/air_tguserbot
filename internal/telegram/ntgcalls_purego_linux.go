//go:build amd64 && linux

package telegram

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ebitengine/purego"
)

func openNtgCallsLibrary() (uintptr, error) {
	candidates := []string{
		"./libntgcalls.so",
		"./slim_libntgcalls.so",
	}

	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "libntgcalls.so"),
			filepath.Join(exeDir, "slim_libntgcalls.so"),
		)
	}

	candidates = append(candidates, "libntgcalls.so", "slim_libntgcalls.so")

	for _, name := range candidates {
		h, err := purego.Dlopen(name, purego.RTLD_LAZY)
		if err == nil {
			return h, nil
		}
	}
	return 0, fmt.Errorf("ntgcalls: unable to load Linux library (tried ./libntgcalls.so, executable dir, and bare name)")
}

func closeNtgCallsLibrary(handle uintptr) error {
	return purego.Dlclose(handle)
}
