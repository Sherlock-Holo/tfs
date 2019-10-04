package server

import (
	"syscall"
)

func chroot(path string) error {
	if err := syscall.Chroot(path); err != nil {
		return err
	}

	return syscall.Chdir("/")
}
