package checkpoint

import (
	"os"
	"syscall"
)

const (
	_FILE_FLAG_WRITE_THROUGH = 0x80000000
)

func create(path string) (*os.File, error) {
	return createWriteThroughFile(path)
}

// createWriteThroughFile creaets a file whose write operations do not go 
// through any intermediary cache, they go directly to disk.
func createWriteThroughFile(path string) (*os.File, error) {
	if len(path) == 0 {
		return nil, syscall.ERROR_FILE_NOT_FOUND
	}

	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	h, err := syscall.CreateFile(
		pathp, // Path
		syscall.GENERIC_READ|syscall.GENERIC_WRITE, // Access mode
		uint32(syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE), // Share mode
		nil, // Security attributes
		syscall.CREATE_ALWAYS, // Create mode
		uint32(syscall.FILE_ATTRIBUTE_NORMAL|_FILE_FLAG_WRITE_THROUGH), // Flags and attributes 
		0) // Template file

	return os.NewFile(uintptr(h), path), err
}