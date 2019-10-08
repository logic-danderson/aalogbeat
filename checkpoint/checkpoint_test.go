package checkpoint

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func eventually(t *testing.T, predicate func() (bool, error), timeout time.Duration) {
	const minInterval = time.Millisecond * 5
	const maxInterval = time.Millisecond * 500

	checkInterval := timeout / 100
	if checkInterval < minInterval {
		checkInterval = minInterval
	}
	if checkInterval > maxInterval {
		checkInterval = maxInterval
	}

	for deadline, first := time.Now().Add(timeout), true; first || time.Now().Before(deadline); first = false {
		ok, err := predicate()
		if err != nil {
			t.Fatal("predicate failed with error:", err)
			return
		}
		if ok {
			return
		}
		time.Sleep(checkInterval)
	}
	t.Fatal("predicate is not true after", timeout)
}

// Test that a write is triggered when the maximum number of updates is reached.
func TestWriteMaxUpdates(t *testing.T) {
	dir, err := ioutil.TempDir("", "aalb-checkpoint-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}()

	file := filepath.Join(dir, "some", "new", "dir", ".aalogbeat.yml")
	if !assert.False(t, fileExists(file), "%s should not exist", file) {
		return
	}

	cp, err := NewCheckpoint(file, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Shutdown()

	// Send update - it's not written to disk but it's in memory.
	cp.Persist("file0001.aaLOG", 1, 1001)
	updated := false
	eventually(t, func() (bool, error) {
		state := cp.State()
		if state.FileName == "file0001.aaLOG" {
			updated = true
		}
		return updated, nil
	}, time.Second*15)
	assert.True(t, updated)

	ps, err := cp.read()
	if err != nil {
		t.Fatal("read failed", err)
	}

	// Send update - it is written to disk.
	cp.Persist("file0001.aaLOG", 2, 20012)
	eventually(t, func() (bool, error) {
		ps, err = cp.read()
		return ps != nil && ps.State.RecordNumber == uint64(2), nil
	}, time.Second*15)

	assert.Equal(t, "file0001.aaLOG", ps.State.FileName)
	assert.Equal(t, uint64(2), ps.State.RecordNumber)
	assert.Equal(t, int32(20012), ps.State.RecordOffset)
}

// Test that a write is triggered when the maximum time period since the last
// write is reached.
func TestWriteTimedFlush(t *testing.T) {
	dir, err := ioutil.TempDir("", "aalb-checkpoint-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}()

	file := filepath.Join(dir, "some", "new", "dir", ".aalogbeat.yml")
	if !assert.False(t, fileExists(file), "%s should not exist", file) {
		return
	}

	cp, err := NewCheckpoint(file, 100, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Shutdown()

	// Send update then wait longer than the flush interval and it should be
	// on disk.
	cp.Persist("file0002.aaLOG", 1, 101)
	eventually(t, func() (bool, error) {
		ps, err := cp.read()
		return ps != nil && ps.State.RecordNumber == 1, err
	}, time.Second*15)

	ps, err := cp.read()
	if err != nil {
		t.Fatal("read failed", err)
	}

	assert.Equal(t, "file0002.aaLOG", ps.State.FileName)
	assert.Equal(t, uint64(1), ps.State.RecordNumber)
	assert.Equal(t, int32(101), ps.State.RecordOffset)
}

// Test that createDir creates the directory with 0750 permissions.
func TestCreateDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "aalb-checkpoint-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}()

	stateDir := filepath.Join(dir, "state", "dir", "does", "not", "exist")
	file := filepath.Join(stateDir, ".aalogbeat.yml")

	if !assert.False(t, fileExists(file), "%s should not exist", file) {
		return
	}

	cp, err := NewCheckpoint(file, 100, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Shutdown()

	if !assert.True(t, fileExists(stateDir), "%s should exist", file) {
		return
	}

	// mkdir on Windows does not pass the POSIX mode to the CreateDirectory
	// syscall so doeesn't test the mode
	if runtime.GOOS != "windows" {
		fileInfo, err := os.Stat(stateDir)
		if assert.NoError(t, err) {
			assert.Equal(t, true, fileInfo.IsDir())
			assert.Equal(t, os.FileMode(0750), fileInfo.Mode().Perm())
		}
	}
}

// Test createDir when the directory already exists to verify that no error is
// returned.
func TestCreateDirAlreadyExists(t *testing.T) {
	dir, err := ioutil.TempDir("", "aalb-checkpoint-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}()

	file := filepath.Join(dir, ".aalogbeat.yml")
	cp, err := NewCheckpoint(file, 100, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Shutdown()

	if !assert.True(t, fileExists(dir), "%s should exist", file) {
		return
	}
	assert.NoError(t, cp.createDir())
}

// fileExists returns true if the specified file exists
func fileExists(file string) bool {
	_, err := os.Stat(file)
	return !os.IsNotExist(err)
}
