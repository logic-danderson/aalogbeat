package checkpoint

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/elastic/beats/libbeat/logp"
)

// Checkpoint persists log state information to disk.
type Checkpoint struct {
	wg            sync.WaitGroup  // WaitGroup used to wait on the shutdown of the checkpoint worker.
	done          chan struct{}   // Channel for shutting down the checkpoint worker.
	once          sync.Once       // Used to guarantee shutdown happens once.
	file          string          // File where the state is persisted.
	fileLock      sync.RWMutex    // Lock that protects concurrent read/writes to the file.
	numUpdates    int             // Number of updates received since last persisting to disk.
	maxUpdates    int             // Maximum number of updates to buffer before persisting to disk.
	flushInterval time.Duration   // Maximum time interval that can pass before persisting to disk.
	sort          []string        // Slice used for sorting states map (store to save on mallocs).
	log           *logp.Logger    // For logging messages
	lock          sync.RWMutex    // To keep writes in sync
	state         AaLogState      // The state we are keeping
	save          chan AaLogState // Queue of states to save
}

// PersistedState represents the format of the data persisted to disk.
type PersistedState struct {
	UpdateTime time.Time  `yaml:"update_time"`
	State      AaLogState `yaml:"logs"`
}

// AaLogState represents the state of an individual log file.
type AaLogState struct {
	FileName     string `yaml:"file_name"`     // Name of the file being read. Don't need full path as the configuration has the folder.
	RecordNumber uint64 `yaml:"record_number"` // Number of the record last read
	RecordOffset int32  `yaml:"record_offset"` // Byte position of the start of the last record read in the file.
}

// NewCheckpoint creates and returns a new Checkpoint. This method loads state
// information from disk if it exists and starts a goroutine for persisting
// state information to disk. Shutdown should be called when finished to
// guarantee any in-memory state information is flushed to disk.
//
// file is the name of the file where event log state is persisted as YAML.
// maxUpdates is the maximum number of updates checkpoint will accept before
// triggering a flush to disk.
// interval is maximum amount of time that can pass since the last flush before
// triggering a flush to disk (minimum value is 1s).
func NewCheckpoint(file string, maxUpdates int, interval time.Duration) (*Checkpoint, error) {
	log := logp.NewLogger("checkpoint")
	c := &Checkpoint{
		done:          make(chan struct{}),
		file:          file,
		maxUpdates:    maxUpdates,
		flushInterval: interval,
		sort:          make([]string, 0, 10),
		state:         AaLogState{},
		save:          make(chan AaLogState, 1),
		log:           log,
	}

	// Enforce minimums
	if c.maxUpdates < 1 {
		c.maxUpdates = 1
	}
	if c.flushInterval < time.Second {
		c.flushInterval = time.Second
	}

	// Read existing state
	ps, err := c.read()
	if err != nil {
		return nil, err
	}
	c.log.Debugf("Successfully tested reading file %s", c.file)

	if ps != nil {
		c.state = ps.State
	}

	// Write the state file to verify we have permissions.
	err = c.flush()
	if err != nil {
		return nil, err
	}
	c.log.Debugf("Successfully tested flushing to file %s", c.file)

	c.wg.Add(1)
	go c.run()
	return c, nil
}

// run is worker loop that reads incoming state information from the save
// channel and persists it when the number of changes reaches maxEvents or
// the amount of time since the last disk write reaches flushInterval.
func (c *Checkpoint) run() {
	defer c.wg.Done()
	defer c.persist()

	flushTimer := time.NewTimer(c.flushInterval)
	defer flushTimer.Stop()

loop:
	for {
		select {
		case <-c.done:
			break loop
		case s := <-c.save:
			c.lock.Lock()
			c.state = s
			c.lock.Unlock()
			c.numUpdates++
			if c.numUpdates < c.maxUpdates {
				continue
			}
		case <-flushTimer.C:
		}

		c.persist()
		flushTimer.Reset(c.flushInterval)
	}
}

// Shutdown stops the checkpoint worker (which persists any state to disk as
// it stops). This method blocks until the checkpoint worker shutdowns. Calling
// this method more once is safe and has no effect.
func (c *Checkpoint) Shutdown() {
	c.log.Infof("Shutdown called")
	c.once.Do(func() {
		close(c.done)
		c.wg.Wait()
	})
}

// State returns the current in-memory log state. This state information
// is bootstrapped with any data found on disk at creation time.
func (c *Checkpoint) State() AaLogState {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.state
}

// Persist queues the given log state information to be written to disk.
func (c *Checkpoint) Persist(fileName string, recordNumber uint64, recordOffset int32) {
	c.PersistState(AaLogState{
		FileName:     fileName,
		RecordNumber: recordNumber,
		RecordOffset: recordOffset,
	})
}

// PersistState queues the given log state to be written to disk.
func (c *Checkpoint) PersistState(st AaLogState) {
	c.save <- st
}

// persist writes the current state to disk if the in-memory state is dirty
func (c *Checkpoint) persist() bool {
	if c.numUpdates == 0 {
		return false
	}

	err := c.flush()
	if err != nil {
		c.log.Errorf("%v", err)
		return false
	}

	c.log.Debugf("checkpoint", "Checkpoint saved to disk. numUpdates=%d", c.numUpdates)
	c.numUpdates = 0
	return true
}

// flush writes the current state to disk
func (c *Checkpoint) flush() error {
	c.fileLock.Lock()
	defer c.fileLock.Unlock()

	tempFile := c.file + ".new"
	file, err := create(tempFile)
	if os.IsNotExist(err) {
		// Try to create directory if it does not exist.
		if createDirErr := c.createDir(); createDirErr == nil {
			file, err = create(tempFile)
		}
	}

	if err != nil {
		return fmt.Errorf("Failed to flush state to disk. %v", err)
	}

	ps := PersistedState{
		UpdateTime: time.Now().UTC(),
		State:      c.state,
	}

	data, err := yaml.Marshal(ps)
	if err != nil {
		file.Close()
		return fmt.Errorf("Failed to flush state to disk. Could not marshal "+
			"data to YAML. %v", err)
	}

	_, err = file.Write(data)
	if err != nil {
		file.Close()
		return fmt.Errorf("Failed to flush state to disk. Could not write to "+
			"%s. %v", tempFile, err)
	}

	file.Close()
	err = os.Rename(tempFile, c.file)
	return err
}

// read loads the persisted state from disk. If the file does not exist
// then the method returns nil and no error.
func (c *Checkpoint) read() (*PersistedState, error) {
	c.fileLock.RLock()
	defer c.fileLock.RUnlock()

	contents, err := ioutil.ReadFile(c.file)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return nil, err
	}

	ps := &PersistedState{}
	err = yaml.Unmarshal(contents, ps)
	if err != nil {
		return nil, err
	}
	return ps, nil
}

// createDir creates the directory in which the state file will reside
// if the directory does not already exist
func (c *Checkpoint) createDir() error {
	dir := filepath.Dir(c.file)
	c.log.Infof("Creating %s if it does not exist.", dir)
	return os.MkdirAll(dir, os.FileMode(0750))
}
