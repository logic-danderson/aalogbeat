package beater

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/paths"

	"github.com/logic-danderson/aalogbeat/aalog"
	"github.com/logic-danderson/aalogbeat/checkpoint"
	"github.com/logic-danderson/aalogbeat/config"
	cfg "github.com/logic-danderson/aalogbeat/config"
)

// Aalogbeat configuration.
type Aalogbeat struct {
	beat       *beat.Beat             // Common beat information
	config     config.AalogbeatConfig // Configuration settings
	logger     *aaLogger              // Log being monitored
	done       chan struct{}          // Channel to initiate shutdown of main event loop
	pipeline   beat.Pipeline          // Interface to publish event
	checkpoint *checkpoint.Checkpoint // Persists log state to disk
	log        *logp.Logger           // For logging messages
}

// New creates an instance of Aalogbeat.
func New(b *beat.Beat, rawConfig *common.Config) (beat.Beater, error) {
	log := logp.NewLogger("aalogbeat")
	config := cfg.DefaultSettings
	if err := rawConfig.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading configuration file. %v", err)
	}

	// Resolve registry file path
	config.RegistryFile = paths.Resolve(paths.Data, config.RegistryFile)
	log.Info("State will be read from and persisted to %s",
		config.RegistryFile)

	bt := &Aalogbeat{
		beat:   b,
		config: config,
		done:   make(chan struct{}),
		log:    log,
	}

	if err := bt.init(b); err != nil {
		return nil, err
	}

	return bt, nil
}

func (bt *Aalogbeat) init(b *beat.Beat) error {
	// Create the log.
	log, err := aalog.New(bt.config)
	if err != nil {
		return fmt.Errorf("Failed to create new log. %v", err)
	}
	bt.log.Debug("Initialized AaLog %s", log.Name())

	logger, err := newAaLogger(log, bt.beat.BeatConfig)
	if err != nil {
		return fmt.Errorf("Failed to create new log. %v", err)
	}

	bt.logger = logger

	return nil
}

// Setup uses the loaded config and creates necessary markers and environment
// settings to allow the beat to be used.
func (bt *Aalogbeat) setup(b *beat.Beat) error {
	config := &bt.config

	var err error
	bt.checkpoint, err = checkpoint.NewCheckpoint(config.RegistryFile, 10, 5*time.Second)
	if err != nil {
		return err
	}

	bt.pipeline = b.Publisher
	return nil
}

// Run starts aalogbeat.
func (bt *Aalogbeat) Run(b *beat.Beat) error {
	if err := bt.setup(b); err != nil {
		return err
	}

	acker := newEventACKer(bt.checkpoint)
	persistedState := bt.checkpoint.States()

	initMetrics("total")

	err := bt.pipeline.SetACKHandler(beat.PipelineACKHandler{
		ACKEvents: acker.ACKEvents,
	})
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	state, _ := persistedState[bt.logger.source.Name()]
	// Start a goroutine for the log
	wg.Add(1)
	go bt.processLog(&wg, bt.logger, state, acker)

	wg.Wait()
	defer bt.checkpoint.Shutdown()

	if bt.config.ShutdownTimeout > 0 {
		bt.log.Info("Shutdown will wait max %v for the remaining %v events to publish.",
			bt.config.ShutdownTimeout, acker.Active())
		ctx, cancel := context.WithTimeout(context.Background(), bt.config.ShutdownTimeout)
		defer cancel()
		acker.Wait(ctx)
	}

	return nil
}

// Stop stops aalogbeat.
func (bt *Aalogbeat) Stop() {
	bt.log.Info("Stopping Aalogbeat")
	if bt.done != nil {
		close(bt.done)
	}
}

func (bt *Aalogbeat) processLog(
	wg *sync.WaitGroup,
	logger *aaLogger,
	state checkpoint.AaLogState,
	acker *eventACKer,
) {
	defer wg.Done()
	logger.run(bt.done, bt.pipeline, state, acker)
}
