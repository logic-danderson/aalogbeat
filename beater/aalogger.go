package beater

import (
	"io"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"

	"github.com/logic-danderson/aalogbeat/aalog"
	"github.com/logic-danderson/aalogbeat/checkpoint"
)

type aaLogger struct {
	source     aalog.AaLog
	eventMeta  common.EventMetadata
	processors beat.ProcessorList
}

type aaLoggerConfig struct {
	common.EventMetadata `config:",inline"`      // Fields and tags to add to records
	Processors           processors.PluginConfig `config:"processors"`
}

func newAaLogger(source aalog.AaLog, options *common.Config) (*aaLogger, error) {
	config := aaLoggerConfig{}
	if err := options.Unpack(&config); err != nil {
		return nil, err
	}

	processors, err := processors.New(config.Processors)
	if err != nil {
		return nil, err
	}

	return &aaLogger{
		source:     source,
		eventMeta:  config.EventMetadata,
		processors: processors,
	}, nil
}

func (l *aaLogger) connect(pipeline beat.Pipeline) (beat.Client, error) {
	filePath := l.source.Name()
	return pipeline.ConnectWith(beat.ClientConfig{
		PublishMode: beat.GuaranteedSend,
		Processing: beat.ProcessingConfig{
			EventMetadata: l.eventMeta,
			Meta:          nil,
			Processor:     l.processors,
		},
		ACKCount: func(n int) {
			addPublished(filePath, n)
			logp.Info("AaLog %s successfully published %d events", filePath, n)
		},
	})
}

func (l *aaLogger) run(
	done <-chan struct{},
	pipeline beat.Pipeline,
	state checkpoint.AaLogState,
	acker *eventACKer,
) {
	log := l.source

	initMetrics(log.Name())

	client, err := l.connect(pipeline)
	if err != nil {
		logp.Warn("Aalog %s Pipeline error. Failed to connect to publisher pipeline",
			log.Name())
		return
	}

	// Close client on function return or when 'done' is triggered (unblock client)
	defer client.Close()
	go func() {
		<-done
		client.Close()
	}()

	err = log.Open(state)
	if err != nil {
		logp.Warn("Aalog %s Open() error, No records will be read from "+
			"this source. %v", log.Name(), err)
		return
	}

	defer func() {
		logp.Info("Aalog %s Stop processing.", log.Name())

		if err := log.Close(); err != nil {
			logp.Warn("Aalog %s Close() error. %v", log.Name(), err)
			return
		}
	}()

	logp.Debug("Aalog %s opened successfully", log.Name())

	for stop := false; !stop; {
		select {
		case <-done:
			return
		default:
		}

		records, err := log.Read()
		switch err {
		case nil:
		case io.EOF:
			stop = true
		default:
			logp.Warn("Aalog %s Read() error: %v", log.Name(), err)
			return
		}

		logp.Debug("Aalog %s Read() returned %d records", log.Name(), len(records))
		if len(records) == 0 {
			time.Sleep(time.Second)
			continue
		}

		acker.Add(len(records))
		for _, lr := range records {
			client.Publish(lr.ToEvent())
		}
	}
}
