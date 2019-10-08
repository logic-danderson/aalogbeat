package aalog

import (
	"expvar"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"

	"github.com/logic-danderson/aalogbeat/checkpoint"
)

var (
	// dropReasons contains counters for the number of dropped events for each reason.
	dropReasons = expvar.NewMap("drop_reasons")

	// readErrors contains counters for the read error types that occur.
	readErrors = expvar.NewMap("read_errors")
)

type AaLog interface {
	// Open the event log. state points to the last successfully read event
	// in this event log. Read will resume from the next record. To start reading
	// from the first event specify a zero-valued AaLogState.
	Open(state checkpoint.AaLogState) error

	// Read records from the log. If io.EOF is returned you should stop reading
	// and close the log.
	Read() ([]Record, error)

	// Close the log. It should not be re-opened after closing.
	Close() error

	// Name returns the log's name.
	Name() string
}

type LogHeader struct {
	length            int32
	messageCount      int32
	firstRecordNumber uint64
	firstRecordOffset int32
	firstRecordTime   time.Time
	lastRecordOffset  int32
	lastRecordTime    time.Time
	computerName      string
	session           string
	file              string
	previousFile      string
}

func (h *LogHeader) lastRecordNumber() uint64 {
	return h.firstRecordNumber + uint64(h.messageCount) - 1
}

type LogRecord struct {
	length                 int32
	number                 uint64
	offset                 int32
	offsetToPreviousRecord int32
	offsetToNextRecord     int32
	sessionId              string
	processId              uint32
	threadId               uint32
	recordTime             time.Time
	logFlag                string
	component              string
	message                string
	processName            string
	file                   string
}

type Record struct {
	LogRecord
	Offset checkpoint.AaLogState // Position of the record within its source stream.
}

// ToEvent returns a new MapStr containing the data from this LogRecord
func (r Record) ToEvent() beat.Event {
	rec := common.MapStr{
		"number":       r.number,
		"session_id":   r.sessionId,
		"process_id":   r.processId,
		"process_name": r.processName,
		"thread_id":    r.threadId,
		"log_flag":     r.logFlag,
		"component":    r.component,
		"message":      r.message,
	}

	fields := common.MapStr{
		"aalog": rec,
	}

	return beat.Event{
		Timestamp: r.recordTime,
		Fields:    fields,
		Private:   r.Offset,
	}
}

// incrementMetric increments a value in the specified expvar.Map. The key
// should be a a string. Any other types will be reported under the "other" key.
func incrementMetric(v *expvar.Map, key interface{}) {
	switch t := key.(type) {
	default:
		v.Add("other", 1)
	case string:
		v.Add(t, 1)
	}
}
