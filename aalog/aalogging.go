package aalog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/elastic/beats/libbeat/logp"

	"github.com/logic-danderson/aalogbeat/checkpoint"
	"github.com/logic-danderson/aalogbeat/config"
)

// Validate that aaLogging implements the AaLog interface.
var _ AaLog = &aaLogging{}

// aaLogging implements the AaLog interface for reading from the
// log files.
type aaLogging struct {
	config    config.AalogbeatConfig // The configuration
	directory string                 // Directory containing the log files
	filePath  string                 // Full path to the file to open
	header    LogHeader              // Header of the current log file
	headers   []LogHeader            // List of known LogHeader ordered by firstRecordTime ascending
	lastRead  checkpoint.AaLogState  // Last read record state
	log       *logp.Logger           // Logger

	recordNumber uint64 // Last record number read
	recordOffset int32  // Byte offset of last record read
}

// Name returns the log file name
func (l *aaLogging) Name() string {
	return filepath.Base(l.filePath)
}

// State returns the most recent AaLogState.
func (l *aaLogging) State() checkpoint.AaLogState {
	return l.lastRead
}

// New creates and returns a new AaLog for reading logs
func New(c config.AalogbeatConfig) (AaLog, error) {
	log := logp.NewLogger("aalog")
	return &aaLogging{
		config:    c,
		directory: c.Directory,
		log:       log,
	}, nil
}

// Sets up the initial state for reading records. Does not keep the
// log file open.
func (l *aaLogging) Open(state checkpoint.AaLogState) error {
	err := l.cacheHeaders()
	if err != nil {
		return err
	}

	// adjustState potentially does a lot of work.
	// One way or the other, state should point to
	// where we should pick back up reading logs.
	state, err = l.adjustState(state)
	if err != nil {
		return err
	}
	l.lastRead = state

	filePath := filepath.Join(l.directory, state.FileName)
	filePath, err = filepath.Abs(filePath)
	if err != nil {
		return err
	}
	l.filePath = filePath

	// Verify we can open the file and read the header.
	l.log.Infof("Open filePath:%s, recordNumber:%d, recordOffset:%d", l.filePath, state.RecordNumber, state.RecordOffset)
	file, err := os.Open(l.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	header, err := l.readLogHeader(file)
	l.log.Debugf("Open header file:%s, mc:%d, frn:%d, fro:%d, frt:%v, lrn:%d, lro:%d, lrt:%v",
		l.filePath, header.messageCount, header.firstRecordNumber, header.firstRecordOffset, header.firstRecordTime,
		header.lastRecordNumber(), header.lastRecordOffset, header.lastRecordTime)
	if err != nil {
		return err
	}
	l.header = header

	var firstNumber, lastNumber uint64
	if header.messageCount > 0 {
		l.recordNumber = state.RecordNumber
		l.recordOffset = state.RecordOffset
		firstNumber = header.firstRecordNumber
		lastNumber = header.lastRecordNumber()

		if l.recordNumber < firstNumber || l.recordNumber > lastNumber {
			// Read from the end of the log.
			l.recordNumber = header.lastRecordNumber()
			l.recordOffset = header.lastRecordOffset
		}
	} else {
		// The log has no messages yet, so read from the beginning.
		l.recordNumber = 0
		l.recordOffset = 0
	}

	l.log.Infof("Open, last file:%s, record count:%d, record number range [%d, %d]. Starting "+
		"at record number %d, offset %d", l.filePath, header.messageCount,
		firstNumber, lastNumber, l.recordNumber, l.recordOffset)

	return nil
}

// Reads records from the current log file. Will attempt to move
// to the next log file and include its records, too, if there is
// a next log file, when it reaches the end of the current log file.
func (l *aaLogging) Read() ([]Record, error) {
	l.log.Debugf("Read file:%s", l.filePath)

	logRecords, err := l.readLogRecords(int32(0))
	if err != nil {
		return nil, err
	}
	if logRecords == nil || len(logRecords) == 0 {
		l.log.Debugf("Read returned zero records")
		return nil, nil
	}

	records := make([]Record, len(logRecords))
	for i, lr := range logRecords {
		r := Record{
			LogRecord: lr,
			Offset: checkpoint.AaLogState{
				FileName:     lr.file,
				RecordNumber: lr.number,
				RecordOffset: lr.offset,
			},
		}
		records[i] = r
		l.lastRead = r.Offset
		l.recordNumber = r.number
		l.recordOffset = r.offset
	}

	l.log.Debugf("Read record count:%d, first record number:%d, last record number:%d, last file read:%s",
		len(records), records[0].number, records[len(records)-1].number, l.filePath)
	return records, nil
}

// The log file is closed each time it is read so
// this is just a placeholder.
func (l *aaLogging) Close() error {
	l.log.Infof("Close called")
	return nil
}

// Creates a cache of log headers from scratch.
func (l *aaLogging) cacheHeaders() error {
	l.log.Infof("Reading headers from all of the log files, directory:%s", l.directory)
	files, err := l.getLogFiles()
	if err != nil {
		return err
	}

	l.headers = []LogHeader{}
	for _, filePath := range files {
		h, err := readLogHeaderFromFile(filePath)
		if err != nil {
			return err
		}
		l.log.Debugf("Caching header, file:%s", h.file)
		l.headers = append(l.headers, h)
	}

	if len(l.headers) == 0 {
		return fmt.Errorf("Unable to find any log files in directory:%s, pattern:%s", l.directory, l.config.FilePattern)
	}

	l.sortHeaders()

	l.log.Infof("Read headers from log files, directory:%s, file count:%d", l.directory, len(l.headers))
	return nil
}

func (l *aaLogging) getLogFiles() ([]string, error) {
	directoryPath, err := filepath.Abs(l.directory)
	if err != nil {
		return nil, err
	}

	pattern := filepath.Join(directoryPath, l.config.FilePattern)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	return files, nil
}

func (l *aaLogging) sortHeaders() {
	// Sort by firstRecordTime ascending.
	sort.Slice(l.headers, func(i, j int) bool {
		return l.headers[i].firstRecordTime.Before(l.headers[j].firstRecordTime)
	})
}

// adjustState verifies we have a valid state and if so simply returns it.
// Otherwise it returns an updated state based on the backfill settings.
func (l *aaLogging) adjustState(state checkpoint.AaLogState) (checkpoint.AaLogState, error) {
	loadLatest := false

	if state.FileName != "" && state.RecordNumber > 0 && state.RecordOffset > 0 {
		canRead, err := l.canReadFileFromState(state)
		if err != nil {
			return checkpoint.AaLogState{}, err
		}

		if canRead {
			// The state should be usable as-is assuming that file still exists.
			return state, nil
		}
		// If canRead is false then that file doesn't exist.
		l.log.Warnf("Unable to read the log specified in the current state, will use the most recent log instead, invalid state file:%s, number:%d, offset:%d", state.FileName, state.RecordNumber, state.RecordOffset)
		loadLatest = true
	}

	if !loadLatest && l.config.BackfillEnabled {
		// If both backfill start and duration are set, time wins.
		if l.config.BackfillStart != "" {
			newState, err := l.getStateForBackfillStart()
			if err != nil {
				return checkpoint.AaLogState{}, err
			}

			return newState, nil
		}

		if l.config.BackfillDuration > 0 {
			newState, err := l.getStateForBackfillDuration()
			if err != nil {
				return checkpoint.AaLogState{}, err
			}

			return newState, nil
		}
	}

	// If we got here, then either we can't load the file specified in
	// the state, backfill is not enabled, or we could not find the
	// appropriate file from which to start the backfill. So start from
	// the most recent record.
	newState, err := l.getStateForLastRecord()
	if err != nil {
		return checkpoint.AaLogState{}, err
	}

	return newState, nil
}

// Returns true if the file and parameters specified in the given state
// are still readable and valid. Otherwise returns false.
func (l *aaLogging) canReadFileFromState(state checkpoint.AaLogState) (bool, error) {
	l.log.Debugf("Verifying state parameters, file:%s, number:%d, offset:%d", state.FileName, state.RecordNumber, state.RecordOffset)

	filePath := filepath.Join(l.directory, state.FileName)
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return false, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		// We won't treat the file not existing anymore as an error.
		// Just return false.
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	header, err := l.readLogHeader(file)
	if err != nil {
		return false, err
	}

	if state.RecordNumber < header.firstRecordNumber || state.RecordNumber > header.lastRecordNumber() {
		return false, nil
	}

	if state.RecordOffset < header.firstRecordOffset || state.RecordOffset > header.lastRecordOffset {
		return false, nil
	}

	_, err = readLogRecord(file, state.RecordNumber, state.RecordOffset)
	if err != nil {
		l.log.Errorf("Failed to read record specified in state from file:%s, number:%d, offset:%d, error:%v", state.FileName, state.RecordNumber, state.RecordOffset, err)
		return false, nil
	}

	l.log.Debugf("Successfully verified state parameters, file:%s, number:%d, offset:%d", state.FileName, state.RecordNumber, state.RecordOffset)
	return true, nil
}

// Returns state as if the last record in the most recent log file has
// already been read.
func (l *aaLogging) getStateForLastRecord() (checkpoint.AaLogState, error) {
	header, err := l.getLatestHeader()
	if err != nil {
		return checkpoint.AaLogState{}, err
	}

	state := checkpoint.AaLogState{
		FileName:     header.file,
		RecordNumber: header.lastRecordNumber(),
		RecordOffset: header.lastRecordOffset,
	}
	l.log.Infof("Using state for last record logged, file:%s, number:%d, offset:%d", state.FileName, state.RecordNumber, state.RecordOffset)
	return state, nil
}

// Returns the LogHeader for the most recent log from the header cache.
// Assumes the header cache is sorted so the last entry is the most recent.
func (l *aaLogging) getLatestHeader() (LogHeader, error) {
	l.log.Infof("Looking for most recent log file, directory:%s", l.directory)

	if len(l.headers) == 0 {
		return LogHeader{}, fmt.Errorf("No log files found, directory:%s", l.directory)
	}

	header := l.headers[len(l.headers)-1]
	l.log.Infof("Most recent log file:%s, first record time:%v, first record number:%d", header.file, header.firstRecordTime, header.firstRecordNumber)
	return header, nil
}

// Returns a checkpoint.AaLogState as if the most recent record before the
// configured backfill start time has been read.
func (l *aaLogging) getStateForBackfillStart() (checkpoint.AaLogState, error) {
	backfillStart, err := time.ParseInLocation(time.RFC3339, l.config.BackfillStart, time.Local)
	if err != nil {
		return checkpoint.AaLogState{}, fmt.Errorf("backfill_start has an invalid time value:'%s'. The value should be in RFC3339 format.", l.config.BackfillStart)
	}

	l.log.Infof("Looking for log to backfill from a start time, directory:%s, time:%v", l.directory, backfillStart)

	state, err := l.getStateForStartTime(backfillStart)
	if err != nil {
		return checkpoint.AaLogState{}, err
	}

	return state, nil
}

func (l *aaLogging) getStateForStartTime(recordTime time.Time) (checkpoint.AaLogState, error) {
	header, err := l.getLatestHeaderBeforeTime(recordTime)
	if err != nil {
		return checkpoint.AaLogState{}, err
	}

	if header.firstRecordTime.IsZero() {
		// We did not find a log header from before the record time.
		// So look for the first header after the record time.
		header, err = l.getFirstHeaderAfterTime(recordTime)
		if err != nil {
			return checkpoint.AaLogState{}, err
		}
	}

	if header.firstRecordTime.IsZero() {
		// We were unable to find any logs.
		return checkpoint.AaLogState{}, nil
	}

	// Now need to find the last record before the start time.
	record, err := l.getLastRecordBeforeTime(header, recordTime)
	if err != nil {
		return checkpoint.AaLogState{}, err
	}

	state := checkpoint.AaLogState{
		FileName:     header.file,
		RecordNumber: record.number,
		RecordOffset: record.offset,
	}
	l.log.Infof("Found state to backfill from a start time:%v, file:%s, number:%d, offset:%d, record time:%v", recordTime, header.file, record.number, record.offset, record.recordTime)
	return state, nil
}

// Returns the LogHeader that has the most recent record before the given time
// or returns an empty LogHeader if none can be found.
// Assumes the header cache is sorted so the last entry is the most recent.
func (l *aaLogging) getLatestHeaderBeforeTime(recordTime time.Time) (LogHeader, error) {
	l.log.Infof("Looking for header containing a record before time value, directory:%s, time:%v", l.directory, recordTime)

	if len(l.headers) == 0 {
		return LogHeader{}, fmt.Errorf("No log files found, directory:%s", l.directory)
	}

	header := LogHeader{}
	for _, h := range l.headers {
		if h.firstRecordTime.After(recordTime) {
			// All of the headers after this one should also
			// be after the given time. So we should have
			// already found a match.
			break
		}

		header = h
	}
	return header, nil
}

// Returns the LogHeader that has the most oldest record after the given time
// or returns an empty LogHeader if none can be found.
// Assumes the header cache is sorted so the last entry is the most recent.
func (l *aaLogging) getFirstHeaderAfterTime(recordTime time.Time) (LogHeader, error) {
	l.log.Infof("Looking for header containing a record before time value, directory:%s, time:%v", l.directory, recordTime)

	if len(l.headers) == 0 {
		return LogHeader{}, fmt.Errorf("No log files found, directory:%s", l.directory)
	}

	header := LogHeader{}
	for i := len(l.headers) - 1; i >= 0; i-- {
		h := l.headers[i]
		if h.lastRecordTime.Before(recordTime) {
			// All of the headers after this one should also
			// be before the given time. So we should have
			// already found a match.
			break
		}

		header = h
	}
	return header, nil
}

// Returns a checkpoint.AaLogState as if the most recent record before the
// configured backfill duration has been read.
func (l *aaLogging) getStateForBackfillDuration() (checkpoint.AaLogState, error) {
	l.log.Infof("Looking for log to backfill from a duration, directory:%s, duration:%v", l.directory, l.config.BackfillDuration)

	backfillStart := time.Now().Add(-l.config.BackfillDuration)

	state, err := l.getStateForStartTime(backfillStart)
	if err != nil {
		return checkpoint.AaLogState{}, err
	}

	return state, nil
}

// Returns the latest record in the given log before the given time.
func (l *aaLogging) getLastRecordBeforeTime(header LogHeader, recordTime time.Time) (LogRecord, error) {
	filePath := filepath.Join(l.directory, header.file)
	filePath, err := filepath.Abs(filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return LogRecord{}, err
	}
	defer file.Close()

	record := LogRecord{}
	number := header.firstRecordNumber
	offset := header.firstRecordOffset
	for i := 0; i < int(header.messageCount); i++ {
		r, err := readLogRecord(file, number, offset)
		if err != nil {
			return LogRecord{}, err
		}

		if r.recordTime.After(recordTime) {
			// All records after this one should also be after the given time.
			// So we should have already found the correct record.
			break
		}

		record = r
		offset = r.offsetToNextRecord
		number++
	}
	return record, nil
}

// Returns the relative file path to the most recent log file.
func (l *aaLogging) getLatestLogFilePath() (string, error) {
	header, err := l.getLatestHeader()
	if err != nil {
		return "", err
	}
	filePath := filepath.Join(l.directory, header.file)
	l.log.Infof("Most recent log file:%s", filePath)
	return filePath, nil
}

func (l *aaLogging) readLogHeader(file *os.File) (LogHeader, error) {
	l.log.Debugf("Read log header, file:%s", file.Name())
	return readLogHeader(file)
}

// Reads records from the current log file. Can be called recusively to
// read from consecutive log files, where currentCount is the number of
// records read so far.
func (l *aaLogging) readLogRecords(currentCount int32) ([]LogRecord, error) {
	file, err := os.Open(l.filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Re-read the header because it was probably updated.
	header, err := l.readLogHeader(file)
	if err != nil {
		return nil, err
	}

	l.header = header
	err = l.updateHeaderInCache(header)
	if err != nil {
		return nil, err
	}

	recordCount, recordNumber, offset, err := l.getStartingRecordStats(file)
	if err != nil {
		return nil, err
	}

	// If we zeros back then we don't know what's going on.
	if recordCount == 0 || recordNumber == 0 || offset == 0 {
		if recordNumber > 0 && recordCount == 0 {
			l.log.Debugf("No new records to read, record count:0")
			return nil, nil
		} else {
			l.log.Warnf("Unable to get record count, record number, or offset for first record to read, file:%s", file.Name())
			return nil, nil
		}
	}

	// In enforcing the maximum batch size, take the number of records we
	// have already read into account.
	batchSize := int32(l.config.BatchSize) - currentCount
	if recordCount > batchSize {
		recordCount = batchSize
	}

	l.log.Debugf("Reading records, expecting count:%d", recordCount)
	records := make([]LogRecord, recordCount)
	for i := 0; i < len(records); i++ {
		record, err := readLogRecord(file, recordNumber, offset)
		if err != nil {
			return nil, err
		}
		records[i] = record
		offset = record.offsetToNextRecord
		recordNumber++
	}

	// If we've read the batch size number of records then return them.
	if recordCount >= batchSize {
		return records, nil
	}

	// If we've read less than the full batch size then check if we
	// can switch to a new log file.
	l.log.Debugf("Read less than maximum records, checking for new log files, directory:%s, record count:%d", l.directory, recordCount)
	nextHeader, isNext, err := l.getNextHeader(l.header)
	if err != nil {
		if recordCount > 0 {
			// Log the error but return the records we have.
			l.log.Errorf("Error calling getNextHeader, returning records that could be read, error:%v", err)
			return records, nil
		}
		// Something is majorly broken.
		return nil, err
	}

	if isNext {
		// Move to the next log file.
		file.Close()

		l.header = nextHeader
		l.filePath = filepath.Join(l.directory, nextHeader.file)
		l.recordNumber = 0
		l.recordOffset = 0

		l.log.Infof("Switching to next log, file:%s", l.filePath)
		nextRecords, err := l.readLogRecords(recordCount + currentCount)
		if err != nil {
			// Log the error but return the records we have.
			l.log.Errorf("Error calling getNextHeader, returning records that could be read, error:%v", err)
			return records, nil
		}

		if len(nextRecords) > 0 {
			records = append(records, nextRecords...)
		}
	}

	return records, nil
}

// Finds the given header in the cache and updates it.
func (l *aaLogging) updateHeaderInCache(header LogHeader) error {
	l.log.Debugf("Updating header for file in cache, file:%s", header.file)
	_, pos, err := l.getHeaderForFileInCache(header.file)
	if err != nil {
		return err
	}

	if pos < 0 {
		return fmt.Errorf("Could not find header for file in the cache, file:%s", header.file)
	}

	l.headers[pos] = header
	return nil
}

// Figures out where we should start reading records, returning the
// expected record count, the first record number, and its offset.
func (l *aaLogging) getStartingRecordStats(file *os.File) (int32, uint64, int32, error) {
	count := int32(0)
	number := uint64(0)
	offset := int32(0)
	if l.recordNumber == 0 || l.recordOffset == 0 {
		l.log.Debugf("No record number nor offset, read records from beginning of log, file:%s", file.Name())
		count = l.header.messageCount
		number = l.header.firstRecordNumber
		offset = l.header.firstRecordOffset
	} else {
		// Read the records starting after the last one read.
		r, err := readLogRecord(file, l.recordNumber, l.recordOffset)
		if err != nil {
			return count, number, offset, err
		}

		number = l.recordNumber + 1
		offset = r.offsetToNextRecord
		lastRecordNumber := l.header.lastRecordNumber()
		count = int32(lastRecordNumber - l.recordNumber)
		l.log.Debugf("Reading records starting after last record read, file:%s, count:%d, prev number:%d, prev offset:%d, next number:%d, next offset:%d", file.Name(), count, l.recordNumber, l.recordOffset, number, offset)
	}
	return count, number, offset, nil
}

func (l *aaLogging) getNextHeader(currentHeader LogHeader) (LogHeader, bool, error) {
	l.log.Infof("Looking for next header, directory:%s, after:%v", l.directory, currentHeader.firstRecordTime)
	_, headerPos, err := l.getHeaderForFileInCache(currentHeader.file)
	if err != nil {
		return LogHeader{}, false, err
	}

	if headerPos < 0 {
		return LogHeader{}, false, fmt.Errorf("Could not find header for file in the cache, file:%s", currentHeader.file)
	}

	if headerPos < len(l.headers)-1 {
		// Return the next header in the list.
		return l.headers[headerPos+1], true, nil
	}

	// We must already be at the end.
	// See if we have any new log files.
	added, err := l.updateHeadersCache()
	if err != nil {
		return LogHeader{}, false, err
	}

	if !added {
		// No new log files were found.
		return LogHeader{}, false, nil
	}

	// Try finding the next header again. The order of the headers in the
	// cache may have changed, so search for the current header again.
	_, headerPos, err = l.getHeaderForFileInCache(currentHeader.file)
	if err != nil {
		return LogHeader{}, false, err
	}

	if headerPos < 0 {
		return LogHeader{}, false, fmt.Errorf("Could not find header for file in the cache, file:%s", currentHeader.file)
	}

	if headerPos < len(l.headers)-1 {
		// Return the next header in the list.
		return l.headers[headerPos+1], true, nil
	}

	// At this point, we must be at the most recent header already.
	return LogHeader{}, false, nil
}

// Looks for the header for a file in the headers cache and returns
// both the header and its position in the cache. If the file is not
// cached returns an empty header and a position of -1.
func (l *aaLogging) getHeaderForFileInCache(file string) (LogHeader, int, error) {
	l.log.Debugf("Getting header for file:%s", file)
	if len(l.headers) == 0 {
		return LogHeader{}, -1, fmt.Errorf("No log files found, directory:%s", l.directory)
	}

	// Most likely looking for a recent header, so start at the
	// end and work backwards.
	for i := len(l.headers) - 1; i >= 0; i-- {
		if l.headers[i].file == file {
			return l.headers[i], i, nil
		}
	}

	return LogHeader{}, -1, nil
}

// Adds any new log headers to the existing headers cache.
// Returns true if any new headers were added.
func (l *aaLogging) updateHeadersCache() (bool, error) {
	if l.headers == nil || len(l.headers) == 0 {
		// Haven't run cacheHeaders yet? Just do that.
		err := l.cacheHeaders()
		if err != nil {
			return false, err
		}
		return true, nil
	}

	l.log.Infof("Reading headers from any new log files, directory:%s", l.directory)
	files, err := l.getLogFiles()
	if err != nil {
		return false, err
	}

	headerAdded := false
	for _, filePath := range files {
		file := filepath.Base(filePath)
		_, pos, err := l.getHeaderForFileInCache(file)
		if err != nil {
			return headerAdded, err
		}

		if pos > 0 {
			// This file has already been cached.
			continue
		}

		h, err := readLogHeaderFromFile(filePath)
		if err != nil {
			return headerAdded, err
		}
		l.headers = append(l.headers, h)
		headerAdded = true
	}

	if headerAdded {
		l.sortHeaders()
	}

	return headerAdded, nil
}
