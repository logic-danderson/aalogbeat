package aalog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

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
func (l aaLogging) Name() string {
	return filepath.Base(l.filePath)
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

	// All aaLogging has at this point is the directory to monitor.
	// checkpoint.AaLogState might have a file name.
	// If a file name is specified in state, try to read it.
	// Otherwise, get the most recent log file.
	filePath := ""
	if state.FileName != "" {
		filePath = filepath.Join(l.directory, state.FileName)
	} else {
		filePath, err = l.getLatestLogFilePath()
		if err != nil {
			return err
		}
	}
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
			// Read from the beginning.
			l.recordNumber = 0
			l.recordOffset = 0
		}
	} else {
		// Read from the beginning.
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
	if logRecords == nil {
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
	l.log.Debugf("Close called")
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

func (l *aaLogging) getLatestLogFilePath() (string, error) {
	l.log.Infof("Looking for most recent log file, directory:%s", l.directory)

	if len(l.headers) == 0 {
		return "", fmt.Errorf("No log files found, directory:%s", l.directory)
	}

	// It's important that l.headers be sorted so that the last entry
	// is always the most recent
	header := l.headers[len(l.headers)-1]
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
