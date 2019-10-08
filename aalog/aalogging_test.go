package aalog

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/logic-danderson/aalogbeat/checkpoint"
	"github.com/logic-danderson/aalogbeat/config"
)

func init() {
	rand.Seed(time.Now().Unix())
}

var oneTimeLogpInit sync.Once

type teardown func()

// Initializes logp if the verbose flag was set.
func configureLogp() {
	oneTimeLogpInit.Do(func() {
		if testing.Verbose() {
			logp.DevelopmentSetup(logp.WithSelectors("aalog"))
			logp.Info("DEBUG enabled for aalog.")
		} else {
			logp.DevelopmentSetup(logp.WithLevel(logp.WarnLevel))
		}
	})
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func deleteFileIfExists(filePath string) error {
	exists, err := pathExists(filePath)
	if err != nil {
		return err
	}
	if exists {
		err = os.Remove(filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func ensureDirectoryExists(directory string) error {
	exists, err := pathExists(directory)
	if err != nil {
		return err
	}
	if !exists {
		err = os.MkdirAll(directory, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeDataToFile(filePath string, data []byte) error {
	err := ensureDirectoryExists(filepath.Dir(filePath))
	if err != nil {
		return err
	}
	err = deleteFileIfExists(filePath)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)

	return err
}

// toBytes converts the LogHeader to []byte for writing to a file.
// This func does update some properties on the given LogHeader:
// length: set to the length of the []byte
// firstRecordOffset: set to length
// lastRecordOffset: set to length, only if it's not already set
func (h *LogHeader) toBytes() []byte {
	data := make([]byte, 56)

	// Don't know header length yet.
	writeUint64(data[20:], h.firstRecordNumber)
	writeInt32(data[28:], h.messageCount)
	writeTime(data[32:], h.firstRecordTime)
	writeTime(data[40:], h.lastRecordTime)
	// Don't know first record offset yet.
	// Don't know last record offset yet.

	data = append(data, stringToBytes(h.computerName)...)
	data = append(data, stringToBytes(h.session)...)
	data = append(data, stringToBytes(h.previousFile)...)

	h.length = int32(len(data))
	h.firstRecordOffset = h.length
	if h.lastRecordOffset == 0 {
		h.lastRecordOffset = h.length
	}
	writeInt32(data[8:], h.length)
	writeInt32(data[48:], h.firstRecordOffset)
	writeInt32(data[52:], h.lastRecordOffset)

	return data
}

func writeInt32(data []byte, value int32) {
	writeUint32(data, uint32(value))
}

func writeUint32(data []byte, value uint32) {
	binary.LittleEndian.PutUint32(data, value)
}

func writeInt64(data []byte, value int64) {
	writeUint64(data, uint64(value))
}

func writeUint64(data []byte, value uint64) {
	binary.LittleEndian.PutUint64(data, value)
}

func writeTime(data []byte, value time.Time) {
	unixTime := uint64(value.UnixNano())
	num := int64((unixTime + 11644473600000000000) / 100)
	writeInt64(data, num)
}

func writeSessionId(data []byte, value string) {
	parts := strings.Split(value, ".")
	i, _ := strconv.Atoi(parts[3])
	data[0] = byte(i)
	i, _ = strconv.Atoi(parts[2])
	data[1] = byte(i)
	i, _ = strconv.Atoi(parts[1])
	data[2] = byte(i)
	i, _ = strconv.Atoi(parts[0])
	data[3] = byte(i)
}

func stringToBytes(value string) []byte {
	codes := utf16.Encode([]rune(value))
	bytes := make([]byte, len(codes)*2)
	for i, code := range codes {
		bytes[i*2] = byte(code)
		bytes[i*2+1] = byte(code >> 8)
	}
	return append(bytes, 0, 0)
}

// toBytes converts the LogRecord to []byte for writing to a file.
// This func does update some properties on the given LogRecord:
// length: set to the length of the []byte
// offsetToNextRecord: set to the given offset + length of this record
// offset is the byte position where this LogRecord data starts in the file.
// offsetToPrevious is the byte position of the previous LogRecord in the file.
func (r *LogRecord) toBytes(offset int32, offsetToPrevious int32) []byte {
	data := make([]byte, 32)

	// Don't know record length yet.
	writeInt32(data[8:], offsetToPrevious)
	writeSessionId(data[12:], r.sessionId)
	writeUint32(data[16:], r.processId)
	writeUint32(data[20:], r.threadId)
	writeTime(data[24:], r.recordTime)

	data = append(data, stringToBytes(r.logFlag)...)
	data = append(data, stringToBytes(r.component)...)
	data = append(data, stringToBytes(r.message)...)
	data = append(data, stringToBytes(r.processName)...)

	r.length = int32(len(data))
	r.offset = offset
	r.offsetToPreviousRecord = offsetToPrevious
	r.offsetToNextRecord = offset + r.length
	writeInt32(data[4:], r.length)

	return data
}

func getDataForFile(h *LogHeader, records []LogRecord) []byte {
	var data []byte

	// Have to generate the header's bytes first because the
	// offset is based on the header size.
	data = append(data, h.toBytes()...)

	// Now we can get the data for the records.
	offset := h.length
	offsetPrevious := int32(0)
	if records != nil {
		for i := range records {
			r := &records[i]
			data = append(data, r.toBytes(offset, offsetPrevious)...)
			offsetPrevious = offset
			offset += r.length
		}
	}

	// Now that we know the position of all of the records,
	// go back to the header and update the last record offset
	h.lastRecordOffset = offsetPrevious
	// Thought about just recreating all of the header bytes
	// which for encapulation purposes is probably more proper.
	// But this is more efficient.
	writeInt32(data[52:], h.lastRecordOffset)

	return data
}

func writeTestLogFile(filePath string, h *LogHeader, records []LogRecord) error {
	data := getDataForFile(h, records)
	return writeDataToFile(filePath, data)
}

func assertRecordsMatch(t *testing.T, expected LogRecord, actual Record) {
	assert.Equal(t, expected.file, actual.file, "file")
	assert.Equal(t, expected.length, actual.length, "length")
	assert.Equal(t, expected.offset, actual.offset, "offset")
	assert.Equal(t, expected.offsetToPreviousRecord, actual.offsetToPreviousRecord, "offsetToPreviousRecord")
	assert.Equal(t, expected.offsetToNextRecord, actual.offsetToNextRecord, "offsetToNextRecord")
	assert.Equal(t, expected.sessionId, actual.sessionId, "sessionId")
	assert.Equal(t, expected.processId, actual.processId, "processId")
	assert.Equal(t, expected.threadId, actual.threadId, "threadId")
	assert.Equal(t, expected.recordTime.Round(time.Millisecond), actual.recordTime.Round(time.Millisecond), "recordTime")
	assert.Equal(t, expected.logFlag, actual.logFlag, "logFlag")
	assert.Equal(t, expected.component, actual.component, "component")
	assert.Equal(t, expected.message, actual.message, "message")
	assert.Equal(t, expected.processName, actual.processName, "processName")
}

var sessionIds = []string{
	"192.168.100.1",
	"192.168.100.2",
	"192.168.100.3",
	"192.168.100.4",
	"192.168.100.5",
}

func randomSessionId() string {
	return sessionIds[rand.Intn(len(sessionIds))]
}

type LogComponent struct {
	processId   uint32
	processName string
	threadId    uint32
	component   string
}

var components = []LogComponent{
	LogComponent{
		processId:   1,
		processName: "view",
		threadId:    101,
		component:   "view",
	},
	LogComponent{
		processId:   1,
		processName: "view",
		threadId:    101,
		component:   "Lmx",
	},
	LogComponent{
		processId:   2,
		processName: "alarmmgr",
		threadId:    123,
		component:   "alarmmgr",
	},
	LogComponent{
		processId:   3,
		processName: "FactMES.Server.Host",
		threadId:    42,
		component:   "FactMES.Server.Communication",
	},
	LogComponent{
		processId:   4,
		processName: "hd",
		threadId:    314,
		component:   "hd",
	},
	LogComponent{
		processId:   1,
		processName: "view",
		threadId:    101,
		component:   "INTSPT",
	},
}

func randomComponent() LogComponent {
	return components[rand.Intn(len(components))]
}

var logFlags = []string{
	"Info",
	"Warning",
	"Error",
}

func randomLogFlag() string {
	return logFlags[rand.Intn(len(logFlags))]
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomMessage() string {
	// https://stackoverflow.com/a/22892986
	length := rand.Intn(20) + 10
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func createTestLogRecords(file string, num int, startingNumber uint64) []LogRecord {
	records := make([]LogRecord, num)
	number := uint64(startingNumber)
	recordTime := time.Now().Add(time.Millisecond * time.Duration(-num*500))
	for i := 0; i < num; i++ {
		component := randomComponent()
		records[i] = LogRecord{
			file:        file,
			number:      number,
			sessionId:   randomSessionId(),
			processId:   component.processId,
			processName: component.processName,
			threadId:    component.threadId,
			recordTime:  recordTime,
			logFlag:     randomLogFlag(),
			component:   component.component,
			message:     randomMessage(),
		}
		number++
		recordTime = recordTime.Add(time.Millisecond * 500)
	}
	return records
}

func createTestLogHeader(file string, computerName string, session string, previousFile string, records []LogRecord) LogHeader {
	count := int32(0)
	firstNumber := uint64(0)
	firstTime := time.Now()
	lastTime := time.Now()
	if records != nil {
		count = int32(len(records))
		firstNumber = records[0].number
		firstTime = records[0].recordTime
		lastTime = records[len(records)-1].recordTime
	}
	return LogHeader{
		file:              file,
		messageCount:      count,
		firstRecordNumber: firstNumber,
		firstRecordTime:   firstTime,
		lastRecordTime:    lastTime,
		computerName:      computerName,
		session:           session,
		previousFile:      previousFile,
	}
}

// Verify we can read a real log file that was created on a
// System Platform 2017 Update 3 SP1 machine.
func TestReadActualLogFile(t *testing.T) {
	configureLogp()

	directory := "../tests/files/logs"
	fileName := "DA17U3SP11565612061.aaLOG"
	filePath := filepath.Join(directory, fileName)

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000000,
	}

	aalog, teardown := setupAaLog(t, filePath, 0, 0, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, records, 1049)

	// First record
	r := records[0]
	assert.Equal(t, "DA17U3SP11565612061.aaLOG", r.file, "file")
	assert.Equal(t, int32(94), r.length, "length")
	assert.Equal(t, uint64(27741), r.number, "number")
	assert.Equal(t, int32(144), r.offset, "offset")
	assert.Equal(t, int32(0), r.offsetToPreviousRecord, "offsetToPreviousRecord")
	assert.Equal(t, int32(238), r.offsetToNextRecord, "offsetToNextRecord")
	assert.Equal(t, "0.0.0.0", r.sessionId, "sessionId")
	assert.Equal(t, uint32(2204), r.processId, "processId")
	assert.Equal(t, uint32(2220), r.threadId, "threadId")
	assert.Equal(t, time.Date(2019, time.August, 12, 7, 14, 21, 890812000, time.Local), r.recordTime, "recordTime")
	assert.Equal(t, "Info", r.logFlag, "logFlag")
	assert.Equal(t, "aaLogger", r.component, "component")
	assert.Equal(t, "Logger Started.", r.message, "message")
	assert.Equal(t, "", r.processName, "processName")

	// Skip a bit
	r = records[19]
	assert.Equal(t, "DA17U3SP11565612061.aaLOG", r.file, "file")
	assert.Equal(t, int32(582), r.length, "length")
	assert.Equal(t, uint64(27760), r.number, "number")
	assert.Equal(t, int32(3454), r.offset, "offset")
	assert.Equal(t, int32(3344), r.offsetToPreviousRecord, "offsetToPreviousRecord")
	assert.Equal(t, int32(4036), r.offsetToNextRecord, "offsetToNextRecord")
	assert.Equal(t, "0.0.0.0", r.sessionId, "sessionId")
	assert.Equal(t, uint32(3236), r.processId, "processId")
	assert.Equal(t, uint32(3564), r.threadId, "threadId")
	assert.Equal(t, time.Date(2019, time.August, 12, 7, 14, 39, 901517900, time.Local), r.recordTime, "recordTime")
	assert.Equal(t, "Warning", r.logFlag, "logFlag")
	assert.Equal(t, "FactMES.Server.Host", r.component, "component")
	assert.Equal(t, `The Microsoft Message Queue (.\private$\MESAsyncQueue) does not exist. The service will not be able to run with Async Messaging Services. Make sure that the DB/MW Communication feature is configured in Wonderware Configurator.`, r.message, "message")
	assert.Equal(t, "FactMES.Server.Host", r.processName, "processName")

	// Last record
	r = records[len(records)-1]
	assert.Equal(t, "DA17U3SP11565612061.aaLOG", r.file, "file")
	assert.Equal(t, int32(94), r.length, "length")
	assert.Equal(t, uint64(28789), r.number, "number")
	assert.Equal(t, int32(281626), r.offset, "offset")
	assert.Equal(t, int32(281520), r.offsetToPreviousRecord, "offsetToPreviousRecord")
	assert.Equal(t, int32(281720), r.offsetToNextRecord, "offsetToNextRecord")
	assert.Equal(t, "0.0.0.0", r.sessionId, "sessionId")
	assert.Equal(t, uint32(2384), r.processId, "processId")
	assert.Equal(t, uint32(2400), r.threadId, "threadId")
	assert.Equal(t, time.Date(2019, time.August, 12, 15, 24, 15, 62385300, time.Local), r.recordTime, "recordTime")
	assert.Equal(t, "Info", r.logFlag, "logFlag")
	assert.Equal(t, "aaLogger", r.component, "component")
	assert.Equal(t, "Logger Started.", r.message, "message")
	assert.Equal(t, "", r.processName, "processName")
}

// Verify we can read one record as if we have not ready any records before,
// i.e., the state has zero for the RecordNumber.
func TestReadOneUnreadRecord(t *testing.T) {
	configureLogp()

	fileName := "test-log-single.aaLOG"

	testRecords := createTestLogRecords(fileName, 1, 1)
	testHeader := createTestLogHeader(fileName, "MyPC", "Session01", "FileX.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	// We haven't ready any messages yet, so pass in a
	// record number of 0.
	aalog, teardown := setupAaLog(t, filePath, 0, 0, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, records, len(testRecords))
	r := records[0]
	assert.Equal(t, uint64(1), r.number)
	assertRecordsMatch(t, testRecords[0], r)
}

// Verify we can read multiple records as if we have not ready any records before,
// i.e., the state has zero for the RecordNumber.
func TestReadMultipleUnreadRecords(t *testing.T) {
	configureLogp()

	fileName := "test-log-multiple.aaLOG"
	testRecords := createTestLogRecords(fileName, 20, 1001)
	testHeader := createTestLogHeader(fileName, "PC0001", "Session27", "SomeFileY.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	// We haven't ready any messages yet, so pass in a
	// record number of 0.
	aalog, teardown := setupAaLog(t, filePath, 0, 0, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, records, len(testRecords))
	for _, r := range records {
		lr, found := findMatchingRecord(r.number, testRecords)
		if !found {
			t.Errorf("Unknown record number %d returned by Read()", r.number)
		}
		assertRecordsMatch(t, lr, r)
	}
}

// Verify we can read one record after the one kept in state, that it will
// return the next record after the last record number, which is not necessarily
// the first record in the file.
func TestReadOneRecordWithState(t *testing.T) {
	configureLogp()

	fileName := "test-log-single-state.aaLOG"
	testRecords := createTestLogRecords(fileName, 3, 101)
	testHeader := createTestLogHeader(fileName, "YourPC", "Sess50", "FileZ.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	// We have already read record 102.
	tr, found := findMatchingRecord(102, testRecords)
	if !found {
		t.Fatalf("Failed to find test record %d during test setup", 102)
	}
	aalog, teardown := setupAaLog(t, filePath, tr.number, tr.offset, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have skipped the record with number 102. So it should skip
	// records 101 and 102 and return only 103.
	assert.Len(t, records, 1)
	r := records[0]
	t.Log(r)
	assert.Equal(t, uint64(103), r.number)
	assertRecordsMatch(t, testRecords[2], r)
}

// Verify we can read multiple records after the one kept in state, that it will
// return the next records after the last record number, which is not necessarily
// the first record in the file.
func TestReadMultipleRecordsWithState(t *testing.T) {
	configureLogp()

	fileName := "test-log-multi-state.aaLOG"
	testRecords := createTestLogRecords(fileName, 20, 1001)
	testHeader := createTestLogHeader(fileName, "HerPC", "Session42", "FileABC.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	// We have already read record 1008.
	tr, found := findMatchingRecord(1008, testRecords)
	if !found {
		t.Fatalf("Failed to find test record %d during test setup", 1008)
	}
	aalog, teardown := setupAaLog(t, filePath, tr.number, tr.offset, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have skipped the record with number 1008. So it should skip
	// records up to 1008 and return records 1009 and after.
	assert.Len(t, records, 12)
	for _, r := range records {
		assert.True(t, r.number > uint64(1008))
		lr, found := findMatchingRecord(r.number, testRecords)
		if !found {
			t.Errorf("Unknown record number %d returned by Read()", r.number)
		}
		assertRecordsMatch(t, lr, r)
	}
}

// Verify we get back an empty list of records after the one kept in state,
// when there are no new records in the log
func TestReadZeroRecordsWithState(t *testing.T) {
	configureLogp()

	fileName := "test-log-zero-state.aaLOG"
	testRecords := createTestLogRecords(fileName, 20, 1001)
	testHeader := createTestLogHeader(fileName, "HisPC", "Session4321", "FileCBA.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	// We have already read all of the records.
	tr, found := findMatchingRecord(1020, testRecords)
	if !found {
		t.Fatalf("Failed to find test record %d during test setup", 1020)
	}
	aalog, teardown := setupAaLog(t, filePath, tr.number, tr.offset, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have returned an empty array.
	assert.Len(t, records, 0)
}

// Verify we get back an empty list of records when the log only contains
// a header
func TestReadZeroRecordsWhenEmpty(t *testing.T) {
	configureLogp()

	fileName := "test-log-just-a-header.aaLOG"
	testHeader := createTestLogHeader(fileName, "PC01", "Session X", "File1234.aaLOG", nil)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	aalog, teardown := setupAaLog(t, filePath, 0, 0, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have returned an empty array.
	assert.Len(t, records, 0)
}

// Verify it reads no more than the configured batch size of records
func TestReadMultipleRecordsUpToBatchLimit(t *testing.T) {
	configureLogp()

	fileName := "test-log-multi-batch-001.aaLOG"
	testRecords := createTestLogRecords(fileName, 1050, 10001)
	testHeader := createTestLogHeader(fileName, "ThisPC", "Session Alpha", "FileBatch001.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	// We have already read record 10011.
	tr, found := findMatchingRecord(10011, testRecords)
	if !found {
		t.Fatalf("Failed to find test record %d during test setup", 10011)
	}
	aalog, teardown := setupAaLog(t, filePath, tr.number, tr.offset, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have skipped the record with number 10011. It should start
	// at record 10012 and read a total of 1,000 records.
	assert.Len(t, records, 1000)
	for _, r := range records {
		assert.True(t, r.number > uint64(10011))
		lr, found := findMatchingRecord(r.number, testRecords)
		if !found {
			t.Errorf("Unknown record number %d returned by Read()", r.number)
		}
		assertRecordsMatch(t, lr, r)
	}
}

// Verify it reads no more than the configured batch size of records
// with a different setting
func TestReadMultipleRecordsUpToDifferentBatchLimit(t *testing.T) {
	configureLogp()

	fileName := "test-log-multi-batch-002.aaLOG"
	testRecords := createTestLogRecords(fileName, 600, 1001)
	testHeader := createTestLogHeader(fileName, "ThatPC", "Session Beta", "FileBatch002.aaLOG", testRecords)

	directory := "test-files"
	filePath := filepath.Join(directory, fileName)
	err := writeTestLogFile(filePath, &testHeader, testRecords)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   500,
	}
	// We have already read record 1021.
	tr, found := findMatchingRecord(1021, testRecords)
	if !found {
		t.Fatalf("Failed to find test record %d during test setup", 1021)
	}
	aalog, teardown := setupAaLog(t, filePath, tr.number, tr.offset, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have skipped the record with number 1021. It should start
	// at record 1022 and read a total of 500 records.
	assert.Len(t, records, 500)
	for _, r := range records {
		assert.True(t, r.number > uint64(1021))
		lr, found := findMatchingRecord(r.number, testRecords)
		if !found {
			t.Errorf("Unknown record number %d returned by Read()", r.number)
		}
		assertRecordsMatch(t, lr, r)
	}
}

// Verify it reads no more than the configured batch size of records
// with a different setting
func TestReadFindsMostRecentFileWhenNoFileInState(t *testing.T) {
	configureLogp()

	directory := "test-files"

	fileNameA := "FileXYZ.aaLOG"
	testRecordsA := createTestLogRecords(fileNameA, 30, 1)
	testHeaderA := createTestLogHeader(fileNameA, "ThisPC", "SessionA", "File000.aaLOG", testRecordsA)
	filePathA := filepath.Join(directory, fileNameA)
	err := writeTestLogFile(filePathA, &testHeaderA, testRecordsA)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePathA)
		if err != nil {
			t.Fatal(err)
		}
	}()

	fileNameB := "FileABC.aaLOG"
	testRecordsB := createTestLogRecords(fileNameB, 10, 11)
	testHeaderB := createTestLogHeader(fileNameB, "ThatPC", "SessionB", "FileXYZ.aaLOG", testRecordsB)
	filePathB := filepath.Join(directory, fileNameB)
	err = writeTestLogFile(filePathB, &testHeaderB, testRecordsB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePathB)
		if err != nil {
			t.Fatal(err)
		}
	}()

	fileNameC := "FileDEF.aaLOG"
	testRecordsC := createTestLogRecords(fileNameC, 40, 21)
	testHeaderC := createTestLogHeader(fileNameC, "OtherPC", "SessionC", "FileABC.aaLOG", testRecordsC)
	filePathC := filepath.Join(directory, fileNameC)
	err = writeTestLogFile(filePathC, &testHeaderC, testRecordsC)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := deleteFileIfExists(filePathC)
		if err != nil {
			t.Fatal(err)
		}
	}()

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   1000,
	}
	aalog, teardown := setupAaLog(t, "", 0, 0, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Should have started with the records in file FileABC.aaLOG
	// starting at record number 11
	assert.Len(t, records, len(testRecordsB))
	for _, r := range records {
		assert.True(t, r.number >= uint64(11))
		lr, found := findMatchingRecord(r.number, testRecordsB)
		if !found {
			t.Errorf("Unknown record number %d returned by Read()", r.number)
		}
		assertRecordsMatch(t, lr, r)
	}
}

// Verify when it reaches the end of one log file it will find the next one
func TestReadSwitchFromOneLogFileToAnother(t *testing.T) {
	configureLogp()

	directory := "../tests/files/logs"
	fileName := "DA17U3SP11565271940.aaLOG"
	filePath := filepath.Join(directory, fileName)

	options := map[string]interface{}{
		"directory":    directory,
		"file_pattern": "*.aaLOG",
		"batch_size":   500,
	}
	// Start a few records from the end of the file
	aalog, teardown := setupAaLog(t, filePath, 27736, 337424, options)
	defer teardown()

	records, err := aalog.Read()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, records, 500)

	fr := records[0]
	assert.Equal(t, uint64(27737), fr.number)
	assert.Equal(t, "DA17U3SP11565271940.aaLOG", fr.file)

	lr := records[len(records)-1]
	assert.Equal(t, uint64(28236), lr.number)
	assert.Equal(t, "DA17U3SP11565612061.aaLOG", lr.file)
}

func findMatchingRecord(number uint64, records []LogRecord) (LogRecord, bool) {
	for _, r := range records {
		if r.number == number {
			return r, true
		}
	}
	return LogRecord{}, false
}

func fatalErr(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
}

func newTestAaLog(t *testing.T, options map[string]interface{}) AaLog {
	commonConfig, err := common.NewConfigFrom(options)
	fatalErr(t, err)
	c := config.AalogbeatConfig{}
	err = config.ReadConfig(commonConfig, &c)
	fatalErr(t, err)
	aaLog, err := New(c)
	fatalErr(t, err)
	return aaLog
}

func setupAaLog(t *testing.T, filePath string, recordNumber uint64, recordOffset int32, options map[string]interface{}) (AaLog, teardown) {
	aaLog := newTestAaLog(t, options)
	if filePath != "" {
		filePath = filepath.Base(filePath)
	}
	fatalErr(t, aaLog.Open(checkpoint.AaLogState{
		FileName:     filePath,
		RecordNumber: recordNumber,
		RecordOffset: recordOffset,
	}))
	return aaLog, func() { fatalErr(t, aaLog.Close()) }
}
