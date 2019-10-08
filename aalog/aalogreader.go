package aalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf16"
)

func readLogHeaderFromFile(filePath string) (LogHeader, error) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return LogHeader{}, err
	}

	file, err := os.Open(absPath)
	if err != nil {
		return LogHeader{}, err
	}
	defer file.Close()

	return readLogHeader(file)
}

func readLogHeader(file *os.File) (LogHeader, error) {
	//Get first 12 byteArray into the byte array.
	//The header length is in the last 4 byteArray
	data := make([]byte, 12)
	_, err := file.Seek(0, 0)
	if err != nil {
		return LogHeader{}, err
	}
	_, err = file.Read(data)
	if err != nil {
		return LogHeader{}, err
	}
	headerLength := readInt32(data[8:12])

	// Read the entire header.
	data = make([]byte, headerLength)
	_, err = file.Seek(0, 0)
	if err != nil {
		return LogHeader{}, err
	}
	file.Read(data)

	// And read everything else from there.
	fileName := filepath.Base(file.Name())
	h := LogHeader{length: headerLength, file: fileName}
	h.firstRecordNumber = readUint64(data[20:28])
	h.messageCount = readInt32(data[28:32])
	h.firstRecordTime = readTime(data[32:40])
	h.lastRecordTime = readTime(data[40:48])
	h.firstRecordOffset = readInt32(data[48:52])
	h.lastRecordOffset = readInt32(data[52:56])
	// We don't know how long the strings are ahead of time,
	// so have to keep track as we go.
	stringLen := 0
	offset := 56
	h.computerName, stringLen, err = readString(data, offset)
	if err != nil {
		return LogHeader{}, err
	}
	offset += stringLen + 2
	h.session, stringLen, err = readString(data, offset)
	if err != nil {
		return LogHeader{}, err
	}
	offset += stringLen + 2
	h.previousFile, stringLen, err = readString(data, offset)
	if err != nil {
		return LogHeader{}, err
	}
	return h, nil
}

func readLogRecord(file *os.File, number uint64, offset int32) (LogRecord, error) {
	//l.log.Debugf("Read log record, file:%s, number:%d, offset:%d", file.Name(), number, offset)
	// First read the first 4 bytes to
	// get the record length.
	_, err := file.Seek(int64(offset)+4, 0)
	if err != nil {
		return LogRecord{}, err
	}
	buf := make([]byte, 4)
	_, err = file.Read(buf)
	if err != nil {
		return LogRecord{}, err
	}
	length := readInt32(buf)

	// Now read the whole record.
	buf = make([]byte, length)
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return LogRecord{}, err
	}
	_, err = file.Read(buf)
	if err != nil {
		return LogRecord{}, err
	}

	fileName := filepath.Base(file.Name())
	r := LogRecord{
		number: number,
		length: length,
		offset: offset,
		file:   fileName,
	}
	r.offsetToPreviousRecord = readInt32(buf[8:12])
	r.offsetToNextRecord = offset + length
	r.sessionId = readSessionId(buf[12:16])
	r.processId = readUint32(buf[16:20])
	r.threadId = readUint32(buf[20:24])
	r.recordTime = readTime(buf[24:32])
	// We don't know how long the strings are ahead of time,
	// so have to keep track as we go.
	stringLen := 0
	bufOffset := 32
	r.logFlag, stringLen, err = readString(buf, bufOffset)
	if err != nil {
		return LogRecord{}, err
	}
	bufOffset += stringLen + 2
	r.component, stringLen, err = readString(buf, bufOffset)
	if err != nil {
		return LogRecord{}, err
	}
	bufOffset += stringLen + 2
	r.message, stringLen, err = readString(buf, bufOffset)
	if err != nil {
		return LogRecord{}, err
	}
	bufOffset += stringLen + 2
	r.processName, _, err = readString(buf, bufOffset)
	if err != nil {
		return LogRecord{}, err
	}
	return r, nil
}

func readUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func readInt16(data []byte) int16 {
	return int16(readUint16(data))
}

func readInt32(data []byte) int32 {
	return int32(readUint32(data))
}

func readUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func readUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func readString(data []byte, offset int) (string, int, error) {
	var length int
	for index := offset; index < len(data); index += 2 {
		value := readInt16(data[index : index+2])
		if value == 0 {
			break
		}
		length += 2
	}
	// https://gist.github.com/bradleypeabody/185b1d7ed6c0c2ab6cec#gistcomment-2780177
	// Otherwise the string still has the 0x00 between each byte,
	// makes string comparisons with default encoding wonky.
	ints := make([]uint16, length/2)
	if err := binary.Read(bytes.NewReader(data[offset:offset+length]), binary.LittleEndian, &ints); err != nil {
		return "", length, err
	}
	s := string(utf16.Decode(ints))
	return s, length, nil
}

func readTime(data []byte) time.Time {
	// http://stackoverflow.com/a/6161842
	// Adapted to handle nanoseconds instead of seconds.
	nano := int64(readUint64(data)*100 - 11644473600000000000)
	return time.Unix(0, nano)
}

func readSessionId(buf []byte) string {
	return fmt.Sprintf("%d.%d.%d.%d", buf[3], buf[2], buf[1], buf[0])
}
