package synapse

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	DATA_FILE_NAME               = "data"
	OFFSET_FILE_NAME             = "offset"
	INDEX_FILE_NAME              = "index"
	OFFSET_BUFFER_MAX_SIZE int64 = 4096
)

type Broker struct {
	id               int
	filepath         string
	namespace        string
	writeBufferSize  int
	offsets          [OFFSET_BUFFER_MAX_SIZE]int64
	offsetWriteIndex int64
	offsetReadIndex  int64
}

func exists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func write(filename string, bufferSize int, data []byte) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("write() failed to open file: %w", err)
	}
	defer f.Close()

	writer := bufio.NewWriterSize(f, bufferSize)
	_, err = writer.Write(data)
	if err != nil {
		return fmt.Errorf("write() failed to write data to file: %w", err)
	}
	return writer.Flush()
}

func writeInt64(filename string, val int64) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("writeInt64() failed to open file: %w", err)
	}
	defer f.Close()

	err = binary.Write(f, binary.LittleEndian, val)
	if err != nil {
		return fmt.Errorf("writeInt64() failed to write data to file: %w", err)
	}
	return nil
}

func read(filename string, startOffset int64, endOffset int64) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("read() failed to open file: %w", err)
	}
	defer f.Close()

	// if endOffset is 0, we read until the end of the file
	if endOffset == 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("read() failed to get file size: %w", err)
		}
		endOffset = info.Size()
	}

	if endOffset <= startOffset {
		return nil, fmt.Errorf("read() endOffset (%d) must be greater than startOffset (%d)", endOffset, startOffset)
	}

	length := endOffset - startOffset
	data := make([]byte, length)
	_, err = f.ReadAt(data, startOffset)
	if err != nil {
		return nil, fmt.Errorf("read() failed to read data: %w", err)
	}

	return data, nil
}

func modulo(n int64, m int64) int64 {
	return ((n % m) + m) % m
}

func NewBroker(id int, namespace string, filepath string, writeBufferSize int) *Broker {
	return &Broker{
		id:              id,
		namespace:       namespace,
		filepath:        filepath,
		writeBufferSize: writeBufferSize,
	}
}

func (b *Broker) fullFilepath(filename string) string {
	return fmt.Sprintf("%s/%s/%d/%s", b.filepath, b.namespace, b.id, filename)
}

func (b *Broker) Initialize() error {
	basePath := b.fullFilepath("")
	fmt.Println(basePath)
	// create directories if they don't exist
	err := os.MkdirAll(basePath, 0755)
	if err != nil {
		return fmt.Errorf("Broker.Initialize() failed to create required directories for path %s: %w", basePath, err)
	}

	// load offsetReadIndex
	indexFilepath := b.fullFilepath(INDEX_FILE_NAME)
	if exists(indexFilepath) {
		f, err := os.OpenFile(indexFilepath, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("Broker.Initialize() failed to open index file %s: %w", indexFilepath, err)
		}
		defer f.Close()
		_, _ = f.Seek(-8, io.SeekEnd)
		err = binary.Read(f, binary.LittleEndian, &b.offsetReadIndex)
		if err != nil {
			return fmt.Errorf("Broker.Initialize() failed to read index file %s: %w", indexFilepath, err)
		}
		f.Truncate(0)
	}

	// load offsetWriteIndex and initialize offsets
	offsetFilepath := b.fullFilepath(OFFSET_FILE_NAME)
	if exists(offsetFilepath) {
		f, err := os.Open(offsetFilepath)
		if err != nil {
			return fmt.Errorf("Broker.Initialize() failed to open offset file %s: %w", offsetFilepath, err)
		}
		defer f.Close()

		info, _ := f.Stat()
		numRecords := info.Size() / 8
		b.offsetWriteIndex = numRecords

		toLoad := min(numRecords, OFFSET_BUFFER_MAX_SIZE)

		for i := int64(1); i <= toLoad; i++ {
			_, err := f.Seek(-i*8, io.SeekEnd)
			if err != nil {
				break
			}

			index := modulo(numRecords-i+1, OFFSET_BUFFER_MAX_SIZE)
			err = binary.Read(f, binary.LittleEndian, &b.offsets[index])
			if err != nil {
				return fmt.Errorf("Broker.Initialize() failed to read offset file %s: %w", offsetFilepath, err)
			}
		}
	}

	b.Print()

	return nil
}

func (b *Broker) Write(data []byte) error {
	// write the data
	dataFilepath := b.fullFilepath(DATA_FILE_NAME)
	err := write(dataFilepath, b.writeBufferSize, data)
	if err != nil {
		return fmt.Errorf("Broker.Write() failed to write to data file %s: %w", dataFilepath, err)
	}
	// get previous offset
	prevOffset := b.offsets[b.offsetWriteIndex%OFFSET_BUFFER_MAX_SIZE]
	newOffset := prevOffset + int64(len(data))
	// save new offset
	b.offsetWriteIndex++
	b.offsets[b.offsetWriteIndex%OFFSET_BUFFER_MAX_SIZE] = newOffset
	// store new offset (for persistence)
	offsetFilepath := b.fullFilepath(OFFSET_FILE_NAME)
	err = writeInt64(offsetFilepath, newOffset)
	if err != nil {
		return fmt.Errorf("Broker.Write() failed to write to offset file %s: %w", offsetFilepath, err)
	}
	return nil
}

func (b *Broker) ReadOne() ([]byte, error) {
	if b.offsetReadIndex >= b.offsetWriteIndex {
		return nil, nil
	}

	startOffset := b.offsets[b.offsetReadIndex]
	endOffset := b.offsets[b.offsetReadIndex+1]

	// read data
	dataFilepath := b.fullFilepath(DATA_FILE_NAME)
	data, err := read(dataFilepath, startOffset, endOffset)
	if err != nil {
		return nil, fmt.Errorf("Broker.ReadOne() failed from data file %s at offset %d - %d: %w", dataFilepath, startOffset, endOffset, err)
	}
	// increment index
	b.offsetReadIndex++
	// store index
	indexFilepath := b.fullFilepath(INDEX_FILE_NAME)
	err = writeInt64(indexFilepath, b.offsetReadIndex)
	if err != nil {
		return nil, fmt.Errorf("Broker.ReadOne() failed to write to index file %s: %w", indexFilepath, err)
	}
	return data, nil
}

func (b *Broker) Print() {
	fmt.Printf("write index: %d, read index: %d\n", b.offsetWriteIndex, b.offsetReadIndex)
	for i := int64(0); i < b.offsetWriteIndex; i++ {
		fmt.Printf("index: %d, val: %d\n", i, b.offsets[i])
	}
	fmt.Println("")
}
