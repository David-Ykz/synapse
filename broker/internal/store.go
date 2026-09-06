package synapse

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	INDEX_FILE_NAME    = "index"
	SEGMENTS_DIR_NAME  = "segments"
	DLQNamespaceSuffix = "-dlq"

	OFFSET_BUFFER_MAX_SIZE int64 = 4096
	// offsetAt/readMessage assume a segment always fits exactly one ring-buffer sweep
	SEGMENT_SIZE = OFFSET_BUFFER_MAX_SIZE
)

// inFlightMessage tracks a delivered-but-unacked message
type inFlightMessage struct {
	payload     []byte
	retryCount  int
	redeliverAt time.Time
}

type Broker struct {
	id               int
	filepath         string
	namespace        string
	writeBufferSize  int
	offsets          [OFFSET_BUFFER_MAX_SIZE]int64
	offsetWriteIndex int64
	offsetReadIndex  int64 // only advances via Raft-applied MarkCompleted

	mu                     sync.Mutex
	offsetDeliverIndex     int64
	inFlightMessages       map[int64]*inFlightMessage
	completedMessages      map[int64]bool
	unreclaimedSegmentBase int64
}

func exists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func write(filename string, bufferSize int, data []byte) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	writer := bufio.NewWriterSize(f, bufferSize)
	_, err = writer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}
	return writer.Flush()
}

func writeInt64(filename string, val int64) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	err = binary.Write(f, binary.LittleEndian, val)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}
	return nil
}

func writeIndexFile(filename string, val int64) error {
	// overwrites the read-index in place instead of appending
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(val))
	_, err = f.WriteAt(buf, 0)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}
	return nil
}

func readIndexFile(filename string) (int64, error) {
	if !exists(filename) {
		return 0, nil
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to read index file %s: %w", filename, err)
	}
	if len(data) < 8 {
		return 0, nil
	}
	return int64(binary.LittleEndian.Uint64(data[:8])), nil
}

func read(filename string, startOffset int64, endOffset int64) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// if endOffset is 0, we read until the end of the file
	if endOffset == 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to get file size: %w", err)
		}
		endOffset = info.Size()
	}

	if endOffset <= startOffset {
		return nil, fmt.Errorf("endOffset (%d) must be greater than startOffset (%d)", endOffset, startOffset)
	}

	length := endOffset - startOffset
	data := make([]byte, length)
	_, err = f.ReadAt(data, startOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	return data, nil
}

func backoffDuration(base, max time.Duration, retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	// guard against overflow from a runaway retry count
	if retryCount > 30 {
		retryCount = 30
	}
	d := base
	for i := 1; i < retryCount; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

func NewBroker(id int, namespace string, filepath string, writeBufferSize int) *Broker {
	return &Broker{
		id:                id,
		namespace:         namespace,
		filepath:          filepath,
		writeBufferSize:   writeBufferSize,
		inFlightMessages:  make(map[int64]*inFlightMessage),
		completedMessages: make(map[int64]bool),
	}
}

func (b *Broker) fullFilepath(filename string) string {
	return fmt.Sprintf("%s/%s/%d/%s", b.filepath, b.namespace, b.id, filename)
}

// segmentBase returns the index of the first message in a given segment
func (b *Broker) segmentBase(messageIndex int64) int64 {
	return (messageIndex / SEGMENT_SIZE) * SEGMENT_SIZE
}

func (b *Broker) segmentPath(base int64, ext string) string {
	return b.fullFilepath(fmt.Sprintf("%s/%020d.%s", SEGMENTS_DIR_NAME, base, ext))
}

func (b *Broker) Initialize() error {
	basePath := b.fullFilepath("")
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return fmt.Errorf("failed to create required directories for path %s: %w", basePath, err)
	}
	segmentsDir := b.fullFilepath(SEGMENTS_DIR_NAME)
	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		return fmt.Errorf("failed to create segments directory %s: %w", segmentsDir, err)
	}

	indexFilepath := b.fullFilepath(INDEX_FILE_NAME)
	var err error
	b.offsetReadIndex, err = readIndexFile(indexFilepath)
	if err != nil {
		return err
	}

	// finds existing segments
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return fmt.Errorf("failed to read segments directory %s: %w", segmentsDir, err)
	}
	writeBase := int64(-1)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".offset") {
			continue
		}
		base, parseErr := strconv.ParseInt(strings.TrimSuffix(name, ".offset"), 10, 64)
		if parseErr != nil {
			continue
		}
		if base+SEGMENT_SIZE <= b.offsetReadIndex {
			os.Remove(b.segmentPath(base, "data"))
			os.Remove(b.segmentPath(base, "offset"))
			continue
		}
		if base > writeBase {
			writeBase = base
		}
	}

	if writeBase >= 0 {
		offsetPath := b.segmentPath(writeBase, "offset")
		f, openErr := os.Open(offsetPath)
		if openErr != nil {
			return fmt.Errorf("failed to open offset file %s: %w", offsetPath, openErr)
		}
		defer f.Close()

		info, _ := f.Stat()
		numRecords := info.Size() / 8
		b.offsetWriteIndex = writeBase + numRecords

		for r := int64(0); r < numRecords; r++ {
			var val int64
			if err := binary.Read(f, binary.LittleEndian, &val); err != nil {
				return fmt.Errorf("failed to read offset file %s: %w", offsetPath, err)
			}
			b.offsets[(writeBase+r+1)%OFFSET_BUFFER_MAX_SIZE] = val
		}
	} else {
		b.offsetWriteIndex = b.segmentBase(b.offsetReadIndex)
	}

	b.offsetDeliverIndex = b.offsetReadIndex
	b.inFlightMessages = make(map[int64]*inFlightMessage)
	b.completedMessages = make(map[int64]bool)
	b.unreclaimedSegmentBase = b.segmentBase(b.offsetReadIndex)

	return nil
}

// offsetAt returns messageIndex's local start offset within its own segment, falling back to disk once messageIndex falls outside the ring buffer's cached window. Callers must hold b.mu
func (b *Broker) offsetAt(messageIndex int64) (int64, error) {
	base := b.segmentBase(messageIndex)
	if messageIndex == base {
		return 0, nil // first message of a segment always starts at local offset 0
	}
	if b.offsetWriteIndex-messageIndex < OFFSET_BUFFER_MAX_SIZE {
		return b.offsets[messageIndex%OFFSET_BUFFER_MAX_SIZE], nil
	}

	offsetPath := b.segmentPath(base, "offset")
	f, err := os.Open(offsetPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open offset file %s: %w", offsetPath, err)
	}
	defer f.Close()

	buf := make([]byte, 8)
	if _, err := f.ReadAt(buf, (messageIndex-base-1)*8); err != nil {
		return 0, fmt.Errorf("failed to read offset file %s at record %d: %w", offsetPath, messageIndex-base-1, err)
	}
	return int64(binary.LittleEndian.Uint64(buf)), nil
}

func (b *Broker) readMessage(messageIndex int64) ([]byte, error) {
	base := b.segmentBase(messageIndex)
	dataPath := b.segmentPath(base, "data")

	startOffset, err := b.offsetAt(messageIndex)
	if err != nil {
		return nil, err
	}

	var endOffset int64
	if messageIndex+1 < base+SEGMENT_SIZE {
		// since messageIndex + 1 is still in the same segment we can use its start offset as messageIndex's end offset
		endOffset, err = b.offsetAt(messageIndex + 1)
		if err != nil {
			return nil, err
		}
	} else {
		// messageIndex is the last message in its segment, so its end is this segment's data file size
		info, statErr := os.Stat(dataPath)
		if statErr != nil {
			return nil, fmt.Errorf("failed to stat segment data file %s: %w", dataPath, statErr)
		}
		endOffset = info.Size()
	}

	return read(dataPath, startOffset, endOffset)
}

func (b *Broker) Write(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	base := b.segmentBase(b.offsetWriteIndex)
	dataPath := b.segmentPath(base, "data")
	offsetPath := b.segmentPath(base, "offset")

	if err := write(dataPath, b.writeBufferSize, data); err != nil {
		return fmt.Errorf("failed to write to data file %s: %w", dataPath, err)
	}

	var prevOffset int64
	if b.offsetWriteIndex != base {
		prevOffset = b.offsets[b.offsetWriteIndex%OFFSET_BUFFER_MAX_SIZE]
	}
	newOffset := prevOffset + int64(len(data))

	b.offsetWriteIndex++
	b.offsets[b.offsetWriteIndex%OFFSET_BUFFER_MAX_SIZE] = newOffset

	if err := writeInt64(offsetPath, newOffset); err != nil {
		return fmt.Errorf("failed to write to offset file %s: %w", offsetPath, err)
	}
	return nil
}

// Deliver returns the next message to redeliver (visibility timeout expired) or if none, the next new message (messageIndex is -1 when nothing is available)
// If a pending message exhausts its retries, we dead-letter it internally, and the caller must replicate it and call Deliver again
func (b *Broker) Deliver(maxRetries int, backoffBase, backoffMax time.Duration) (messageIndex int64, payload []byte, deadLetterIndex int64, deadLetterPayload []byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	deadLetterIndex = -1
	now := time.Now()

	// redeliver old pending messages over new messages
	expired := int64(-1)
	for i, msg := range b.inFlightMessages {
		if now.Before(msg.redeliverAt) {
			continue
		}
		if expired == -1 || i < expired {
			expired = i
		}
	}

	if expired != -1 {
		msg := b.inFlightMessages[expired]
		msg.retryCount++
		if msg.retryCount > maxRetries {
			delete(b.inFlightMessages, expired)
			return -1, nil, expired, msg.payload, nil
		}
		msg.redeliverAt = now.Add(backoffDuration(backoffBase, backoffMax, msg.retryCount))
		return expired, msg.payload, -1, nil, nil
	}

	if b.offsetDeliverIndex < b.offsetReadIndex {
		b.offsetDeliverIndex = b.offsetReadIndex
	}

	if b.offsetDeliverIndex >= b.offsetWriteIndex {
		return -1, nil, -1, nil, nil
	}

	deliverIndex := b.offsetDeliverIndex
	data, e := b.readMessage(deliverIndex)
	if e != nil {
		return -1, nil, -1, nil, e
	}

	b.offsetDeliverIndex++
	b.inFlightMessages[deliverIndex] = &inFlightMessage{
		payload:     data,
		retryCount:  0,
		redeliverAt: now.Add(backoffBase),
	}
	return deliverIndex, data, -1, nil, nil
}

// Nack marks messageIndex as failed right now, making it immediately eligible for redelivery (or dead-lettering) on the next Deliver call
func (b *Broker) Nack(messageIndex int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if msg, ok := b.inFlightMessages[messageIndex]; ok {
		msg.redeliverAt = time.Time{}
	}
}

func (b *Broker) MarkCompleted(messageIndex int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if messageIndex < b.offsetReadIndex {
		return nil
	}

	delete(b.inFlightMessages, messageIndex)
	b.completedMessages[messageIndex] = true

	for b.completedMessages[b.offsetReadIndex] {
		delete(b.completedMessages, b.offsetReadIndex)
		b.offsetReadIndex++
	}

	indexFilepath := b.fullFilepath(INDEX_FILE_NAME)
	if err := writeIndexFile(indexFilepath, b.offsetReadIndex); err != nil {
		return err
	}

	for b.unreclaimedSegmentBase+SEGMENT_SIZE <= b.offsetReadIndex {
		os.Remove(b.segmentPath(b.unreclaimedSegmentBase, "data"))
		os.Remove(b.segmentPath(b.unreclaimedSegmentBase, "offset"))
		b.unreclaimedSegmentBase += SEGMENT_SIZE
	}

	return nil
}

// Lag reports how many produced messages are not yet durably resolved (what the autoscaler should treat as outstanding work)
func (b *Broker) Lag() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	lag := b.offsetWriteIndex - b.offsetReadIndex
	if lag < 0 {
		lag = 0
	}
	return lag
}
