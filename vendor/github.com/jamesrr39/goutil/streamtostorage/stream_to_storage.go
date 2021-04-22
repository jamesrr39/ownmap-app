package streamtostorage

import (
	"encoding/binary"
	"fmt"
	"io"
)

type MessageSizeBufferLen uint

const (
	MessageSizeBufferLenSmall   MessageSizeBufferLen = 2 // supports up to 65,536 bytes message size
	MessageSizeBufferLenDefault MessageSizeBufferLen = 4 // supports up to 4 GB message size
	MessageSizeBufferLenLegacy  MessageSizeBufferLen = 8 // supports up to 2 ^ 64 bytes message size (huge)
)

type putFuncType func(messageSize int) []byte

// Writer is a writer for writing streams of messages to a writer.
// The caller must provide synchronization for this writer; or use the SynchronizedWriter provided.
type Writer struct {
	file           io.Writer
	maxMessageSize uint64
	putFunc        putFuncType
}

func getMaxMessageSize(messageSizeBufferLen MessageSizeBufferLen) uint64 {
	if messageSizeBufferLen == MessageSizeBufferLenLegacy {
		// special case for 64-bit messages, since 2^64 doesn't fit into a uint64, so we allow (2^64)-1. Should be enough.
		return uint64((1 << 64) - 1)
	}
	return uint64(1 << (messageSizeBufferLen * 8))
}

func NewWriter(file io.Writer, messageSizeBufferLen MessageSizeBufferLen) (*Writer, error) {
	putFunc, err := getPutFunc(messageSizeBufferLen)
	if err != nil {
		return nil, err
	}
	return &Writer{file, getMaxMessageSize(messageSizeBufferLen), putFunc}, nil
}

func getPutFunc(messageSizeBufferLen MessageSizeBufferLen) (putFuncType, error) {
	switch messageSizeBufferLen {
	case MessageSizeBufferLenSmall:
		return func(messageSize int) []byte {
			lenBuffer := make([]byte, messageSizeBufferLen)
			binary.LittleEndian.PutUint16(lenBuffer, uint16(messageSize))
			return lenBuffer
		}, nil

	case MessageSizeBufferLenDefault:
		return func(messageSize int) []byte {
			lenBuffer := make([]byte, messageSizeBufferLen)
			binary.LittleEndian.PutUint32(lenBuffer, uint32(messageSize))
			return lenBuffer
		}, nil

	case MessageSizeBufferLenLegacy:
		return func(messageSize int) []byte {
			lenBuffer := make([]byte, messageSizeBufferLen)
			binary.LittleEndian.PutUint64(lenBuffer, uint64(messageSize))
			return lenBuffer
		}, nil

	default:
		return nil, fmt.Errorf("unsupported size buffer len: %d", messageSizeBufferLen)
	}
}

func (s *Writer) Write(message []byte) (int, error) {
	messageWithLen, err := makeMessageWithLen(message, s.maxMessageSize, s.putFunc)
	if err != nil {
		return 0, err
	}
	_, err = s.file.Write(messageWithLen)
	if err != nil {
		return 0, err
	}

	return len(message), nil
}

func makeMessageWithLen(message []byte, maxMessageSize uint64, putFunc putFuncType) ([]byte, error) {
	messageLen := len(message)

	if uint64(messageLen) > maxMessageSize {
		return nil, fmt.Errorf("message larger than sizing allows; message is %d but max size is %d", messageLen, maxMessageSize)
	}

	lenBuffer := putFunc(messageLen)

	messageWithLen := append(lenBuffer, message...)

	return messageWithLen, nil
}

type synchronizedWriteMessage struct {
	MessageWithLen []byte
	OnFinishedChan chan (error)
}

type SynchronizedWriter struct {
	writer         io.Writer
	writeChan      chan *synchronizedWriteMessage
	maxMessageSize uint64
	putFunc        putFuncType
}

func NewSynchronizedWriter(file io.Writer, messageSizeBufferLen MessageSizeBufferLen) (*SynchronizedWriter, error) {
	putFunc, err := getPutFunc(messageSizeBufferLen)
	if err != nil {
		return nil, err
	}

	w := &SynchronizedWriter{file, make(chan (*synchronizedWriteMessage)), getMaxMessageSize(messageSizeBufferLen), putFunc}

	go func() {
		for {
			message := <-w.writeChan
			_, err := w.writer.Write(message.MessageWithLen)

			message.OnFinishedChan <- err
		}
	}()

	return w, nil
}

func (s *SynchronizedWriter) Write(message []byte) (int, error) {
	messageWithLen, err := makeMessageWithLen(message, s.maxMessageSize, s.putFunc)
	if err != nil {
		return 0, err
	}

	syncMessage := &synchronizedWriteMessage{
		MessageWithLen: messageWithLen,
		OnFinishedChan: make(chan (error)),
	}

	s.writeChan <- syncMessage

	err = <-syncMessage.OnFinishedChan

	return len(message), err
}

type Reader struct {
	file                 io.Reader
	messageSizeBufferLen MessageSizeBufferLen
}

func NewReader(file io.Reader, messageSizeBufferLen MessageSizeBufferLen) *Reader {
	return &Reader{file, messageSizeBufferLen}
}

// ReadNextMessage reads the next message from the reader (starting from the beginning)
// Once the end of the reader has been reached, an io.EOF error is returned.
func (sr *Reader) ReadNextMessage() ([]byte, error) {
	lenBuffer := make([]byte, sr.messageSizeBufferLen)
	_, err := sr.file.Read(lenBuffer)
	if err != nil {
		return nil, err
	}

	var messageLen uint64
	switch sr.messageSizeBufferLen {
	case MessageSizeBufferLenLegacy:
		messageLen = binary.LittleEndian.Uint64(lenBuffer)
	case MessageSizeBufferLenDefault:
		messageLen = uint64(binary.LittleEndian.Uint32(lenBuffer))
	case MessageSizeBufferLenSmall:
		messageLen = uint64(binary.LittleEndian.Uint16(lenBuffer))
	default:
		return nil, fmt.Errorf("unknown messageSizeBufferLen: %v", sr.messageSizeBufferLen)
	}
	messageBuffer := make([]byte, messageLen)

	_, err = sr.file.Read(messageBuffer)
	if err != nil {
		return nil, err
	}

	return messageBuffer, nil
}
