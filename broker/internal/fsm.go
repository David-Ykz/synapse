package synapse

import (
	"archive/tar"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type CommandType byte

const (
	CmdProduce CommandType = 0x01
	CmdConsume CommandType = 0x02
)

type Command struct {
	Type      CommandType
	Namespace string
	Data      []byte
}

// encodes a Command as: [1 byte type][1 byte namespace len][4 bytes data len][namespace][data]
func (c Command) encode() []byte {
	nsLen := len(c.Namespace)
	buf := make([]byte, 6+nsLen+len(c.Data))
	buf[0] = byte(c.Type)
	buf[1] = byte(nsLen)
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(c.Data)))
	copy(buf[6:], c.Namespace)
	copy(buf[6+nsLen:], c.Data)
	return buf
}

func decodeCommand(b []byte) (Command, error) {
	if len(b) < 6 {
		return Command{}, errors.New("raft command too short")
	}
	nsLen := int(b[1])
	dataLen := int(binary.BigEndian.Uint32(b[2:6]))
	if len(b) < 6+nsLen+dataLen {
		return Command{}, errors.New("raft command truncated")
	}
	var data []byte
	if dataLen > 0 {
		data = b[6+nsLen : 6+nsLen+dataLen]
	}
	return Command{
		Type:      CommandType(b[0]),
		Namespace: string(b[6 : 6+nsLen]),
		Data:      data,
	}, nil
}

type brokerFSM struct {
	server *Server
}

func (f *brokerFSM) Apply(log *raft.Log) interface{} {
	cmd, err := decodeCommand(log.Data)
	if err != nil {
		f.server.logger.Error("failed to decode raft command", zap.Error(err))
		return err
	}

	broker := f.server.getOrCreateBroker(cmd.Namespace)

	switch cmd.Type {
	case CmdProduce:
		if err := broker.Write(cmd.Data); err != nil {
			f.server.logger.Error("FSM Write failed", zap.Error(err))
			return err
		}
	case CmdConsume:
		if len(cmd.Data) < 8 {
			err := errors.New("CmdConsume missing index payload")
			f.server.logger.Error("FSM MarkCompleted failed", zap.Error(err))
			return err
		}
		index := int64(binary.BigEndian.Uint64(cmd.Data))
		if err := broker.MarkCompleted(index); err != nil {
			f.server.logger.Error("FSM MarkCompleted failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func (f *brokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.server.logger.Info("Starting FSM snapshot")
	return &brokerSnapshot{
		basePath: f.server.brokerFilepath,
	}, nil
}

func (f *brokerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	f.server.logger.Info("Restoring FSM from snapshot")

	// clear existing data directory
	os.RemoveAll(f.server.brokerFilepath)
	os.MkdirAll(f.server.brokerFilepath, 0755)

	// extract snapshot into the data directory
	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(f.server.brokerFilepath, header.Name)
		if header.FileInfo().IsDir() {
			os.MkdirAll(target, 0755)
			continue
		}

		os.MkdirAll(filepath.Dir(target), 0755)
		file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, header.FileInfo().Mode())
		if err != nil {
			return err
		}
		if _, err := io.Copy(file, tr); err != nil {
			file.Close()
			return err
		}
		file.Close()
	}

	// re-initialize all brokers
	f.server.mutex.Lock()
	defer f.server.mutex.Unlock()
	f.server.Brokers = make(map[string]*Broker)

	// get all namespaces from directories
	entries, err := os.ReadDir(f.server.brokerFilepath)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				ns := entry.Name()
				broker := NewBroker(0, ns, f.server.brokerFilepath, f.server.brokerWriteBufferSize)
				if err := broker.Initialize(); err == nil {
					f.server.Brokers[ns] = broker
				}
			}
		}
	}
	return nil
}

type brokerSnapshot struct {
	basePath string
}

func (s *brokerSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	// tar the entire base directory to the Raft sink
	tw := tar.NewWriter(sink)
	defer tw.Close()

	err := filepath.Walk(s.basePath, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// only tar the contents of the base file
		if file == s.basePath {
			return nil
		}

		relPath, _ := filepath.Rel(s.basePath, file)
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !fi.IsDir() {
			data, err := os.Open(file)
			if err != nil {
				return err
			}
			defer data.Close()
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (s *brokerSnapshot) Release() {}
