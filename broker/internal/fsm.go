package synapse

import (
	"archive/tar"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type CommandType string

const (
	CmdProduce CommandType = "produce"
	CmdConsume CommandType = "consume"
)

type Command struct {
	Type      CommandType `json:"type"`
	Namespace string      `json:"namespace"`
	Data      []byte      `json:"data,omitempty"`
}

type brokerFSM struct {
	server *Server
}

func (f *brokerFSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.server.logger.Error("failed to unmarshal raft command", zap.Error(err))
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
		if err := broker.AdvanceReadIndex(); err != nil {
			f.server.logger.Error("FSM AdvanceReadIndex failed", zap.Error(err))
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
