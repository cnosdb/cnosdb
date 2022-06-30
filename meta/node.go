package meta

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const (
	nodeFile = "node.json"
)

type Node struct {
	path  string
	ID    uint64
	Peers []string
}

// LoadNode will load the node information from disk if present
func LoadNode(path, fileName string) (*Node, error) {
	nodeFile := nodeFile
	if fileName != "" {
		nodeFile = fileName
	}
	n := &Node{
		path: path,
	}

	f, err := os.Open(filepath.Join(path, nodeFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(n); err != nil {
		return nil, err
	}

	return n, nil
}

// NewNode will return a new node
func NewNode(path string) *Node {
	return &Node{
		path: path,
	}
}

// Save will save the node file to disk and replace the existing one if present
func (n *Node) Save(fileName string) error {
	nodeFile := nodeFile
	if fileName != "" {
		nodeFile = fileName
	}
	file := filepath.Join(n.path, nodeFile)
	tmpFile := file + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	if err = json.NewEncoder(f).Encode(n); err != nil {
		f.Close()
		return err
	}

	if err = f.Close(); nil != err {
		return err
	}

	return os.Rename(tmpFile, file)
}
