//Copyright [2022] [FreeTSDB]
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0

package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/server"
)

var (
	ErrEmptyPeers = errors.New("Failed to get MetaServerInfo: empty Peers")
)

func getNodeInfo(metaAddr string) (*meta.NodeInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/node", metaAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(string(b))
	}

	node := meta.NodeInfo{}
	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		if b, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf(string(b))
		}
	}
	return &node, err
}

func getMetaServers(metaAddr string) ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/meta-servers", metaAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(string(b))
	}

	peers := []string{}
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		if b, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf(string(b))
		}
	}

	return peers, nil
}

func addMetaServer(metaAddr, newNodeAddr string) error {
	peers, err := getMetaServers(metaAddr)
	if err != nil {
		return err
	}

	if len(peers) == 0 {
		return ErrEmptyPeers
	}

	b, err := json.Marshal(peers)
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/join-cluster", newNodeAddr),
		"application/json",
		bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	n := meta.NodeInfo{}
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
			return err
		}

	}

	fmt.Printf("Added meta node %d at %s\n", n.ID, n.Host)

	return nil
}

func removeMetaServer(metaAddr, remoteNodeAddr string) error {
	node, err := getNodeInfo(remoteNodeAddr)
	if err != nil {
		return err
	}
	if node == nil || node.TCPHost == "" {
		return fmt.Errorf("remote meta-server %s tcp host is not set", remoteNodeAddr)
	}

	n := &meta.NodeInfo{
		ID:      node.ID,
		Host:    remoteNodeAddr,
		TCPHost: node.TCPHost,
	}

	b, err := json.Marshal(n)
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/remove-meta", metaAddr),
		"application/json",
		bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
			return err
		}

	}

	fmt.Printf("Removed meta node %d at %s\n", n.ID, n.Host)

	return nil
}

func dial(network, address, header string) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte(header)); err != nil {
		return nil, fmt.Errorf("write mux header: %s", err)
	}
	return conn, nil
}

func addDataServer(metaAddr, newNodeAddr string) error {
	peers, err := getMetaServers(metaAddr)
	if err != nil {
		return err
	}

	if len(peers) == 0 {
		return ErrEmptyPeers
	}

	r := server.NodeRequest{}
	r.Type = server.RequestClusterJoin
	r.Peers = peers

	conn, err := dial("tcp", newNodeAddr, server.NodeMuxHeader)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(r); err != nil {
		return fmt.Errorf("Encode snapshot request: %s", err)
	}

	node := meta.NodeInfo{}
	if err := json.NewDecoder(conn).Decode(&node); err != nil {
		return err
	}

	fmt.Printf("Added data node %d at %s\n", node.ID, node.TCPHost)

	return nil
}

func removeDataServer(metaAddr, remoteNodeAddr string) error {
	peers, err := getMetaServers(metaAddr)
	if err != nil {
		return err
	}

	if len(peers) == 0 {
		return ErrEmptyPeers
	}

	metaClient := meta.NewRemoteClient()
	metaClient.SetMetaServers(peers)
	if err := metaClient.Open(); err != nil {
		return err
	}
	defer metaClient.Close()

	n, err := metaClient.DataNodeByTCPHost(remoteNodeAddr)
	if err != nil {
		return err
	}

	if err := metaClient.DeleteDataNode(n.ID); err != nil {
		return err
	}

	fmt.Printf("Removed data node %d at %s\n", n.ID, n.TCPHost)

	return nil
}

func updateDataNode(metaAddr, oldNode, newNode string) error {
	peers, err := getMetaServers(metaAddr)
	if err != nil {
		return err
	}

	if len(peers) == 0 {
		return ErrEmptyPeers
	}

	request := &server.NodeRequest{
		Type:     server.RequestUpdateDataNode,
		Peers:    peers,
		OldAddr:  oldNode,
		NodeAddr: newNode,
	}

	conn, err := dial("tcp", newNode, server.NodeMuxHeader)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return fmt.Errorf("Encode snapshot request: %s", err.Error())
	}

	rsp := server.NodeResponse{}
	if err := json.NewDecoder(conn).Decode(&rsp); err != nil {
		return err
	}

	fmt.Printf("update data node %s to %s\n", oldNode, newNode)

	return nil
}
