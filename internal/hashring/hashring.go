/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hashring

import (
	//
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"sync"

	//
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
)

// HashRing is a consistent hashing implementation
type HashRing struct {
	sync.RWMutex

	//
	nodes           []Node
	vnodesPerNode   int
	binlogPositions []BinlogPositions
}

// Node represents a server in the hash ring
type Node struct {
	hash   int
	server string
}

// BinlogPositions represents the binlog position for the server
type BinlogPositions struct {
	Server         string
	BinlogPosition uint32
	BinlogFile     string
}

// BinlogResponse represents the response for the binlog position from the API
type BinlogResponse struct {
	File     string `json:"file"`
	Position uint32 `json:"position"`
}

// NewHashRing creates a new HashRing
func NewHashRing(vnodesPerNode int) *HashRing {
	return &HashRing{
		vnodesPerNode: vnodesPerNode,
	}
}

// AddServer adds a server to the hash ring
func (h *HashRing) AddServer(app *v1alpha1.Application, server string) {
	h.Lock()
	defer h.Unlock()

	//
	for i := 0; i < h.vnodesPerNode; i++ {
		vnode := server + "#" + strconv.Itoa(i)
		hash := int(crc32.ChecksumIEEE([]byte(vnode)))
		h.nodes = append(h.nodes, Node{hash: hash, server: server})
	}

	// Add the binlog position for the server to the hashring.
	// If the server is the same as the current server, use the current binlog position
	if server == app.Config.ServerId {
		h.binlogPositions = append(h.binlogPositions, BinlogPositions{Server: server,
			BinlogPosition: app.BinLogPosition, BinlogFile: app.BinLogFile})
	}

	// If the server is different, get the binlog position from the server via HTTP request
	if server != app.Config.ServerId {
		binlogPosition, binLogFile, err := h.GetServerBinlogPosition(server)
		if err != nil {
			app.Logger.Error(fmt.Sprintf("Error getting binlog position for server %s", server), zap.Error(err))
			// If there is an error getting the binlog position, use the current binlog position for this server
			binlogPosition = app.BinLogPosition
		}
		// Add the binlog position to the hashring
		h.binlogPositions = append(h.binlogPositions, BinlogPositions{Server: server,
			BinlogPosition: binlogPosition, BinlogFile: binLogFile})
	}

	//
	sort.Slice(h.nodes, func(i, j int) bool {
		return h.nodes[i].hash < h.nodes[j].hash
	})
}

// RemoveServer removes a server from the hash ring
func (h *HashRing) RemoveServer(server string) {
	h.Lock()
	defer h.Unlock()

	//
	var newNodes []Node
	for _, node := range h.nodes {
		if node.server != server {
			newNodes = append(newNodes, node)
		}
	}
	h.nodes = newNodes

	// Remove the binlog position for the server who lefts the hashring
	var newBinLogPositions []BinlogPositions
	for _, node := range h.binlogPositions {
		if node.Server != server {
			newBinLogPositions = append(newBinLogPositions, node)
		}
	}
	h.binlogPositions = newBinLogPositions
}

// GetServer returns the server for a given key
func (h *HashRing) GetServer(key string) string {
	h.RLock()
	defer h.RUnlock()

	//
	if len(h.nodes) == 0 {
		return ""
	}
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	idx := sort.Search(len(h.nodes), func(i int) bool {
		return h.nodes[i].hash >= hash
	})
	if idx == len(h.nodes) {
		idx = 0
	}
	return h.nodes[idx].server
}

// GetServerList returns the list of servers in the hash ring
// This function is useful as servers can be defined by static configuration
// or discovered by DNS
func (h *HashRing) GetServerList() (servers []string) {
	h.RLock()
	defer h.RUnlock()

	//
	numRealNodes := len(h.nodes)
	if h.vnodesPerNode != 0 {
		numRealNodes = len(h.nodes) / h.vnodesPerNode
	}

	for _, nodeValue := range h.nodes {

		if !slices.Contains(servers, nodeValue.server) {
			servers = append(servers, nodeValue.server)
		}

		if len(servers) == numRealNodes {
			break
		}
	}

	// Sorting is performed to ensure that the order of servers is always the same
	// This will help to avoid unnecessary changes for the functions using this list
	slices.Sort(servers)

	return servers
}

// SyncBinLogPositions syncs the binlog positions for all the nodes in the hashring
func (h *HashRing) SyncBinLogPositions(app *v1alpha1.Application) {
	h.Lock()
	defer h.Unlock()

	// Iterate over the binlog positions and update the binlog position for the server
	for i, node := range h.binlogPositions {

		// If the server is the same as the current server, use the current binlog position
		if node.Server == app.Config.ServerId {
			app.Logger.Debug("syncing binlog positions for this server", zap.String("file", app.BinLogFile),
				zap.Uint32("position", app.BinLogPosition))
			h.binlogPositions[i].BinlogPosition = app.BinLogPosition
			h.binlogPositions[i].BinlogFile = app.BinLogFile
			continue
		}

		// If the server is different, get the binlog position from the server via HTTP
		binlogPosition, binLogFile, err := h.GetServerBinlogPosition(node.Server)
		if err != nil {
			app.Logger.Error(fmt.Sprintf("Error getting binlog position for server %s, using this server positions",
				node.Server), zap.Error(err))
			// If there is an error getting the binlog position, use the current binlog position for this server
			binlogPosition = app.BinLogPosition
			binLogFile = app.BinLogFile
		}
		app.Logger.Debug(fmt.Sprintf("syncing binlog positions for %s server", node.Server),
			zap.String("file", binLogFile), zap.Uint32("position", binlogPosition))
		h.binlogPositions[i].BinlogPosition = binlogPosition
		h.binlogPositions[i].BinlogFile = binLogFile
	}
}

// GetServerBinlogPosition returns the binlog position for a given server via HTTP request
func (h *HashRing) GetServerBinlogPosition(server string) (position uint32, file string, err error) {

	// Create the HTTP client
	c := &http.Client{}

	// Create the HTTP request to http://server/position path which returns the binlog position as JSON
	// { "file": "mysql-bin.000001", "position": 1234 }
	r, err := http.NewRequest("GET", fmt.Sprintf("http://%s/position", server), nil)
	if err != nil {
		return position, file, fmt.Errorf("error creating HTTP Request for webhook integration: %v", err)
	}

	rsp, err := c.Do(r)
	if err != nil {
		return position, file, fmt.Errorf("error sending HTTP request for webhook integration: %v", err)
	}

	defer rsp.Body.Close()

	// Decode the JSON response
	var binlogData BinlogResponse
	err = json.NewDecoder(rsp.Body).Decode(&binlogData)
	if err != nil {
		return 0, "", fmt.Errorf("error decoding JSON: %v", err)
	}

	return binlogData.Position, binlogData.File, nil
}

// GetServerBinlogPositionMem returns the binlog position for a given server from memory store
func (h *HashRing) GetServerBinlogPositionMem(server string) (position uint32, file string, err error) {

	servers := h.binlogPositions
	for _, s := range servers {
		if s.Server == server {
			return s.BinlogPosition, s.BinlogFile, nil
		}
	}

	return position, file, fmt.Errorf("server %s not found in binlog positions", server)
}

// String returns a string representation of the hashring
func (h *HashRing) String() string {
	servers := h.GetServerList()
	str := "{"
	for _, v := range servers {
		str += fmt.Sprintf("[host: '%s']", v)
	}
	str += "}"
	return str
}
