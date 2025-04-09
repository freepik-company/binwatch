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
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	//
	"binwatch/api/v1alpha1"
)

// HashRing is a consistent hashing implementation
type HashRing struct {
	sync.RWMutex

	//
	nodes         []Node
	vnodesPerNode int
}

// Node represents a server in the hash ring
type Node struct {
	hash   int
	server string
}

// NewHashRing creates a new HashRing
func NewHashRing(vnodesPerNode int) *HashRing {
	return &HashRing{
		vnodesPerNode: vnodesPerNode,
	}
}

func (h *HashRing) Replace(servers []string) {
	h.Lock()
	defer h.Unlock()

	h.nodes = []Node{}
	for _, sv := range servers {
		h.AddServer(sv)
	}
}

// AddServer adds a server to the hash ring
func (h *HashRing) AddServer(server string) {
	h.Lock()
	defer h.Unlock()

	//
	for i := 0; i < h.vnodesPerNode; i++ {
		vnode := server + "#" + strconv.Itoa(i)
		hash := int(crc32.ChecksumIEEE([]byte(vnode)))
		h.nodes = append(h.nodes, Node{hash: hash, server: server})
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

// GetRedisLogPos
func GetRedisLogPos(app *v1alpha1.Application) (uint32, string, error) {
	result, err := app.RedisClient.Get(app.Context, fmt.Sprintf("%s-%s", app.Config.Hashring.Redis.KeyPrefix,
		app.Config.ServerId)).Result()
	if err == redis.Nil {
		app.Logger.Info(fmt.Sprintf("No binlog position found in memory store for server %s", app.Config.ServerId))
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("error getting binlog position from memory store: %s", err)
	}
	app.Logger.Debug(fmt.Sprintf("Redis position from memory store: %s: %s", fmt.Sprintf("%s-%s", app.Config.Hashring.Redis.KeyPrefix,
		app.Config.ServerId), result))

	// Split the result to get the file and position
	parts := strings.Split(result, "/")
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("error parsing binlog position from memory store: %s", result)
	}
	position64, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("error parsing binlog position from memory store: %s", err)
	}
	app.Logger.Debug(fmt.Sprintf("getting binlog position from memory store for server %s: %s/%s", app.Config.ServerId, parts[0], parts[1]))
	return uint32(position64), parts[0], nil
}

// SetRedisLogPos
func SetRedisLogPos(app *v1alpha1.Application) error {
	err := app.RedisClient.Set(app.Context, fmt.Sprintf("%s-%s", app.Config.Hashring.Redis.KeyPrefix,
		app.Config.ServerId), fmt.Sprintf("%s/%d", app.BinLogFile, app.BinLogPosition), 0).Err()
	return err
}
