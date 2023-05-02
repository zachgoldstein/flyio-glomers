package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()

	n.Handle("read", handleRead)
	n.Handle("add", handleAdd)
	n.Handle("count", handleCount)

	tickerReadNodeCount := time.NewTicker(1000 * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-tickerReadNodeCount.C:
				mergeNodeCounts()
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	// Run should run indefinitely, but as a last resort, sleep 1 minute then stop
	time.Sleep(1 * time.Minute)
	tickerReadNodeCount.Stop()
	done <- true
}

//	{
//	  "type": "add",
//	  "delta": 123
//	}
//
// return { "type": "add_ok" }
// handleAdd accepts add requests and increments the local memory store for the current node
func handleAdd(msg maelstrom.Message) error {
	err := n.Reply(msg, map[string]any{
		"type": "add_ok",
	})
	if err != nil {
		fmt.Printf("Could not reply to add, err: %s", err)
		return err
	}

	type msgStruct struct {
		Type  string  `json:"type"`
		Delta float64 `json:"delta"`
	}
	var msgData msgStruct
	if err = json.Unmarshal(msg.Body, &msgData); err != nil {
		fmt.Printf("error unmarshalling add message body: %s\n", err)
		return err
	}
	// Increment local counter for this node by delta
	addCountForNode(n.ID(), msgData.Delta)

	return nil
}

type readReplyStruct struct {
	Type  string  `json:"type"`
	Value float64 `json:"value"`
}

// {"type": "read"}
// return:
//
//	{
//	  "type": "read_ok",
//	  "value": 1234
//	}
//
// handleRead accepts read requests and returns the current value of the global counter.
// It adds together the counts in local memory for all nodes and returns the sum
func handleRead(msg maelstrom.Message) error {
	storeDataSlice := readStore()

	sum := 0.0
	for _, storeData := range storeDataSlice {
		sum += storeData.Message
	}

	replyData := readReplyStruct{
		Type:  "read_ok",
		Value: sum,
	}
	err := n.Reply(msg, replyData)
	if err != nil {
		log.Printf("Error replying... %v", err)
	}

	return err
}

// handleCount replies to msgs with the current count for only this node
func handleCount(msg maelstrom.Message) error {
	storeData := readStoreCurrentNode()
	err := n.Reply(msg, map[string]any{
		"type":  "count",
		"value": storeData.Message,
	})
	if err != nil {
		log.Printf("Error replying... %v", err)
	}

	return err
}

// mergeNodeCounts sends a "count" message to all other nodes, storing
// in local memory the count for each other node
func mergeNodeCounts() {
	for _, neighbor := range n.NodeIDs() {
		if n.ID() == neighbor {
			continue
		}
		go mergeNodeCount(neighbor)
	}
}

// mergeNodeCount sends a count request to another node, storing the result in local memory
func mergeNodeCount(node string) {
	ctx, ctxCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxCancel()
	msg := map[string]any{
		"type": "count",
	}
	msgReply, err := n.SyncRPC(ctx, node, msg)
	if err != nil {
		log.Printf("Could not send message %v to node %v, err %v", msg, node, err)
	}

	type msgReplyStruct struct {
		Type  string  `json:"type"`
		Value float64 `json:"value"`
	}
	var msgReplyData msgReplyStruct
	if err = json.Unmarshal(msgReply.Body, &msgReplyData); err != nil {
		log.Printf("error unmarshalling manifest body: %s\n", err)
	}
	setCountForNode(node, msgReplyData.Value)
}

// addCountForNode will read the current count for a given node in local memory,
// add the givent count to that, then set the
func addCountForNode(nodeID string, count float64) {
	nodeCurrCount := 0.0
	currentData := readStoreNode(nodeID)
	if currentData.Node == "" {
		log.Printf("Attempting to addCountForNode with zero, warning")
	}
	nodeCurrCount = currentData.Message
	sum := nodeCurrCount + count
	writeStore(StoreData{
		Node:    nodeID,
		Message: sum,
	})
}

// setCountForNode sets the count associated with a specific node in local memory
func setCountForNode(nodeID string, count float64) {
	writeStore(StoreData{
		Node:    nodeID,
		Message: count,
	})
}

type StoreData struct {
	Message float64
	Node    string
}

func (m *StoreData) messageKey() string {
	key := fmt.Sprintf("%v", m.Node)
	return key
}

var storeMutex sync.RWMutex = sync.RWMutex{}

var dataStore map[string]StoreData = map[string]StoreData{}

func writeStore(data StoreData) {
	storeMutex.Lock()
	dataStore[data.messageKey()] = data
	storeMutex.Unlock()
}

func readStore() []StoreData {
	storeMutex.RLock()
	defer storeMutex.RUnlock()
	allStoreData := []StoreData{}
	for _, v := range dataStore {
		allStoreData = append(allStoreData, v)
	}
	return allStoreData
}

func readStoreCurrentNode() StoreData {
	return readStoreNode(n.ID())
}

func readStoreNode(nodeID string) StoreData {
	storeMutex.RLock()
	defer storeMutex.RUnlock()
	for _, v := range dataStore {
		if v.Node == nodeID {
			return v
		}
	}
	return StoreData{}
}
