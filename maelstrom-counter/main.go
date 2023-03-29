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
	log.Println("Ticker stopped")
}

//	{
//	  "type": "add",
//	  "delta": 123
//	}
//
// return { "type": "add_ok" }
// handleAdd accepts add requests and increments the local memory store for the current node
func handleAdd(msg maelstrom.Message) error {
	log.Printf("Handling add")

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

	log.Printf("Adding delta to current node %v", msgData.Delta)
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
	log.Printf("Handling read")
	storeDataSlice := readStore()
	log.Printf("Node counts %v", storeDataSlice)

	sum := 0.0
	for _, storeData := range storeDataSlice {
		log.Printf("Summing from data: %v", storeData)
		sum += storeData.Message
	}
	log.Printf("Counted up node counts, sum: %v", sum)

	replyData := readReplyStruct{
		Type:  "read_ok",
		Value: sum,
	}
	log.Printf("Replying to node %v with data: %v", msg.Src, replyData)
	err := n.Reply(msg, replyData)
	if err != nil {
		log.Printf("Error replying... %v", err)
	}

	return err
}

// handleCount replies to msgs with the current count for only this node
func handleCount(msg maelstrom.Message) error {
	log.Printf("Handling count")
	storeData := readStoreCurrentNode()
	log.Printf("Returning count %v", storeData.Message)
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
	log.Printf("Merging node counts")
	for _, neighbor := range n.NodeIDs() {
		if n.ID() == neighbor {
			continue
		}
		go mergeNodeCount(neighbor)
	}
}

// mergeNodeCount sends a count request to another node, storing the result in local memory
func mergeNodeCount(node string) {
	log.Printf("Attempting to get count from other node %v", node)
	ctx, ctxCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxCancel()
	msg := map[string]any{
		"type": "count",
	}
	msgReply, err := n.SyncRPC(ctx, node, msg)
	if err != nil {
		log.Printf("Could not send message %v to node %v, err %v", msg, node, err)
	}
	log.Printf("Successfully sent message %v to node %v, reply msg %v", msg, node, msgReply)

	type msgReplyStruct struct {
		Type  string  `json:"type"`
		Value float64 `json:"value"`
	}
	var msgReplyData msgReplyStruct
	if err = json.Unmarshal(msgReply.Body, &msgReplyData); err != nil {
		log.Printf("error unmarshalling manifest body: %s\n", err)
	}
	log.Printf("Merging count from other node %v", node)
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
	log.Printf("Adding current count %v to new delta %v, setting to %v", nodeCurrCount, count, sum)
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
	log.Printf("returning message key %v", key)
	return key
}

var storeMutex sync.RWMutex = sync.RWMutex{}

var dataStore map[string]StoreData = map[string]StoreData{}

func writeStore(data StoreData) {
	storeMutex.Lock()
	dataStore[data.messageKey()] = data
	storeMutex.Unlock()
}

func hasData(data StoreData) bool {
	storeMutex.RLock()
	defer storeMutex.RUnlock()
	_, ok := dataStore[data.messageKey()]
	return ok
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
