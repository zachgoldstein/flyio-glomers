package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageData struct {
	Message float64
}

func (m *MessageData) messageKey() string {
	return fmt.Sprintf("%v", m.Message)
}

var storeMutex sync.RWMutex = sync.RWMutex{}

var dataStore map[string]MessageData = map[string]MessageData{}

var n *maelstrom.Node

func writeStore(data MessageData) {
	storeMutex.Lock()
	dataStore[data.messageKey()] = data
	storeMutex.Unlock()
}

func hasData(data MessageData) bool {
	storeMutex.RLock()
	_, ok := dataStore[data.messageKey()]
	storeMutex.RUnlock()
	return ok
}

func readStore() []MessageData {
	storeMutex.RLock()
	allMessageData := []MessageData{}
	for _, v := range dataStore {
		allMessageData = append(allMessageData, v)
	}
	storeMutex.RUnlock()
	return allMessageData
}

type readMsgResp struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type topologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type sendReadMsg struct {
	Type     string    `json:"type"`
	Messages []float64 `json:"messages"`
}

var currTopology map[string][]string

var activeNeighborsMap map[string]bool
var partitionedNeighborsMap map[string]bool

func activeNeighbors() []string {
	var neighbors []string
	for k, _ := range activeNeighborsMap {
		neighbors = append(neighbors, k)
	}
	return neighbors
}

func partitionedNeighbors() []string {
	var neighbors []string
	for k, _ := range partitionedNeighborsMap {
		neighbors = append(neighbors, k)
	}
	return neighbors
}

func handleBroadcast(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message, ok := body["message"].(float64)
	if !ok {
		return fmt.Errorf("could not convert message to float64, message %s", body["message"])
	}

	msgData := MessageData{
		Message: message,
	}
	// Store the message
	writeStore(msgData)

	err := n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})

	// if hasData(msgData) {
	// 	log.Printf("Already received message with data %v", message)
	// 	return n.Reply(msg, map[string]any{
	// 		"type": "broadcast_ok",
	// 	})
	// }

	// Topology and push vs pull propagation determines msgs-per-op and latency fundamentally

	// Broadcasting to all other nodes couples broadcast requests to msgs-per-op and latency
	// Alternative is to do it periodically, tune it with ticker time there
	// log.Println("sending new broadcast data to other nodes")
	// sendBroadcastMsgToActiveNeighbors(map[string]any{
	// 	"type":    "broadcast",
	// 	"message": message,
	// })

	// This works great for 5 nodes, but broadcast replies start to fail for 25 nodes
	// I think b/c the jepsen sender is torn down by the time we go to reply
	// sendBroadcastMsgToAll(map[string]any{
	// 	"type":    "broadcast",
	// 	"message": message,
	// })

	log.Println("broadcast replying")
	// Echo the original message back with the updated message type.
	return err
}

func sendBroadcastMsgToNeighbor(neighbour string, msg any) {
	ctx, ctxCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxCancel()
	_, err := n.SyncRPC(ctx, neighbour, msg)
	if err != nil {
		log.Printf("Could not send message %v to neighbour %v, err %v", msg, neighbour, err)
		markPartitionedNeighbor(neighbour)
		couldNotSendMsg(neighbour, msg)
	}
	log.Printf("Successfully sent message %v to neighbour %v, reply msg %v", msg, neighbour, msg)
}

func sendBroadcastMsgToActiveNeighbors(msg any) {
	for _, neighbour := range activeNeighbors() {
		go sendBroadcastMsgToNeighbor(neighbour, msg)
	}
}

func sendBroadcastMsgToAll(msg any) {
	for k := range currTopology {
		if k == n.ID() {
			continue
		}
		go sendBroadcastMsgToNeighbor(k, msg)
	}
}

var unsentMsgs []unsentMSG

type unsentMSG struct {
	Src  string
	Dest string
	Body any
}

func couldNotSendMsg(neighbour string, body any) {
	msg := unsentMSG{
		Src:  n.ID(),
		Dest: neighbour,
		Body: body,
	}
	log.Printf("Queuing msg to send later %v", msg)
	unsentMsgs = append(unsentMsgs, msg)
}

func sendLatestQueueMsg() {
	if len(unsentMsgs) == 0 {
		return
	}
	msg := unsentMsgs[len(unsentMsgs)-1]
	unsentMsgs = unsentMsgs[:len(unsentMsgs)-1]
	sendBroadcastMsgToActiveNeighbors(msg.Body)
	// sendBroadcastMsgToAll(msg.Body)
	log.Printf("Attempting to send queued msg %v", msg)
}

func sendAllQueueMsgs() {
	log.Printf("Attempting to send all queued msgs () %v", len(unsentMsgs))
	for _, msg := range unsentMsgs {
		// sendBroadcastMsgToActiveNeighbors(msg.Body)
		sendBroadcastMsgToActiveNeighbors(msg.Body)
		log.Printf("Attempting to send queued msg %v", msg)
	}
	unsentMsgs = []unsentMSG{}
}

// F
func markPartitionedNeighbor(partitionedNeighbor string) {
	log.Printf("Marking neighbor %s as partitioned", partitionedNeighbor)
	delete(activeNeighborsMap, partitionedNeighbor)
	partitionedNeighborsMap[partitionedNeighbor] = true

	// Add the partitioned neighbor's neighbors to our active neighbors
	neighbours, ok := currTopology[partitionedNeighbor]
	if !ok {
		log.Fatalf("Could not find neighbors of partitioned node %v", partitionedNeighbor)
	}
	for _, neighbor := range neighbours {
		activeNeighborsMap[neighbor] = true
	}
	log.Printf("New neighbors %v", activeNeighbors())
}

func healPartition() error {
	// Attempt to read from partitioned nodes and load missing values
	for _, neighbor := range partitionedNeighbors() {
		log.Printf("Checking on partitioned neighbor %v", neighbor)
		ctx, ctxCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer ctxCancel()
		readResp, err := n.SyncRPC(ctx, neighbor, map[string]any{
			"type": "read",
		})
		if err != nil {
			log.Printf("Error trying to heal partition: %v", err)
			return err
		}
		log.Printf("Partition heal read msg received!")
		var readMsg sendReadMsg
		if err := json.Unmarshal(readResp.Body, &readMsg); err != nil {
			log.Printf("Could not use read msg after healing partition: %v", err)
			return err
		}

		log.Printf("Healing partition with data %v", readMsg)
		for _, msg := range readMsg.Messages {
			writeStore(MessageData{
				Message: msg,
			})
		}

		log.Printf("Fixing original topology for this neighbor %v", readMsg)
		// remove partitionedNeighbors, add back to activeNeighbors
		activeNeighborsMap[neighbor] = true
		delete(partitionedNeighborsMap, neighbor)

		// remove added neighbours,
		neighbors, ok := currTopology[neighbor]
		if !ok {
			log.Fatalf("Could not find neighbors of partitioned node %v", neighbor)
		}
		for _, neighbor := range neighbors {
			delete(activeNeighborsMap, neighbor)
		}
		log.Printf("New neighbors %v", activeNeighbors())
	}
	return nil
}

func handleRead(msg maelstrom.Message) error {
	data := readStore()
	// {
	// "type": "read_ok",
	// "messages": [1, 8, 72, 25]
	// }

	var respData []int
	for _, msg := range data {
		respData = append(respData, int(msg.Message))
	}

	readResp := readMsgResp{
		Type:     "read_ok",
		Messages: respData,
	}

	return n.Reply(msg, readResp)
}

// {
// "type": "topology",
//
//		"topology": {
//			"n1": ["n2", "n3"],
//			"n2": ["n1"],
//			"n3": ["n1"]
//		}
//	}
//
// example topology: "n0":["n3","n1"],"n1":["n4","n2","n0"],"n2":["n1"],"n3":["n0","n4"],"n4":["n1","n3"]
// is it cylic??
// n0 -> ["n3","n1"] 	-> n3 ["n0","n4"] 		-> n4 ["n1","n3"] -> n1
//
//	-> n1 ["n4","n2","n0"]	-> n2 ["n4","n2"]
//							-> n4 ["n1","n3"] -> n3 ["n0","n4"] -> n0
//
// Yes.....
func handleTopology(msg maelstrom.Message) error {
	var tmsg topologyMsg
	if err := json.Unmarshal(msg.Body, &tmsg); err != nil {
		return err
	}
	log.Printf("Got topology and unmarshalled msg, %v \n", tmsg)
	currTopology = tmsg.Topology
	log.Printf("topology , %v \n", currTopology)

	neighbours, ok := currTopology[n.ID()]
	if !ok {
		log.Fatal("Could not neighbours for current node in topology call, very weird")
	}
	for _, neighbor := range neighbours {
		activeNeighborsMap[neighbor] = true
	}

	return n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

// Proactively issue read requests to other nodes, merge their messages into this node
func triggerMergeAll() {
	for _, k := range n.NodeIDs() {
		if k == n.ID() {
			continue
		}
		go readNodeData(k)
	}
}

// Pick random % of nodes to merge with
func triggerMergeSome(percentage float64) {

	shufflesNodes := n.NodeIDs()
	rand.Shuffle(len(shufflesNodes), func(i, j int) {
		shufflesNodes[i], shufflesNodes[j] = shufflesNodes[j], shufflesNodes[i]
	})

	numNodes := math.Floor(float64(len(shufflesNodes)) * percentage)
	log.Printf("Merging with %v nodes vs total %v", len(shufflesNodes[:int(numNodes)]), len(n.NodeIDs()))
	for _, k := range shufflesNodes[:int(numNodes)] {
		if k == n.ID() {
			continue
		}
		go readNodeData(k)
	}
}

func triggerMergeNeighbors() {
	for _, k := range activeNeighbors() {
		if k == n.ID() {
			continue
		}
		go readNodeData(k)
	}
}

func readNodeData(node string) error {
	ctx, ctxCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer ctxCancel()
	readResp, err := n.SyncRPC(ctx, node, map[string]any{
		"type": "read",
	})
	if err != nil {
		log.Printf("Error trying to read other node data: %v", err)
		return err
	}
	log.Printf("Received node read msg!")
	var readMsg sendReadMsg
	if err := json.Unmarshal(readResp.Body, &readMsg); err != nil {
		log.Printf("Could not use read msg after healing partition: %v", err)
		return err
	}

	log.Printf("Merging node %v data %v", node, readMsg)
	for _, msg := range readMsg.Messages {
		writeStore(MessageData{
			Message: msg,
		})
	}
	return nil
}

func main() {

	n = maelstrom.NewNode()
	activeNeighborsMap = map[string]bool{}
	partitionedNeighborsMap = map[string]bool{}

	// {
	// "type": "broadcast",
	// "message": 1000
	// }
	n.Handle("broadcast", handleBroadcast)

	// {"type": "read"}
	n.Handle("read", handleRead)

	n.Handle("topology", handleTopology)

	// Periodically check queue for unsent message and retry sending them
	tickerSendQueue := time.NewTicker(200 * time.Millisecond)
	tickerHealPartition := time.NewTicker(200 * time.Millisecond)
	tickerReadNodes := time.NewTicker(1000 * time.Millisecond)
	tickerReadNodesSampling := time.NewTicker(100 * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-tickerHealPartition.C:
				healPartition()
			case <-tickerSendQueue.C:
				sendAllQueueMsgs()
			case <-tickerReadNodes.C:
				triggerMergeAll()
				// triggerMergeSome(0.90)
				// triggerMergeNeighbors()
				// triggerMergeSome(0.60)
			case <-tickerReadNodesSampling.C:
				triggerMergeSome(0.10)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	// Run will run indefinitely, but as a last resort, sleep 1 minute then stop
	time.Sleep(1 * time.Minute)
	tickerHealPartition.Stop()
	tickerSendQueue.Stop()
	tickerReadNodes.Stop()
	tickerReadNodesSampling.Stop()
	done <- true
	log.Println("Ticker stopped")
}
