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

const BroadcastType = BroadcastActiveNeighbors
const BroadcastSamplePercentage float64 = 0.5
const BroadcastTimeout = 500 * time.Millisecond

const PeriodicSyncType = PeriodicSyncAll
const PeriodicSyncInterval = 1800 * time.Millisecond
const PeriodicSamplePercentage float64 = 0.9

const MessageTimeout = 500 * time.Millisecond

const (
	BroadcastAll             string = "all"
	BroadcastSample          string = "sample"
	BroadcastActiveNeighbors string = "activeNeighbors"
	BroadcastNone            string = "none"
)

const (
	PeriodicSyncAll       string = "periodicSyncAll"
	PeriodicSyncSample    string = "periodicSyncSample"
	PeriodicSyncNone      string = "periodicSyncNone"
	PeriodicSyncNeighbors string = "periodicSyncNeighbors"
)

func main() {

	n = maelstrom.NewNode()
	activeNeighborsMap = map[string]bool{}
	partitionedNeighborsMap = map[string]bool{}

	n.Handle("broadcast", handleBroadcast)
	n.Handle("read", handleRead)
	n.Handle("topology", handleTopology)

	// Periodically check queue for unsent message and retry sending them
	tickerSendQueue := time.NewTicker(200 * time.Millisecond)
	tickerHealPartition := time.NewTicker(200 * time.Millisecond)
	tickerReadNodes := time.NewTicker(PeriodicSyncInterval)
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
				switch PeriodicSyncType {
				case PeriodicSyncAll:
					triggerMergeAll()
				case PeriodicSyncSample:
					triggerMergeSome(PeriodicSamplePercentage)
				case PeriodicSyncNeighbors:
					triggerMergeNeighbors()
				}
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
	done <- true
	log.Println("Ticker stopped")
}

// {
// "type": "read_ok",
// "messages": [1, 8, 72, 25]
// }
// handleRead sends a reply message with all messages currently stored in memory
func handleRead(msg maelstrom.Message) error {
	data := readStore()

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
// handleTopology sends a reply message to acknowledge the given topology
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

// {
// "type": "broadcast",
// "message": 1000
// }
// handleBroadcast replies to ack a new message, then propagates this new message out to the network
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

	if hasData(msgData) {
		log.Printf("Already received message with data %v", message)
		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	}
	broadcastMsg := map[string]any{
		"type":    "broadcast",
		"message": message,
	}

	// Send push broadcast with new message data to other nodes
	switch BroadcastType {
	case BroadcastAll:
		sendBroadcastMsgToAll(broadcastMsg)
	case BroadcastSample:
		sendBroadcastMsgToSample(broadcastMsg)
	case BroadcastActiveNeighbors:
		sendBroadcastMsgToActiveNeighbors(broadcastMsg)
	}
	return err
}

// sendBroadcastMsgToNeighbor will send the given message to a neighbor,
// and if the message errors, either by timeout or something else, stick
// the message in a queue to send later, and mark the neighbor as partitioned
func sendBroadcastMsgToNeighbor(neighbour string, msg any) {
	ctx, ctxCancel := context.WithTimeout(context.Background(), BroadcastTimeout)
	defer ctxCancel()
	_, err := n.SyncRPC(ctx, neighbour, msg)
	if err != nil {
		log.Printf("Could not send message %v to neighbour %v, err %v", msg, neighbour, err)
		markPartitionedNeighbor(neighbour)
		couldNotSendMsg(neighbour, msg)
	}
	log.Printf("Successfully sent message %v to neighbour %v, reply msg %v", msg, neighbour, msg)
}

// sendBroadcastMsgToActiveNeighbors sends the newly received broadcast message
// to only the "neighbors". These neighbors are based off the provided topology
func sendBroadcastMsgToActiveNeighbors(msg any) {
	log.Println("sending new broadcast data to active neighbor nodes")
	for _, neighbour := range activeNeighbors() {
		go sendBroadcastMsgToNeighbor(neighbour, msg)
	}
}

// sendBroadcastMsgToAll sends the newly received broadcast message to all other nodes
func sendBroadcastMsgToAll(msg any) {
	log.Println("sending new broadcast data to all other nodes")
	for k := range currTopology {
		if k == n.ID() {
			continue
		}
		go sendBroadcastMsgToNeighbor(k, msg)
	}
}

// sendBroadcastMsgToSample sends the newly received broadcast message to
// only some other nodes, based on the BroadcastSamplePercentage
func sendBroadcastMsgToSample(msg any) {
	randomNodes := getRandomNodeSample(BroadcastSamplePercentage)
	for _, k := range randomNodes {
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

func sendAllQueueMsgs() {
	log.Printf("Attempting to send all queued msgs () %v", len(unsentMsgs))
	for _, msg := range unsentMsgs {
		sendBroadcastMsgToActiveNeighbors(msg.Body)
		log.Printf("Attempting to send queued msg %v", msg)
	}
	unsentMsgs = []unsentMSG{}
}

// markPartitionedNeighbor stores a reference to a node that is partitioned
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

// healPartition attempts to read data from previously partitioned nodes,
// if it does not error out, it will read the nodes data, then remove it from
// the map of partitioned neighbors
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

// getRandomNodeSample returns a random sample of all nodes
func getRandomNodeSample(percentage float64) []string {
	shufflesNodes := n.NodeIDs()
	rand.Shuffle(len(shufflesNodes), func(i, j int) {
		shufflesNodes[i], shufflesNodes[j] = shufflesNodes[j], shufflesNodes[i]
	})

	numNodes := math.Floor(float64(len(shufflesNodes)) * percentage)
	return shufflesNodes[:int(numNodes)]
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
	randomNodes := getRandomNodeSample(percentage)
	log.Printf("Merging with %v nodes vs total %v", len(randomNodes), len(n.NodeIDs()))
	for _, k := range randomNodes {
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

// readNodeData will send a read message to another node, then store all the messages
// that are returned
func readNodeData(node string) error {
	ctx, ctxCancel := context.WithTimeout(context.Background(), MessageTimeout)
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
