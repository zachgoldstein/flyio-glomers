package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// var kv maelstrom.KV
var n *maelstrom.Node
var kv *maelstrom.KV

func main() {
	n = maelstrom.NewNode()
	kv = maelstrom.NewLinKV(n)

	n.Handle("send", handleSend)
	n.Handle("poll", handlePoll)
	n.Handle("commit_offsets", handleCommitOffsets)
	n.Handle("list_committed_offsets", handleListCommittedOffsets)

	periodicTicker := time.NewTicker(1000 * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-periodicTicker.C:
				periodicTick()
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	// Run should run indefinitely, but as a last resort, sleep 1 minute then stop
	time.Sleep(1 * time.Minute)
	periodicTicker.Stop()
	done <- true
	log.Println("Ticker stopped")
}

type LogMsgs struct {
	Msgs []LogMsg `json:"msgs"`
}

type LogMsg struct {
	Offset int
	Value  int
}

func offsetKey(logName string) string {
	return fmt.Sprintf("offset-%v", logName)
}

func logKey(logName string, offset int) string {
	return fmt.Sprintf("log-%v-%v", logName, offset)
}

func commitKey(logName string) string {
	return fmt.Sprintf("commit-%v", logName)
}

//	REQ {
//	  "type": "send",
//	  "key": "k1",
//	  "msg": 123
//	}
//
//	RESP {
//	  "type": "send_ok",
//	  "offset": 1000
//	}
//
// This message requests that a "msg" value be appended to a log identified by "key"
func handleSend(msg maelstrom.Message) error {
	log.Printf("Handling send")
	var msgRecv struct {
		Type string `json:"type"`
		Key  string `json:"key"`
		Msg  int    `json:"msg"`
	}
	err := json.Unmarshal(msg.Body, &msgRecv)
	if err != nil {
		log.Printf("Could not unmarshall body of send msg, err: %v", err)
		return err
	}

	// Read offset for this key
	log.Printf("Reading offset for this key: %s", msgRecv.Key)
	offsetKey := offsetKey(msgRecv.Key)
	ctxWrite, ctxWriteCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxWriteCancel()
	readOffset, err := kv.Read(ctxWrite, offsetKey)
	if err != nil {
		log.Printf("Could not read log with offset %v. Setting to zero. err: %v", readOffset, err)
		readOffset = int(0)
	}

	// increment offset for this specific log and cas on kv store
	log.Printf("Incrementing offset for this key: %s, prev read offset is %v", msgRecv.Key, readOffset)
	castReadOffset := readOffset.(int)
	log.Printf("Cast ok????")
	ctxOffset, ctxOffsetCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxOffsetCancel()
	newOffsetKey := castReadOffset + int(1)
	err = kv.CompareAndSwap(ctxOffset, offsetKey, readOffset.(int), newOffsetKey, true)
	if err != nil {
		log.Printf("Could not increment offset for key %v from %v to %v, err: %v", offsetKey, readOffset, newOffsetKey, err)
		return err
	}

	// Read current log for this key
	logKey := logKey(msgRecv.Key, newOffsetKey)
	log.Printf("Reading log for this key: %s", msgRecv.Key)
	// ctxReadLog, ctxReadLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	// defer ctxReadLogCancel()
	// readLog, err := kv.Read(ctxReadLog, logKey)
	// if err != nil {
	// 	log.Printf("Could not read log with key. Setting to zero %v, err: %v", logKey, err)
	// 	readLog = []LogMsg{}
	// }
	// // {"Msgs":[{"Offset":1,"Value":1}]}
	// // weird jank casting
	// // offsetList := readLog.(map[string]any)["Msgs"].([]LogMsg)
	// // panic: interface conversion: interface {} is map[string]interface {}, not main.LogMsgs
	// // panic: interface conversion: interface {} is main.LogMsgs, not map[string]interface {}
	// readLogAsserted, ok := readLog.([]LogMsg)
	// if !ok {
	// 	log.Printf("Could not assert readLog %v, to LogMsgs", readLog)
	// }
	// // map[msgs:[map[offset:1 value:1]]]
	// offsetList := readLogAsserted

	// TODO: store log in it's own key

	log.Printf("Writing log key %s = %s", logKey, msgRecv.Key)
	// logMsg := LogMsg{
	// 	Offset: newOffsetKey,
	// 	Value:  msgRecv.Msg,
	// }
	// newMsgs := append(offsetList, logMsg)
	// logData := newMsgs

	ctxWriteLog, ctxWriteLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxWriteLogCancel()
	err = kv.Write(ctxWriteLog, logKey, msgRecv.Msg)
	if err != nil {
		log.Printf("Could not write log %v, err: %v", logKey, err)
		return err
	}

	log.Printf("new offset key: %v", newOffsetKey)
	msgReply := map[string]any{
		"type":   "send_ok",
		"offset": newOffsetKey,
	}
	log.Printf("Replying to node %v with data: %v", msg.Src, msgReply)
	err = n.Reply(msg, msgReply)
	if err != nil {
		log.Printf("Error replying to send... %v", err)
	}

	return nil
}

//	REQ {
//	  "type": "poll",
//	  "offsets": {
//	    "k1": 1000,
//	    "k2": 2000
//	  }
//	}
//
//	RESP {
//	  "type": "poll_ok",
//	  "msgs": {
//	    "k1": [[1000, 9], [1001, 5], [1002, 15]],
//	    "k2": [[2000, 7], [2001, 2]]
//	  }
//	}
//
// This message requests that a node return messages from a set of logs starting from the given offset in each log.
func handlePoll(msg maelstrom.Message) error {
	log.Printf("Handling poll")
	var msgRecv struct {
		Type    string         `json:"type"`
		Offsets map[string]int `json:"offsets"`
	}
	err := json.Unmarshal(msg.Body, &msgRecv)
	if err != nil {
		log.Printf("Could not unmarshall body of send msg, err: %v", err)
		return err
	}

	type msgReplyStruct struct {
		Type string             `json:"type"`
		Msgs map[string][][]int `json:"msgs"`
	}
	msgReply := msgReplyStruct{
		Type: "poll_ok",
		Msgs: map[string][][]int{},
	}

	log.Printf("Retrieving logs and scanning for subset")

	for key, offset := range msgRecv.Offsets {
		replyData := [][]int{}
		// Get log for this key
		// Start at offset, request keys for all data up to current offset
		log.Printf("Reading offset for this key: %s", key)
		offsetKey := offsetKey(key)
		ctxWrite, ctxWriteCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer ctxWriteCancel()
		readOffset, err := kv.Read(ctxWrite, offsetKey)
		if err != nil {
			log.Printf("Could not read log latest offset when reading key %v continuing. err: %v", offsetKey, err)
			continue
		}

		startOffset := offset
		keysToPoll := readOffset.(int) - startOffset + 1 // We actually start to store at 1, b/c of early increment
		log.Printf("Reading kv %v times (%v-%v) for poll of log %s", keysToPoll, readOffset.(int), startOffset, key)

		for i := 0; i < keysToPoll; i++ {
			currOffset := startOffset + i
			keyToPoll := logKey(key, currOffset)
			ctxReadLog, ctxReadLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer ctxReadLogCancel()
			readLog, err := kv.Read(ctxReadLog, keyToPoll)
			if err != nil {
				log.Printf("Could not read log with key %v, err: %v", keyToPoll, err)
				continue
			}
			replyData = append(replyData, []int{currOffset, readLog.(int)})

		}
		msgReply.Msgs[key] = replyData
	}

	log.Printf("Replying to node %v with data: %v", msg.Src, msgReply)
	err = n.Reply(msg, msgReply)
	if err != nil {
		log.Printf("Error replying to poll... %v", err)
	}

	return nil
}

//	REQ {
//	  "type": "commit_offsets",
//	  "offsets": {
//	    "k1": 1000,
//	    "k2": 2000
//	  }
//	}
//
//	RESP {
//	  "type": "commit_offsets_ok"
//	}
//
// This message informs the node that messages have been successfully processed up to and including the given offset.
func handleCommitOffsets(msg maelstrom.Message) error {
	log.Printf("Handling commit offsets")
	// store the committed offset for each key
	var msgRecv struct {
		Type    string         `json:"type"`
		Offsets map[string]int `json:"offsets"`
	}
	err := json.Unmarshal(msg.Body, &msgRecv)
	if err != nil {
		log.Printf("Could not unmarshall body of send msg, err: %v", err)
		return err
	}

	log.Printf("Storing commit_offsets")
	for key, offset := range msgRecv.Offsets {
		commitKey := commitKey(key)
		ctxWriteLog, ctxWriteLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer ctxWriteLogCancel()
		err = kv.Write(ctxWriteLog, commitKey, offset)
		if err != nil {
			log.Printf("Could not write commit offset %s, err: %v", commitKey, err)
			return err
		}
	}
	msgReply := map[string]any{
		"type": "commit_offsets_ok",
	}
	log.Printf("Replying to node %v with data: %v", msg.Src, msgReply)
	err = n.Reply(msg, msgReply)
	if err != nil {
		log.Printf("Error replying to commit offsets... %v", err)
	}

	return nil
}

//	REQ {
//	  "type": "list_committed_offsets",
//	  "keys": ["k1", "k2"]
//	}
//
//	RESP {
//	  "type": "list_committed_offsets_ok",
//	  "offsets": {
//	    "k1": 1000,
//	    "k2": 2000
//	  }
//	}
//
// This message returns a map of committed offsets for a given set of logs.
// Clients use this to figure out where to start consuming from in a given log.
func handleListCommittedOffsets(msg maelstrom.Message) error {
	log.Printf("Handling list committed offsets")

	// read the committed offset for each key
	var msgRecv struct {
		Type string   `json:"type"`
		Keys []string `json:"keys"`
	}
	err := json.Unmarshal(msg.Body, &msgRecv)
	if err != nil {
		log.Printf("Could not unmarshall body of send msg, err: %v", err)
		return err
	}

	type msgReplyStruct struct {
		Type    string         `json:"type"`
		Offsets map[string]int `json:"offsets"`
	}
	msgReply := msgReplyStruct{
		Type:    "list_committed_offsets_ok",
		Offsets: map[string]int{},
	}

	log.Printf("Retrieving commit_offsets")
	for _, key := range msgRecv.Keys {
		commitKey := commitKey(key)
		ctxReadLog, ctxReadLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer ctxReadLogCancel()
		offset, err := kv.Read(ctxReadLog, commitKey)
		if err != nil {
			log.Printf("Could not read commit offset %s, err: %v", commitKey, err)
			return err
		}
		msgReply.Offsets[key] = offset.(int)
	}

	log.Printf("Replying to node %v with data: %v", msg.Src, msgReply)
	err = n.Reply(msg, msgReply)
	if err != nil {
		log.Printf("Error replying to commit offsets... %v", err)
	}

	return nil
}

func periodicTick() {

}

// var storeMutex sync.RWMutex = sync.RWMutex{}

// var dataStore map[string]StoreData = map[string]StoreData{}

// type StoreData struct {
// 	Message float64
// 	Node    string
// }

// func (m *StoreData) messageKey() string {
// 	key := fmt.Sprintf("%v", m.Node)
// 	log.Printf("returning message key %v", key)
// 	return key
// }

// func writeStore(data StoreData) {
// 	storeMutex.Lock()
// 	dataStore[data.messageKey()] = data
// 	storeMutex.Unlock()
// }

// func hasData(data StoreData) bool {
// 	storeMutex.RLock()
// 	defer storeMutex.RUnlock()
// 	_, ok := dataStore[data.messageKey()]
// 	return ok
// }

// func readStore() []StoreData {
// 	storeMutex.RLock()
// 	defer storeMutex.RUnlock()
// 	allStoreData := []StoreData{}
// 	for _, v := range dataStore {
// 		allStoreData = append(allStoreData, v)
// 	}
// 	return allStoreData
// }

// func readStoreCurrentNode() StoreData {
// 	return readStoreNode(n.ID())
// }

// func readStoreNode(nodeID string) StoreData {
// 	storeMutex.RLock()
// 	defer storeMutex.RUnlock()
// 	for _, v := range dataStore {
// 		if v.Node == nodeID {
// 			return v
// 		}
// 	}
// 	return StoreData{}
// }
