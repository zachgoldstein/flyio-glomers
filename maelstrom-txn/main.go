package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var kv *maelstrom.KV
var n *maelstrom.Node
var msgCounter int

func main() {
	n = maelstrom.NewNode()
	msgCounter = 0
	kv = maelstrom.NewLinKV(n)

	n.Handle("txn", handleTXN)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

// REQ
//
//	{
//	  "type": "txn",
//	  "msg_id": 3,
//	  "txn": [
//	    ["r", 1, null],
//	    ["w", 1, 6],
//	    ["w", 2, 9]
//	  ]
//	}
//
// RESP
//
//	{
//	  "type": "txn_ok",
//	  "msg_id": 1,
//	  "in_reply_to": 3,
//	  "txn": [
//	    ["r", 1, 3],
//	    ["w", 1, 6],
//	    ["w", 2, 9]
//	  ]
//	}
func handleTXN(msg maelstrom.Message) error {

	type TxnReq struct {
		Type  string      `json:"type"`
		MsgID int         `json:"msg_id"`
		Ops   []Operation `json:"txn"`
	}

	var msgReq TxnReq
	err := json.Unmarshal(msg.Body, &msgReq)
	if err != nil {
		log.Printf("Could not unmarshall body of txn msg, err: %v", err)
		return err
	}
	txn := Transaction{Operations: msgReq.Ops}

	// Try using a lock per key we are interacting with in the transaction
	// Using kv store, start transaction by locking relevant keys
	err = txn.Lock()
	if err != nil {
		log.Printf("Could not lock all keys, aborting err:%v", err)
		return err
	}

	// process reads and writes
	err = txn.Execute()
	if err != nil {
		log.Printf("Could not exeucte transaction, err: %v", err)
		return err
	}

	// commit transaction by unlocking key
	txn.Unlock()

	msgCounter += 1
	type TxnResp struct {
		Type      string      `json:"type"`
		MsgID     int         `json:"msg_id"`
		InReplyTo int         `json:"in_reply_to"`
		Ops       []Operation `json:"txn"`
	}
	ops := txn.Operations
	msgReply := TxnResp{
		Type:      "txn_ok",
		MsgID:     msgCounter,
		InReplyTo: msgReq.MsgID,
		Ops:       ops,
	}
	log.Printf("Replying to node %v with data: %v", msg.Src, msgReply)
	err = n.Reply(msg, msgReply)
	if err != nil {
		log.Printf("Error replying to send... %v", err)
	}

	return nil
}

func lockKey(key string) string {
	return fmt.Sprintf("%v-lock", key)
}

func dataKey(key string) string {
	return fmt.Sprintf("%v-data", key)
}

type Transaction struct {
	Operations []Operation
}

func (txn *Transaction) Keys() []string {
	keySet := map[string]bool{}
	for _, txn := range txn.Operations {
		keySet[fmt.Sprintf("%v", txn.Key)] = true
	}
	keys := []string{}
	for key := range keySet {
		keys = append(keys, key)
	}
	return keys
}

func (txn *Transaction) Lock() error {
	keys := txn.Keys()
	log.Printf("Keys to lock %v", keys)
	for _, key := range keys {
		err := txn.LockKey(key)
		if err != nil {
			log.Printf("Error locking key %v", err)
			return err
		}
	}
	log.Printf("Locked all keys %v", keys)

	return nil
}

func (txn *Transaction) LockKey(key string) error {
	lockKey := lockKey(key)
	log.Printf("Attempting to lock key %v", lockKey)

	maxRetries := 10
	sleepTime := 500 * time.Millisecond
	currRetries := 0
	for {
		if currRetries > maxRetries {
			log.Printf("Hit max retries, continuing")
			return fmt.Errorf("max retries when locking key %v", key)
		}
		ctxOffset, ctxOffsetCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer ctxOffsetCancel()
		err := kv.CompareAndSwap(ctxOffset, lockKey, "free", "locked", true)
		currRetries += 1
		if err != nil {
			log.Printf("RETRYING... Could not lock key %v from %v to %v, err: %v", lockKey, "free", "locked", err)
			time.Sleep(sleepTime)
			continue
		}
		return nil
	}
}

func (txn *Transaction) Unlock() error {
	keys := txn.Keys()
	log.Printf("Keys to unlock %v", keys)
	for _, key := range keys {
		err := txn.UnlockKey(key)
		if err != nil {
			log.Printf("Error locking key %v", err)
		}
	}

	return nil
}

func (txn *Transaction) UnlockKey(key string) error {
	log.Printf("Unlocking key %v", key)
	ctxOffset, ctxOffsetCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxOffsetCancel()
	lockKey := lockKey(key)
	err := kv.CompareAndSwap(ctxOffset, lockKey, "locked", "free", true)
	if err != nil {
		log.Printf("Could not lock key %v from %v to %v, err: %v", lockKey, "locked", "free", err)
		return err
	}
	return nil
}

func (txn *Transaction) Execute() error {
	log.Printf("Executing ops %v", txn.Operations)
	for _, op := range txn.Operations {
		err := op.Execute()
		if err != nil {
			log.Printf("Error executing operation %v", err)
		}
	}
	return nil
}

// func (txn *Transaction) MarshalJSON() ([]byte, error) {
// 	var txnBytes [][]byte
// 	for _, op := range txn.Operations {
// 		opBytes, err := json.Marshal([]interface{}{op.Type, op.Key, op.Value})
// 		if err != nil {
// 			return nil, err
// 		}
// 		txnBytes = append(txnBytes, opBytes)
// 	}
// 	return txnBytes, nil
// }

type Operation struct {
	Type  string // r is read, w is write
	Key   int    // can be null if write op
	Value int
}

// Operation is: ["r", 1, null],
func (op *Operation) UnmarshalJSON(p []byte) error {
	var tmp []interface{}
	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}
	op.Type = tmp[0].(string)
	op.Key = int(tmp[1].(float64))
	if op.Type == "w" {
		op.Value = int(tmp[2].(float64))
	} else {
		op.Value = -1
	}
	return nil
}

func (op *Operation) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{op.Type, op.Key, op.Value})
}

func (op *Operation) Execute() error {
	log.Printf("Executing operation type:%v, key:%v, value:%v", op.Type, op.Key, op.Value)
	if op.Type == "r" {
		readVal, err := op.Read()
		if err != nil {
			return err
		}
		op.Value = readVal
	} else if op.Type == "w" {
		return op.Write()
	}
	return nil
}

func (op *Operation) Read() (int, error) {
	dataKey := dataKey(fmt.Sprintf("%v", op.Key))
	log.Printf("Reading %v", dataKey)

	ctxOffset, ctxOffsetCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxOffsetCancel()
	readVal, err := kv.Read(ctxOffset, dataKey)
	rpcErr, ok := err.(*maelstrom.RPCError)
	if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
		log.Printf("Key does not exist %v, err:%v", dataKey, err)
		return -1, err
	}
	if err != nil {
		log.Printf("Could not read key %v, err:%v", dataKey, err)
		return -1, err
	}
	val := readVal.(int)
	log.Printf("Read %v", val)
	return val, err
}

func (op *Operation) Write() error {
	dataKey := dataKey(fmt.Sprintf("%v", op.Key))
	log.Printf("Writing %v with %v", dataKey, op.Value)

	ctxOffset, ctxOffsetCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer ctxOffsetCancel()
	err := kv.Write(ctxOffset, dataKey, op.Value)
	if err != nil {
		log.Printf("Could not write key %v to %v, err:%v", dataKey, op.Value, err)
		return err
	}
	log.Printf("Wrote %v with %v", dataKey, op.Value)
	return err
}

// func handleSend(msg maelstrom.Message) error {
// 	log.Printf("Handling send")
// 	var msgRecv struct {
// 		Type string `json:"type"`
// 		Key  string `json:"key"`
// 		Msg  int    `json:"msg"`
// 	}
// 	err := json.Unmarshal(msg.Body, &msgRecv)
// 	if err != nil {
// 		log.Printf("Could not unmarshall body of send msg, err: %v", err)
// 		return err
// 	}

// 	// Read offset for this key
// 	log.Printf("Reading offset for this key: %s", msgRecv.Key)
// 	offsetKey := offsetKey(msgRecv.Key)
// 	ctxWrite, ctxWriteCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
// 	defer ctxWriteCancel()
// 	readOffset, err := kv.Read(ctxWrite, offsetKey)
// 	if err != nil {
// 		log.Printf("Could not read log with offset %v. Setting to zero. err: %v", readOffset, err)
// 		readOffset = int(0)
// 	}

// 	// increment offset for this specific log and cas on kv store
// 	log.Printf("Incrementing offset for this key: %s, prev read offset is %v", msgRecv.Key, readOffset)
// 	castReadOffset := readOffset.(int)
// 	log.Printf("Cast ok????")
// 	ctxOffset, ctxOffsetCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
// 	defer ctxOffsetCancel()
// 	newOffsetKey := castReadOffset + int(1)
// 	err = kv.CompareAndSwap(ctxOffset, offsetKey, readOffset.(int), newOffsetKey, true)
// 	if err != nil {
// 		log.Printf("Could not increment offset for key %v from %v to %v, err: %v", offsetKey, readOffset, newOffsetKey, err)
// 		return err
// 	}

// 	// Read current log for this key
// 	logKey := logKey(msgRecv.Key, newOffsetKey)
// 	log.Printf("Reading log for this key: %s", msgRecv.Key)
// 	// ctxReadLog, ctxReadLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
// 	// defer ctxReadLogCancel()
// 	// readLog, err := kv.Read(ctxReadLog, logKey)
// 	// if err != nil {
// 	// 	log.Printf("Could not read log with key. Setting to zero %v, err: %v", logKey, err)
// 	// 	readLog = []LogMsg{}
// 	// }
// 	// // {"Msgs":[{"Offset":1,"Value":1}]}
// 	// // weird jank casting
// 	// // offsetList := readLog.(map[string]any)["Msgs"].([]LogMsg)
// 	// // panic: interface conversion: interface {} is map[string]interface {}, not main.LogMsgs
// 	// // panic: interface conversion: interface {} is main.LogMsgs, not map[string]interface {}
// 	// readLogAsserted, ok := readLog.([]LogMsg)
// 	// if !ok {
// 	// 	log.Printf("Could not assert readLog %v, to LogMsgs", readLog)
// 	// }
// 	// // map[msgs:[map[offset:1 value:1]]]
// 	// offsetList := readLogAsserted

// 	// TODO: store log in it's own key

// 	log.Printf("Writing log key %s = %s", logKey, msgRecv.Key)
// 	// logMsg := LogMsg{
// 	// 	Offset: newOffsetKey,
// 	// 	Value:  msgRecv.Msg,
// 	// }
// 	// newMsgs := append(offsetList, logMsg)
// 	// logData := newMsgs

// 	ctxWriteLog, ctxWriteLogCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
// 	defer ctxWriteLogCancel()
// 	err = kv.Write(ctxWriteLog, logKey, msgRecv.Msg)
// 	if err != nil {
// 		log.Printf("Could not write log %v, err: %v", logKey, err)
// 		return err
// 	}

// 	log.Printf("new offset key: %v", newOffsetKey)
// 	msgReply := map[string]any{
// 		"type":   "send_ok",
// 		"offset": newOffsetKey,
// 	}
// 	log.Printf("Replying to node %v with data: %v", msg.Src, msgReply)
// 	err = n.Reply(msg, msgReply)
// 	if err != nil {
// 		log.Printf("Error replying to send... %v", err)
// 	}

// 	return nil
// }

// func periodicTick() {

// }
