package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// generateUID returns a uid, based on the unix nanosecond time, a secure random number and the node id
func generateUID(nodeID string) string {
	timeNs := time.Now().UnixNano()
	// generate 10 random numbers with crypto/rand
	c := 10
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("error generating UID:", err)
		return ""
	}
	return fmt.Sprintf("%v-%v-%v", timeNs, big.NewInt(0).SetBytes(b).Uint64(), nodeID)
}

type generateMsgReq struct {
	Type string
}

type generateMsgResp struct {
	Type string `json:"type"`
	Id   string `json:"id"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body generateMsgReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		resp := generateMsgResp{
			Type: "generate_ok",
			Id:   generateUID(n.ID()),
		}

		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
