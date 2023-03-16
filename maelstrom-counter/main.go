package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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
		// Unmarshal the message body as an loosely-typed map.
		var body generateMsgReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		resp := generateMsgResp{
			Type: "generate_ok",
			Id:   "",
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
