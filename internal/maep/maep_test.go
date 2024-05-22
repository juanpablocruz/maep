package maep_test

import (
	"bytes"
	"encoding/gob"
	"log"
	"testing"

	"github.com/juanpablocruz/maep/internal/maep"
)

type Argument struct {
	Args map[string]interface{}
}

func TestMain(t *testing.T) {
	// This is a test function

	n := maep.NewNode()

	args := Argument{
		Args: map[string]interface{}{
			"test":  "test",
			"test2": "test2",
		},
	}
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(args)

	n.AddOperation([]maep.Operation{
		{
			Arguments: b.Bytes(),
			Id:        "1",
			Data:      []string{"test"},
			Timestamp: 0,
		},
	})

	mr := n.VersionTree.Root()
	log.Println(mr)
	// Verify the tree
	vt, err := n.VersionTree.VerifyTree()
	if err != nil {
		t.Errorf("Error verifying tree: %s", err)
	}
	log.Println("Verified tree: ", vt)

	log.Println(n.VersionTree.Print())
}
