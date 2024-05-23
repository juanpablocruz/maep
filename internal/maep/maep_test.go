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

	o := maep.NewOperation(b.Bytes(), []string{"test"})
	o2 := maep.NewOperation(b.Bytes(), []string{"test 2"})
	op_list := []maep.Operation{o}

	ob1, err := n.AddOperation(op_list)
	if err != nil {
		t.Errorf("Error adding operation: %s", err)
	}
	mr1 := n.VersionTree.ShortRoot()
	log.Println(mr1)

	ob2, err := n.AddOperation([]maep.Operation{o2})
	if err != nil {
		t.Errorf("Error adding operation: %s", err)
	}
	mr2 := n.VersionTree.ShortRoot()
	log.Println(mr2)

	// Verify the tree
	_, err = n.VersionTree.VerifyTree()
	if err != nil {
		t.Errorf("Error verifying tree: %s", err)
	}

	log.Println("OP1")

	hash_str := ob1.GetHash()
	block, path, err := n.FindHash(hash_str)
	if err != nil {
		t.Errorf("Error finding hash: %s", err)
	}

	if block != nil {
		ops := n.OperationMap[hash_str]

		log.Printf("%v\n", ops)
	}
	log.Printf("%s\n", path)

	log.Println("OP2")

	hash_str = ob2.GetHash()
	block, path, err = n.FindHash(hash_str)
	if err != nil {
		t.Errorf("Error finding hash: %s", err)
	}
	if block != nil {
		ops := n.OperationMap[hash_str]

		if len(ops) > 0 {
			log.Printf("%v\n", ops[0])
		}

	}
	log.Printf("%s\n", path)
}
