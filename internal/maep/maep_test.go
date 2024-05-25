package maep_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/juanpablocruz/maep/internal/maep"
)

type Argument struct {
	Args map[string]interface{}
}

func TestPrint(t *testing.T) {
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
	o3 := maep.NewOperation(b.Bytes(), []string{"test 3"})

	_, err := n.AddOperation(o)
	if err != nil {
		t.Errorf("Error adding operation: %s", err)
	}
	_, err = n.AddOperation(o2)
	if err != nil {
		t.Errorf("Error adding operation: %s", err)
	}
	_, err = n.AddOperation(o3)
	if err != nil {
		t.Errorf("Error adding operation: %s", err)
	}

	fmt.Printf("Clock: %d\n", n.Clock.Now().Ticks)
	fmt.Printf("\n%s\n\n", n.Print())
}

func TestGetDiff(t *testing.T) {
	n := maep.NewNode()
	args := Argument{
		Args: map[string]interface{}{
			"test":  "test",
			"test2": "test2",
		},
	}
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(args)

	op_list := make([]*maep.OperationBlock, 0)
	for i := 0; i < 10; i++ {
		o := maep.NewOperation(b.Bytes(), []string{fmt.Sprintf("test %d", i)})
		ob, err := n.AddOperation(o)
		if err != nil {
			t.Errorf("Error adding operation: %s", err)
		}
		op_list = append(op_list, ob)
	}

	fmt.Printf("Get diff since %s\n", op_list[3].GetShortHash())

	_, path, err := n.FindHash(op_list[3].GetHash())
	if err != nil {
		t.Errorf("Error finding hash: %s", err)
	}
	if len(path) != 6 {
		t.Errorf("Error finding hash: %s", err)
	}

	diff, err := n.GetDiff(op_list[3].GetHash())
	if err != nil {
		t.Errorf("Error getting diff: %s", err)
	}
	fmt.Println(diff)
}
