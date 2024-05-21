package main

import (
	"fmt"
	"strconv"
	"sync"
)

func main() {
	var N int = 10
	processes := make([]*proc, N)
	for i := 0; i < N; i++ {
		processes[i] = NewProc()
		processes[i].id = i
	}

	for i := 0; i < N; i++ {
        // I don't think that I can have a channel go both ways like this. 
        // Each proc should have 4 unidirectional channels for communication
		c := make(chan string)
		processes[i].leftChan = c
		processes[(i+1)%N].rightChan = c
	}

    for _, v := range processes {
        fmt.Printf("%v\n", v)
    }

    ElectLeader(processes)
}

/*
processes must be slice of pointesr to processes that have been initialized
with shared channels and unique, sequential ids.
*/
func ElectLeader(processes []*proc) (elected []*proc, unelected []*proc) {
	var wg sync.WaitGroup
	size := len(processes)
	elected = make([]*proc, 1)
	unelected = make([]*proc, size)

	for _, p := range processes {
		wg.Add(1)
		go func(p *proc) {
			defer wg.Done()
			processStateMachine(p)
		}(p)
	}

	wg.Wait()

	return elected, unelected
}

func processStateMachine(p *proc) {
	p.SendLeft(fmt.Sprintf(IdMsgFmt, p.id))
	for {
		rightMsg := p.RecvRight()
        if rightMsg == TerminationMsg {
            p.SendLeft(rightMsg)
            return
        }
		rId, err := strconv.ParseInt(rightMsg, 10, 64)
        // rIdInt := int(rId)
		if err != nil {
			fmt.Printf("no errors are allowed but an error ocurred!! %s\n", err.Error())
			return
		}
        if rId == int64(p.id) {
            p.isLeader = true
            p.SendLeft(TerminationMsg)
            fmt.Printf("Node %d has been elected leader!\n", p.id)
            return
        } else if rId > int64(p.id) {
            p.SendLeft(rightMsg)
        } else {
            continue
        }
	}
}

type Participant interface {
	IsLeader() bool
	SendLeft(string)
	RecvLeft() string
	SendRight(string)
	RecvRight() string
}

type proc struct {
	id        int
	isLeader  bool
	leftChan  chan string
	rightChan chan string
}

func NewProc() *proc {
	return &proc{
		id:        -1,
		isLeader:  false,
		leftChan:  make(chan string),
		rightChan: make(chan string),
	}
}

func (p *proc) IsLeader() bool {
	return p.isLeader
}

func (p *proc) SendLeft(msg string) {
	p.leftChan <- msg
}

func (p *proc) RecvLeft() string {
	return <-p.leftChan
}

func (p *proc) SendRight(msg string) {
	p.rightChan <- msg
}

func (p *proc) RecvRight() string {
	return <-p.rightChan
}

const (
	IdMsgFmt       string = "%d"
	TerminationMsg string = "Terminate"
)
