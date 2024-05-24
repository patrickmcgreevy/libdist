package main

import (
	"fmt"
	"math"
	"strconv"
	"sync"
)

func main() {
	var N int = 100
	processes := make([]*proc, N)
	for i := 0; i < N; i++ {
		processes[i] = NewProc()
		processes[i].id = i
	}

	for i := 0; i < N; i++ {
		// The channel must be a *buffered* channel!
		cur := processes[i]
		left := processes[(i+1)%N]
		cur.leftRecvChan = left.rightSendChan
		left.rightRecvChan = cur.leftSendChan
	}

	for _, v := range processes {
		fmt.Println(v)
	}

    // elected, unelected := ElectLeaderN2(processes)
    elected, unelected := ElectLeaderNLogN(processes)
	fmt.Println(elected, unelected)
}

/*
processes must be slice of pointesr to processes that have been initialized
with shared channels and unique, sequential ids.
*/
func ElectLeaderNLogN(processes []*proc) (elected []*proc, unelected []*proc) {
	var wg sync.WaitGroup
	size := len(processes)
	elected = make([]*proc, 0, 1)
	unelected = make([]*proc, 0, size)

	for _, p := range processes {
		wg.Add(1)
		go func(p *proc) {
			defer wg.Done()
			electLeaderNlogNStateMachine(p)
		}(p)
	}

	wg.Wait()

	for _, p := range processes {
		if p.IsLeader() {
			elected = append(elected, p)
		} else {
			unelected = append(unelected, p)
		}
	}

	return elected, unelected
}

func electLeaderNlogNStateMachine(p *proc) {
	var err error = nil
	var j, k, d int = 0, 0, 0
	var msg string
	var receivedProbes map[string]int = make(map[string]int)
	// Send round-0 messages
	p.SendLeft(fmtProbeMsg(p.id, 0, 1))
	p.SendRight(fmtProbeMsg(p.id, 0, 1))

	for i := 0; true; {
		if len(p.rightRecvChan) > 0{
            i = 0
			msg = p.RecvRight()
		} else  if len(p.leftRecvChan) > 0 {
            i = 1
			msg = p.RecvLeft()
		} else {
            continue
        }

		if msg == TerminationMsg {
			// fmt.Printf("%d got termination msg", p.id)
			p.terminate()
			return
		}

		if j, k, d, err = scanProbeMsg(msg); err == nil {
			// do probe stuff
			if j == p.id {
				p.isLeader = true
				fmt.Printf("proc %d elected as leader!\n", p.id)
				p.terminate()
				return
			} else if (j > p.id) && (d < int(math.Pow(2, float64(k)))) {
				if i%2 == 0 {
					p.SendLeft(fmtProbeMsg(j, k, d+1))
				} else {
					p.SendRight(fmtProbeMsg(j, k, d+1))
				}
			} else if (j > p.id) && (d >= int(math.Pow(2, float64(k)))) {
				if i%2 == 0 {
					p.SendRight(fmtReplyMsg(j, k))
				} else {
					p.SendLeft(fmtReplyMsg(j, k))
				}
			} else {
				//pass
			}
		} else if j, k, err = scanReplyMsg(msg); err == nil {
			// do reply stuff
			if j != p.id {
				// forward reply
				if i%2 == 0 {
					p.SendLeft(fmtReplyMsg(j, k))
				} else {
					p.SendRight(fmtReplyMsg(j, k))
				}
			} else {
				if _, ok := receivedProbes[msg]; !ok {
					// fmt.Printf("proc %d adding '%s' to received replies\n", p.id, msg)
					receivedProbes[msg] = 1
				} else {
					// fmt.Printf("proc %d won round %d!\n", p.id, k)
					p.SendLeft(fmtProbeMsg(p.id, k+1, 1))
					p.SendRight(fmtProbeMsg(p.id, k+1, 1))
				}
			}
		} else {
			fmt.Printf("Panicked on message: '%s'", msg)
			panic(err.Error())
		}
	}
}

/*
processes must be slice of pointesr to processes that have been initialized
with shared channels and unique, sequential ids.
*/
func ElectLeaderN2(processes []*proc) (elected []*proc, unelected []*proc) {
	var wg sync.WaitGroup
	size := len(processes)
	elected = make([]*proc, 0, 1)
	unelected = make([]*proc, 0, size)

	for _, p := range processes {
		wg.Add(1)
		go func(p *proc) {
			defer wg.Done()
			electLeaderN2StateMachine(p)
		}(p)
	}

	wg.Wait()

	for _, p := range processes {
		if p.IsLeader() {
			elected = append(elected, p)
		} else {
			unelected = append(unelected, p)
		}
	}

	return elected, unelected
}

func electLeaderN2StateMachine(p *proc) {
	p.SendRight(fmt.Sprintf(IdMsgFmt, p.id))
	for {
		rightMsg := p.RecvLeft()
		if rightMsg == TerminationMsg {
			p.SendRight(rightMsg)
			return
		}
		rId, err := strconv.ParseInt(rightMsg, 10, 64)
		// rIdInt := int(rId)
		if err != nil {
			fmt.Printf("no errors are allowed but an error ocurred!! %s\n", err.Error())
			panic(err.Error())
		}
		if rId == int64(p.id) {
			p.isLeader = true
			p.SendRight(TerminationMsg)
			fmt.Printf("Node %d has been elected leader!\n", p.id)
			return
		} else if rId > int64(p.id) {
			p.SendRight(rightMsg)
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
	id            int
	isLeader      bool
	leftSendChan  chan string
	leftRecvChan  chan string
	rightSendChan chan string
	rightRecvChan chan string
}

func NewProc() *proc {
	return &proc{
		id:            -1,
		isLeader:      false,
		leftRecvChan:  make(chan string, 100),
		leftSendChan:  make(chan string, 100),
		rightSendChan: make(chan string, 100),
		rightRecvChan: make(chan string, 100),
	}
}

func (p *proc) String() string {
	return fmt.Sprintf("processes %d: sl(%x) rl(%x) sr(%x) rr(%x)",
		p.id,
		p.leftSendChan,
		p.leftRecvChan,
		p.rightSendChan,
		p.rightRecvChan)
}

func (p *proc) IsLeader() bool {
	return p.isLeader
}

func (p *proc) SendLeft(msg string) {
	p.leftSendChan <- msg
	// fmt.Printf(LoggingMsgFmt, p.id, "sent left", msg)
}

func (p *proc) RecvLeft() string {
	msg := <-p.leftRecvChan
	// fmt.Printf(LoggingMsgFmt, p.id, "received left", msg)
	return msg
}

func (p *proc) SendRight(msg string) {
	p.rightSendChan <- msg
	// fmt.Printf(LoggingMsgFmt, p.id, "sent right", msg)
}

func (p *proc) RecvRight() string {
	msg := <-p.rightRecvChan
	// fmt.Printf(LoggingMsgFmt, p.id, "received right", msg)
	return msg
}

func (p *proc) terminate() {
	p.SendLeft(TerminationMsg)
}

func fmtProbeMsg(id, phase, hops int) string {
	return fmt.Sprintf(ProbeMsgFmt, id, phase, hops)
}

func scanProbeMsg(msg string) (id, phase, hops int, err error) {
	_, err = fmt.Sscanf(msg, ProbeMsgFmt, &id, &phase, &hops)

	return id, phase, hops, err
}

func fmtReplyMsg(id, phase int) string {
	return fmt.Sprintf(ReplyMsgFmt, id, phase)
}

func scanReplyMsg(msg string) (id, phase int, err error) {
	_, err = fmt.Sscanf(msg, ReplyMsgFmt, &id, &phase)

	return id, phase, err
}

const (
	IdMsgFmt       string = "%d"
	TerminationMsg string = "Terminate"
	ProbeMsgFmt    string = "%d:%d:%d"
	ReplyMsgFmt    string = "%d:%d"
	LoggingMsgFmt  string = "proc %d %s '%s'\n"
)
