package main

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
)

func main() {
	var N int = 10000

    processes := NewProcSlice(N)

	participants := make([]Participant, len(processes))
	for i, v := range processes {
		participants[i] = v
	}

	t0 := time.Now()
	Cluster(participants).RunElection(electLeaderNlogNStateMachine)
	dur2 := time.Since(t0)
	fmt.Println("nlogn alg took: ", dur2)
}

type Cluster []Participant

// func NewCluster(participants ...Participant) Cluster {
//     c := make(Cluster, 0, 100)
//     for _, p := range participants {
//         c = append(c, p)
//     }
//
//     return c
// }

func (processes Cluster) RunElection(stateMachine func(Participant)) (elected []Participant, unelected []Participant) {
	var wg sync.WaitGroup
	size := len(processes)
	elected = make([]Participant, 0, 1)
	unelected = make([]Participant, 0, size)

	for _, p := range processes {
		wg.Add(1)
		go func(p Participant) {
			defer wg.Done()
			stateMachine(p)
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

func (c Cluster) ElectLeaderNLogN() (elected []Participant, unelected []Participant) {
	var wg sync.WaitGroup
	size := len(c)
	elected = make([]Participant, 0, 1)
	unelected = make([]Participant, 0, size)

	for _, p := range c {
		wg.Add(1)
		go func(p Participant) {
			defer wg.Done()
			electLeaderNlogNStateMachine(p)
		}(p)
	}

	wg.Wait()

	for _, p := range c {
		if p.IsLeader() {
			elected = append(elected, p)
		} else {
			unelected = append(unelected, p)
		}
	}

	return elected, unelected
}

func electLeaderNlogNStateMachine(p Participant) {
	var err error = nil
	var id, round, hops int = 0, 0, 0
	var msg string
	var dir Direction
	var receivedProbes map[string]int = make(map[string]int)

	// Send round-0 messages
	p.SendLeft(fmtProbeMsg(p.Id(), 0, 1))
	p.SendRight(fmtProbeMsg(p.Id(), 0, 1))

	for {
		msg, dir = p.WaitForMsg()

		if msg == TerminationMsg {
			p.Terminate()
			return
		}

		if id, round, hops, err = scanProbeMsg(msg); err == nil {
			// do probe stuff
			if id == p.Id() {
				p.SetLeader(true)
				fmt.Printf("proc %d elected as leader!\n", p.Id())
				p.Terminate()
				return
			} else if (id > p.Id()) && (hops < int(math.Pow(2, float64(round)))) {
				if dir == Right {
					p.SendLeft(fmtProbeMsg(id, round, hops+1))
				} else {
					p.SendRight(fmtProbeMsg(id, round, hops+1))
				}
			} else if (id > p.Id()) && (hops >= int(math.Pow(2, float64(round)))) {
				if dir == Right {
					p.SendRight(fmtReplyMsg(id, round))
				} else {
					p.SendLeft(fmtReplyMsg(id, round))
				}
			} else {
				//pass
			}
		} else if id, round, err = scanReplyMsg(msg); err == nil {
			// do reply stuff
			if id != p.Id() {
				// forward reply
				if dir == Right {
					p.SendLeft(fmtReplyMsg(id, round))
				} else {
					p.SendRight(fmtReplyMsg(id, round))
				}
			} else {
				if _, ok := receivedProbes[msg]; !ok {
					// fmt.Printf("proc %d adding '%s' to received replies\n", p.id, msg)
					receivedProbes[msg] = 1
				} else {
					// fmt.Printf("proc %d won round %d!\n", p.id, k)
					p.SendLeft(fmtProbeMsg(p.Id(), round+1, 1))
					p.SendRight(fmtProbeMsg(p.Id(), round+1, 1))
				}
			}
		} else {
			fmt.Printf("Panicked on message: '%s'", msg)
			panic(err.Error())
		}
	}
}

func electLeaderN2StateMachine(p Participant) {
	p.SendRight(fmt.Sprintf(IdMsgFmt, p.Id()))
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
		if rId == int64(p.Id()) {
			p.SetLeader(true)
			p.SendRight(TerminationMsg)
			fmt.Printf("Node %d has been elected leader!\n", p.Id())
			return
		} else if rId > int64(p.Id()) {
			p.SendRight(rightMsg)
		} else {
			continue
		}
	}
}

type Participant interface {
	Id() int
	IsLeader() bool
	SetLeader(bool)
	SendLeft(string)
	RecvLeft() string
	SendRight(string)
	RecvRight() string
	WaitForMsg() (string, Direction)
	Terminate()
}

var _ Participant = &proc{}

type proc struct {
	id            int
	isLeader      bool
	leftSendChan  chan string
	leftRecvChan  chan string
	rightSendChan chan string
	rightRecvChan chan string
}

func NewProcSlice(n int) []*proc {
	processes := make([]*proc, n)
	for i := 0; i < n; i++ {
		processes[i] = NewProc()
		processes[i].id = i
	}

	for i := 0; i < n; i++ {
		// The channel must be a *buffered* channel!
		cur := processes[i]
		left := processes[(i+1)%n]
		cur.leftRecvChan = left.rightSendChan
		left.rightRecvChan = cur.leftSendChan
	}

    return processes
}

func NewProc() *proc {
	return &proc{
		id:            -1,
		isLeader:      false,
		leftRecvChan:  make(chan string, 2),
		leftSendChan:  make(chan string, 2),
		rightSendChan: make(chan string, 2),
		rightRecvChan: make(chan string, 2),
	}
}

func (p *proc) String() string {
	return fmt.Sprintf("processes %d: IsLeader(%v)",
		p.id,
		p.IsLeader())
}

func (p *proc) Id() int {
	return p.id
}

func (p *proc) SetLeader(l bool) {
	p.isLeader = l
}

func (p *proc) IsLeader() bool {
	return p.isLeader
}

type Direction int

const (
	Left Direction = iota
	Right
)

func (p *proc) WaitForMsg() (msg string, dir Direction) {
	select {
	case msg = <-p.leftRecvChan:
		return msg, Left
	case msg = <-p.rightRecvChan:
		return msg, Right
	}
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

func (p *proc) Terminate() {
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
