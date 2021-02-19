package group

import (
	"fmt"
	"sort"
)

type Group struct {
	Windows  int
	replicas map[uint64]*Replica
}

func New(peers []uint64) *Group {
	g := Group{replicas: make(map[uint64]*Replica)}
	for _, peer := range peers {
		g.replicas[peer] = newReplica()
	}
	return &g
}

func (g *Group) Members() int {
	return len(g.replicas)
}

// This implies that each step of the protocol must be processed
// by f + 1 replicas. These f + 1 together with the f that may
// not respond give us the smallest group size of 2f + 1
func (g *Group) Smallest() int {
	return group(1)
}

func (g *Group) Faulty() int {
	return g.Members()/2
}

// the quorum of replicas that processes a particular step
// of the protocol must have a non-empty intersection with
// the group of replicas available to handle the next step,
// since this way we can ensure that at each next step at
// least one participant knows what happened in the previous
// step. In a group of 2f + 1 replicas, f + 1 is the smallest
// quorum size that will work.
func (g *Group) Quorum() int {
	return g.Faulty() + 1
}

func (g *Group) Progresses() uint64s {
	members := make(uint64s, 0, g.Members())
	for i := range g.replicas {
		members = append(members, g.replicas[i].progress())
	}
	return members
}

func (g *Group) Commit() uint64 {
	nums := g.Progresses()
	sort.Sort(sort.Reverse(nums))
	return nums[g.Quorum()-1]
}

func (g *Group) Replicas() map[uint64]*Replica {
	return g.replicas
}

func (g *Group) ReplicaNums() []uint64 {
	nums := make([]uint64, 0, g.Members())
	for num := range g.Replicas() {
		nums = append(nums, num)
	}
	sort.Sort(uint64s(nums))
	return nums
}

func (g *Group) Replica(i uint64) *Replica {
	if r, ok := g.replicas[i]; ok {
		return r
	}
	//TODO: panic?
	return nil
}

func (g *Group) Exist(i uint64) bool {
	if _, ok := g.replicas[i]; ok {
		return true
	}
	return false
}

func (g *Group) Reset(opNum, replicaNum uint64) {
	for num := range g.replicas {
		g.replicas[num] = &Replica{
			Progress: &Progress{Next: opNum + 1},
		}
		if num == replicaNum {
			g.replicas[num].Ack = opNum
		}
	}
}

func (g *Group) Set(num, offset, next uint64) {
	g.replicas[num]	= &Replica{Progress: &Progress{Next: next, Ack: offset}}
}

func (g *Group) Del(num uint64) {
	delete(g.replicas, num)
}

func group(f int) int {
	if f < 1 {
		panic("f must be greater than or equal to 1")
	}
	return 2*f + 1
}

const (
	Normal = iota
	WarmedUp
)

type Replica struct {
	*Progress
	// used to represent the progress status of the
	// current Progress
	Status   int
}

func newReplica() *Replica {
	r := Replica{
		Progress: NewProgress(),
		Status: Normal,
	}
	return &r
}

func (r *Replica) progress() uint64 {
	return r.Ack
}

type uint64s []uint64

func (r uint64s) Len() int {
	return len(r)
}

func (r uint64s) Less(i, j int) bool {
	return r[i] < r[j]
}

func (r uint64s) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}



// control and manage the current sync windows and status
// for a replica
type Progress struct {
	// the confirmed sync position of all nodes in the
	// replication group
	Ack   uint64
	// if data sync is currently not possible, how long
	// do I need to wait?
	Delay int
	// record the next location that needs to be sync
	Next  uint64
}

func NewProgress() *Progress {
	return &Progress{
		Next: 1,
	}
}

func (p *Progress) DelaySet(d int) {
	p.Delay = d
}

func (p *Progress) DelayReset() {
	p.Delay = 0
}

func (p *Progress) NeedDelay() bool {
	return p.Ack == 0 && p.Delay > 0
}

func (p *Progress) Update(n uint64) {
	p.DelayReset()
	if p.Ack < n {
		p.Ack = n
	}
	if p.Next < n + 1 {
		p.Next = n + 1
	}
}

func (p *Progress) NiceUpdate(n uint64) {
	p.Next = n + 1
}

func (p *Progress) TryDecTo(ignored, last uint64) bool {
	p.DelayReset()
	if p.Ack != 0 {
		if ignored <= p.Ack {
			return false
		}
		p.Next = p.Ack + 1
		return true
	}
	if p.Next-1 != ignored {
		return false
	}
	if p.Next = min(ignored, last+1); p.Next < 1 {
		p.Next = 1
	}
	return true
}

func (p *Progress) DelayDec(i int) {
	p.Delay -= i
	if p.Delay < 0 {
		p.Delay = 0
	}
}

func (p *Progress) DelayInc(i int) {
}

func (p *Progress) String() string {
	return fmt.Sprintf("next = %d, offsets = %d, delay = %v", p.Next, p.Ack, p.Delay)
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}