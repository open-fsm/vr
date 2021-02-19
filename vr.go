package vr

import (
	"fmt"
logger "log"
	"math"
	"math/rand"
	"encoding/json"
	"time"
	"strings"
	"context"
	"errors"
	"os"
	"os/exec"
	"io"
	"io/ioutil"
	"github.com/open-fsm/log"
	"github.com/open-fsm/vr/group"
	"github.com/open-fsm/spec/proto"
)

var (
	nilHardState = proto.HardState{}
	ErrStopped   = errors.New("vr.bus: replicator stopped")
)

type Option struct {
	Action int
	Filter func()
}

type Bus interface {
	Advance()
	Change(ctx context.Context) error
	Call(context.Context, proto.Message) error
	Clock()
	Membership(context.Context, proto.Configuration)
	Propose(context.Context, []byte)
	Reconfiguration(proto.Configuration) *proto.ConfigurationState
	Status() Status
	Stop()
	Tuple(...Option) <-chan Tuple
}

func Start(c *Config) Bus {
	b := newBus()
	vr := newVR(c)
	vr.becomeBackup(proto.ViewStamp{ViewNum:One}, None)
	vr.log.CommitNum = vr.log.LastOpNum()
	vr.CommitNum = vr.log.CommitNum
	for _, num := range c.Peers {
		vr.createReplica(num)
	}
	go b.cycle(vr)
	return b
}

func Restart(c *Config) Bus {
	b := newBus()
	vr := newVR(c)
	go b.cycle(vr)
	return b
}

type bus struct {
	requestC            chan proto.Message
	receiveC            chan proto.Message
	tupleC              chan Tuple
	advanceC            chan struct{}
	clockC              chan struct{}
	configurationC      chan proto.Configuration
	configurationStateC chan proto.ConfigurationState
	doneC               chan struct{}
	stopC               chan struct{}
	statusC             chan chan Status
}

func newBus() *bus {
	return &bus{
		requestC:            make(chan proto.Message),
		receiveC:            make(chan proto.Message),
		tupleC:              make(chan Tuple),
		advanceC:            make(chan struct{}),
		configurationC:      make(chan proto.Configuration),
		configurationStateC: make(chan proto.ConfigurationState),
		clockC:              make(chan struct{}),
		doneC:               make(chan struct{}),
		stopC:               make(chan struct{}),
		statusC:             make(chan chan Status),
	}
}

func (b *bus) cycle(vr *VR) {
	var requestC chan proto.Message
	var tupleC chan Tuple
	var advanceC chan struct{}
	var prevUnsafeOpNum uint64
	var prevUnsafeViewNum uint64
	var needToSafe bool
	var prevAppliedStateOpNum uint64
	var tp Tuple

	prim := None
	prevSoftState := vr.softState()
	prevHardState := nilHardState

	for {
		if prim != vr.prim {
			if vr.existPrimary() {
				if prim == None {
					logger.Printf("vr.bus: %x change primary %x at view-number %d", vr.replicaNum, vr.prim, vr.ViewStamp.ViewNum)
				} else {
					logger.Printf("vr.bus: %x changed primary from %x to %x at view-number %d", vr.replicaNum, prim, vr.prim, vr.ViewStamp.ViewNum)
				}
				requestC = b.requestC
			} else {
				logger.Printf("vr.bus: %x faulty primary %x at view-number %d", vr.replicaNum, prim, vr.ViewStamp.ViewNum)
				requestC = nil
			}
			prim = vr.prim
		}
		if advanceC != nil {
			tupleC = nil
		} else {
			tp = newTuple(vr, prevSoftState, prevHardState)
			if tp.PreCheck() {
				tupleC = b.tupleC
			} else {
				tupleC = nil
			}
		}
		select {
		case m := <-requestC:
			m.From = vr.replicaNum
			vr.Call(m)
		case m := <-b.receiveC:
			if vr.group.Exist(m.From) || !IsReplyMessage(m) {
				vr.Call(m)
			}
		case <-advanceC:
			if prevHardState.CommitNum != 0 {
				vr.log.AppliedTo(prevHardState.CommitNum)
			}
			if needToSafe {
				vr.log.SafeTo(prevUnsafeOpNum, prevUnsafeViewNum)
				needToSafe = false
			}
			// TODO: need to check?
			vr.log.SafeAppliedStateTo(prevAppliedStateOpNum)
			advanceC = nil
		case tupleC <- tp:
			if n := len(tp.PersistentEntries); n > 0 {
				prevUnsafeOpNum = tp.PersistentEntries[n-1].ViewStamp.OpNum
				prevUnsafeViewNum = tp.PersistentEntries[n-1].ViewStamp.ViewNum
				needToSafe = true
			}
			if tp.SoftState != nil {
				prevSoftState = tp.SoftState
			}
			if !IsInvalidHardState(tp.HardState) {
				prevHardState = tp.HardState
			}
			if !IsInvalidAppliedState(tp.AppliedState) {
				prevAppliedStateOpNum = tp.AppliedState.Applied.ViewStamp.OpNum
			}
			vr.messages = nil
			advanceC = b.advanceC
		case c := <-b.statusC:
			c <- getStatus(vr)
		case c := <-b.configurationC:
			b.handleConfiguration(c, vr)
		case <-b.clockC:
			vr.clock()
		case <-b.stopC:
			close(b.doneC)
			return
		}
	}
}

func (b *bus) Advance() {
	select {
	case b.advanceC <- struct{}{}:
	case <-b.doneC:
	}
}

func (b *bus) Change(ctx context.Context) error {
	return b.call(ctx, proto.Message{Type: proto.Change})
}

func (b *bus) Call(ctx context.Context, m proto.Message) error {
	if IsIgnorableMessage(m) {
		return nil
	}
	// TODO: notify the outside that the system is doing reconfiguration
	return b.call(ctx, m)
}

func (b *bus) call(ctx context.Context, m proto.Message) error {
	ch := b.receiveC
	if m.Type == proto.Request {
		ch = b.requestC
	}
	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.doneC:
		return ErrStopped
	}
}

func (b *bus) Membership(ctx context.Context, cfg proto.Configuration) {
	data, err := cfg.Marshal()
	if err != nil {
		logger.Fatal("vr.bus membership configure data marshal error ", err)
	}
	b.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Type:proto.Configure, Data: data}}})
}

func (b *bus) Propose(ctx context.Context, data []byte) {
	b.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Type:proto.Log, Data: data}}})
}

func (b *bus) Tuple(opt ...Option) <-chan Tuple {
	if opt != nil {
		for _, this := range opt {
			if f := this.Filter; f != nil {
				f()
			}
		}
	}
	return b.tupleC
}

func (b *bus) Reconfiguration(c proto.Configuration) *proto.ConfigurationState {
	var state proto.ConfigurationState
	select {
	case b.configurationC <- c:
	case <-b.doneC:
	}
	select {
	case state = <-b.configurationStateC:
	case <-b.doneC:
	}
	return &state
}

func (b *bus) handleConfiguration(c proto.Configuration, vr *VR) {
	if c.ReplicaNum == None {
		select {
		case b.configurationStateC <- proto.ConfigurationState{Configuration: vr.group.ReplicaNums()}:
		case <-b.doneC:
		}
		return
	}
	switch c.Type {
	case proto.AddReplica:
		vr.createReplica(c.ReplicaNum)
	case proto.DelReplica:
		if c.ReplicaNum == vr.replicaNum {
			b.requestC = nil
		}
		vr.destroyReplica(c.ReplicaNum)
	default:
		panic("unexpected configuration type")
	}
	select {
	case b.configurationStateC <- proto.ConfigurationState{Configuration: vr.group.ReplicaNums()}:
	case <-b.doneC:
	}
}

func (b *bus) Status() Status {
	c := make(chan Status)
	b.statusC <- c
	return <-c
}

func (b *bus) Clock() {
	select {
	case b.clockC <- struct{}{}:
	case <-b.doneC:
	}
}

func (b *bus) Stop() {
	select {
	case b.stopC <- struct{}{}:
	case <-b.doneC:
		return
	}
	<-b.doneC
}

type SoftState struct {
	Prim uint64
	Role role
}

func (r *SoftState) equal(ss *SoftState) bool {
	return r.Prim == ss.Prim && r.Role == ss.Role
}

type Tuple struct {
	proto.AppliedState
	proto.HardState
	*SoftState

	PersistentEntries []proto.Entry
	ApplicableEntries []proto.Entry
	Messages          []proto.Message
}

func newTuple(vr *VR, prevSS *SoftState, prevHS proto.HardState) Tuple {
	t := Tuple{
		PersistentEntries: vr.log.UnsafeEntries(),
		ApplicableEntries: vr.log.SafeEntries(),
		Messages:          vr.messages,
	}
	if ss := vr.softState(); !ss.equal(prevSS) {
		t.SoftState = ss
	}
	if !hardStateCompare(vr.HardState, prevHS) {
		t.HardState = vr.HardState
	}
	if appliedState := vr.log.UnsafeAppliedState(); appliedState != nil {
		t.AppliedState = *appliedState
	}
	return t
}

func (t Tuple) PreCheck() bool {
	return t.SoftState != nil || !IsInvalidHardState(t.HardState) || len(t.PersistentEntries) > 0 ||
		len(t.ApplicableEntries) > 0 || len(t.Messages) > 0
}

const (
	None uint64 = iota
	One
)

const noLimit = math.MaxUint64

// status type represents the current status of a bus in a cluster.
type status uint64

// status code
const (
	Normal     status = iota
	ViewChange
	Recovering
	Transitioning
)

// status name
var statusName = [...]string{"Normal", "ViewChange", "Recovering", "Transitioning"}

// role type represents the current role of a bus in a cluster.
type role uint64

// role code
const (
	Replica role = iota
	Primary
	Backup
)

// role name
var roleName = [...]string{"Replica", "Primary", "Backup"}

// view change phrases
const (
	Change          = iota
	StartViewChange
	DoViewChange
)

// view stamped replication configure
type Config struct {
	Num               uint64         // bus number, from 1 start
	Peers             []uint64       // all the nodes in a replication group, include self
	Store             *log.Store   // state machine storage models
	TransitionTimeout time.Duration  // maximum processing time (ms) for primary
	HeartbeatTimeout  time.Duration  // maximum waiting time (ms) for backups
	AppliedNum        uint64         // where the logger has been applied ?
	Selector          int            // new primary node selection strategy
}

// configure check
func (c *Config) validate() error {
	if c.Num < 0 || c.Num == None {
		return fmt.Errorf("vr: bus number cannot be zero or a number smaller than zero")
	}
	if c.Store == nil {
		return fmt.Errorf("vr: store is not initialized in config")
	}
	cs, err := c.Store.LoadConfigurationState()
	if err != nil {
		return err
	}
	if len(cs.Configuration) > 0 {
		if len(c.Peers) > 0 {
			return fmt.Errorf("vr: found that there are replicas in the configuration file, and ignore the user's input")
		}
		c.Peers = cs.Configuration
	}
	if n := len(c.Peers); c.Peers != nil && n < 1 {
		return fmt.Errorf("vr: there are no available nodes in the replication group")
	}
	if c.AppliedNum < 0 {
		return fmt.Errorf("vr: applied number cannot be smaller than zero")
	}
	if c.TransitionTimeout == 0 {
		c.TransitionTimeout = 300*time.Millisecond
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 100*time.Millisecond
	}
	if !isInvalidSelector(c.Selector) {
		return fmt.Errorf("vr: selector is invalid: %d", c.Selector)
	}
	return nil
}

// protocol control models for VR
type VR struct {
	// The key state of the replication group that has landed
	proto.HardState

	replicaNum uint64             // bus number, from 1 start
	log        *log.Log           // used to manage operation loggers
	group      *group.Group       // control and manage the current synchronization replicas
	status     status             // record the current replication group status
	role       role               // mark the current bus role
	views      [3]map[uint64]bool // count the views of each bus during the view change process
	messages   []proto.Message    // temporarily store messages that need to be sent
	prim       uint64             // who is the primary ?
	pulse      int                // occurrence frequency
	transitionTimeout int         // maximum processing time for primary
	heartbeatTimeout  int         // maximum waiting time for backups
	rand              *rand.Rand  // generate random seed
	call              callFn      // intervention automaton device through external events
	clock      clockFn            // drive clock oscillator
	selected   selectFn           // new primary node selection algorithm
	seq        uint64             // monotonically increasing number
}

func newVR(c *Config) *VR {
	if err := c.validate(); err != nil {
		panic(fmt.Sprintf("vr: config validate error: %v", err))
	}
	hs, err := c.Store.LoadHardState()
	if err != nil {
		panic(fmt.Sprintf("vr: load hard state error: %v", err))
	}
	vr := &VR{
		replicaNum:        c.Num,
		prim:              None,
		log:               log.New(c.Store),
		group:             group.New(c.Peers),
		HardState:         hs,
		transitionTimeout: int(c.TransitionTimeout),
		heartbeatTimeout:  int(c.HeartbeatTimeout),
	}
	vr.rand = rand.New(rand.NewSource(int64(c.Num)))
	vr.initSelector(c.Selector)
	if !hardStateCompare(hs, nilHardState) {
		vr.loadHardState(hs)
	}
	if num := c.AppliedNum; num > 0 {
		vr.log.AppliedTo(num)
	}
	vr.becomeBackup(vr.ViewStamp, None)
	var replicaList []string
	for _, n := range vr.group.ReplicaNums() {
		replicaList = append(replicaList, fmt.Sprintf("%x", n))
	}
	logger.Printf("vr: new vr %x [nodes: [%s], view-number: %d, commit-number: %d, applied-number: %d, last-op-number: %d, last-view-number: %d]",
		vr.replicaNum, strings.Join(replicaList, ","), vr.ViewStamp.ViewNum, vr.log.CommitNum, vr.log.AppliedNum, vr.log.LastOpNum(), vr.log.LastViewNum())
	return vr
}

func (v *VR) becomePrimary() {
	if v.role == Backup {
		panic("vr: invalid transition [backup to primary]")
	}
	v.call = callPrimary
	v.clock = v.clockHeartbeat
	v.reset(v.ViewStamp.ViewNum)
	v.initEntry()
	v.prim = v.replicaNum
	v.role = Primary
	v.status = Normal
	logger.Printf("vr: %x became primary at view-number %d", v.replicaNum, v.ViewStamp.ViewNum)
}

func (v *VR) becomeReplica() {
	if v.role == Primary {
		panic("vr: invalid transition [primary to bus]")
	}
	v.call = callReplica
	v.clock = v.clockTransition
	v.reset(v.ViewStamp.ViewNum + 1)
	v.role = Replica
	v.status = ViewChange
	logger.Printf("vr: %x became bus at view-number %d", v.replicaNum, v.ViewStamp.ViewNum)
}

func (v *VR) becomeBackup(vs proto.ViewStamp, prim uint64) {
	v.call = callBackup
	v.clock = v.clockTransition
	v.reset(vs.ViewNum)
	v.prim = prim
	v.role = Backup
	v.status = Normal
	if v.prim == None {
		logger.Printf("vr: %x became backup at view-number %d, primary not found", v.replicaNum, v.ViewStamp.ViewNum)
	} else {
		logger.Printf("vr: %x became backup at view-number %d, primary is %d", v.replicaNum, v.ViewStamp.ViewNum, v.prim)
	}
}

func (v *VR) initEntry() {
	v.appendEntry(proto.Entry{Data: nil})
}

func (v *VR) existPrimary() bool {
	return v.prim != None
}

func (v *VR) tryCommit() bool {
	return v.log.TryCommit(v.group.Commit(), v.ViewStamp.ViewNum)
}

func (v *VR) resetViews()  {
	for i := 0; i < len(v.views); i++ {
		v.views[i] = map[uint64]bool{}
	}
}

func (v *VR) reset(ViewNum uint64) {
	if v.ViewStamp.ViewNum != ViewNum {
		v.ViewStamp.ViewNum = ViewNum
	}
	v.prim = None
	v.pulse = 0
	v.resetViews()
	v.group.Reset(v.log.LastOpNum(), v.replicaNum)
}

func (v *VR) Call(m proto.Message) error {
	if m.Type == proto.Change {
		logger.Printf("vr: %x is starting a new view changes at view-number %d", v.replicaNum, v.ViewStamp.ViewNum)
		change(v)
		v.CommitNum = v.log.CommitNum
		return nil
	}
	switch {
	case m.ViewStamp.ViewNum == 0:
	case m.ViewStamp.ViewNum > v.ViewStamp.ViewNum:
		prim := m.From
		if (m.Type == proto.StartViewChange) || m.Type == proto.DoViewChange {
			prim = None
		}
		logger.Printf("vr: %x [view-number: %d] received a %s message with higher view-number from %x [view-number: %d]",
			v.replicaNum, v.ViewStamp.ViewNum, m.Type, m.To, m.ViewStamp.ViewNum)
		v.becomeBackup(m.ViewStamp, prim)
	case m.ViewStamp.ViewNum < v.ViewStamp.ViewNum:
		logger.Printf("vr: %x [view-number: %d] ignored a %s message with lower view-number from %x [view-number: %d]",
			v.replicaNum, v.ViewStamp.ViewNum, m.Type, m.To, m.ViewStamp.ViewNum)
		return nil
	}
	v.call(v, m)
	v.CommitNum = v.log.CommitNum
	return nil
}

func (v *VR) take(num uint64, phrase int) bool {
	return v.views[phrase][num]
}

func (v *VR) stats(phrase int) (count int) {
	for _, v := range v.views[phrase] {
		if v {
			count++
		}
	}
	return count
}

func (v *VR) collect(num uint64, view bool, phrase int) int {
	if view {
		logger.Printf("vr: %x received view-change from %x at view-number %d and phrase %d",
			v.replicaNum, num, v.ViewStamp.ViewNum, phrase)
	} else {
		logger.Printf("vr: %x received ignore view from %x at view-number %d and phrase %d",
			v.replicaNum, num, v.ViewStamp.ViewNum, phrase)
	}
	if _, ok := v.views[phrase][num]; !ok {
		v.views[phrase][num] = view
	}
	return v.stats(phrase)
}

func (v *VR) send(m proto.Message) {
	m.From = v.replicaNum
	if m.Type != proto.Request {
		m.ViewStamp.ViewNum = v.ViewStamp.ViewNum
	}
	v.messages = append(v.messages, m)
}

func (v *VR) sendAppend(to uint64, typ ...proto.MessageType) {
	replica := v.group.Replica(to)
	if replica.NeedDelay() {
		return
	}
	m := proto.Message{
		Type: proto.Prepare,
		To: to,
	}
	defer func() {
		v.send(m)
	}()
	if v.tryAppliedState(replica.Next) {
		m.Type = proto.PrepareAppliedState
		state, err := v.log.AppliedState()
		if err != nil {
			panic(err)
		}
		if IsInvalidAppliedState(state) {
			panic("must be a valid state")
		}
		m.AppliedState = state
		opNum, viewNum := state.Applied.ViewStamp.OpNum, state.Applied.ViewStamp.ViewNum
		logger.Printf("vr: %x [start op-number: %d, commit-number: %d] sent applied-number state[op-number: %d, view-number: %d] to %x [%s]",
			v.replicaNum, v.log.StartOpNum(), v.CommitNum, opNum, viewNum, to, replica)
		replica.DelaySet(v.transitionTimeout)
		return
	}
	if typ != nil {
		m.Type = typ[0]
	}
	m.ViewStamp.OpNum = replica.Next - 1
	m.LogNum = v.log.ViewNum(replica.Next-1)
	m.Entries = v.log.Entries(replica.Next)
	m.CommitNum = v.log.CommitNum
	if n := len(m.Entries); replica.Ack != 0 && n != 0 {
		replica.NiceUpdate(m.Entries[n-1].ViewStamp.OpNum)
	} else if replica.Ack == 0 {
		replica.DelaySet(v.heartbeatTimeout)
	}
}

func (v *VR) sendHeartbeat(to uint64) {
	commit := min(v.group.Replica(to).Ack, v.log.CommitNum)
	v.send(proto.Message{
		To:        to,
		Type:      proto.Commit,
		CommitNum: commit,
	})
}

func (v *VR) sendto(m proto.Message, to uint64) {
	m.To = to
	v.send(m)
}

func (v *VR) tryAppliedState(num uint64) bool {
	return num < v.log.StartOpNum()
}

type clockFn func()

func (v *VR) clockTransition() {
	if !v.raising() {
		v.pulse = 0
		return
	}
	v.pulse++
	if v.isTransitionTimeout() {
		v.pulse = 0
		v.Call(proto.Message{From: v.replicaNum, Type: proto.Change})
	}
}

func (v *VR) clockHeartbeat() {
	v.pulse++
	if v.pulse >= v.heartbeatTimeout {
		v.pulse = 0
		v.Call(proto.Message{From: v.replicaNum, Type: proto.Heartbeat})
	}
}

func (v *VR) raising() bool {
	return v.group.Exist(v.replicaNum)
}

func (v *VR) broadcastStartEpoch() {
	v.broadcastAppend(proto.StartEpoch)
}

func (v *VR) broadcastAppend(mt ...proto.MessageType) {
	for num := range v.group.Replicas() {
		if num == v.replicaNum {
			continue
		}
		v.sendAppend(num, mt...)
	}
}

func (v *VR) broadcastHeartbeat() {
	for num := range v.group.Replicas() {
		if num == v.replicaNum {
			continue
		}
		v.sendHeartbeat(num)
		v.group.Replica(num).DelayDec(v.heartbeatTimeout)
	}
}

func change(v *VR) {
	v.becomeReplica()
	if v.group.Quorum() == v.collect(v.replicaNum, true, Change) {
		v.becomePrimary()
		return
	}
	for num := range v.group.Replicas() {
		if num == v.replicaNum {
			continue
		}
		logger.Printf("vr: %x [oplogger view-number: %d, op-number: %d] sent START-VIEW-CHANGE request to %x at view-number %d",
			v.replicaNum, v.log.LastViewNum(), v.log.LastOpNum(), num, v.ViewStamp.ViewNum)
		v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartViewChange, ViewStamp: proto.ViewStamp{ViewNum:v.log.LastViewNum(),OpNum:v.log.LastOpNum()}})
	}
}

func startViewChange(v *VR, m *proto.Message) {
	num := v.replicaNum
	view := true
	if m != nil {
		num = m.From
	}
	views := v.collect(num, view, StartViewChange)
	if views == 1 && !v.take(v.replicaNum, Change) {
		for num := range v.group.Replicas() {
			if num == v.replicaNum {
				continue
			}
			logger.Printf("vr: %x [oplogger view-number: %d, op-number: %d] sent START-VIEW-CHANGE request to %x at view-number %d",
				v.replicaNum, v.log.LastViewNum(), v.log.LastOpNum(), num, v.ViewStamp.ViewNum)
			v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartViewChange, ViewStamp: proto.ViewStamp{ViewNum:v.log.LastViewNum(),OpNum:v.log.LastOpNum()}})
		}
	}
	if v.group.Faulty() == views {
		logger.Printf("vr: %x has received %d views, start send DO-VIEW-CHANGE message", v.replicaNum, views)
		doViewChange(v, m)
	}
}

func doViewChange(v *VR, m *proto.Message) {
	// If it is not the primary node, send a DO-VIEW-CHANGE message to the new
	// primary node that has been pre-selected.
	if num := v.selected(v.ViewStamp, v.group.Members()); num != v.replicaNum {
		logger.Printf("vr: %x [oplogger view-number: %d, op-number: %d] sent DO-VIEW-CHANGE request to %x at view-number %d, replicas: %d",
				v.replicaNum, v.log.LastViewNum(), v.log.LastOpNum(), num, v.ViewStamp.ViewNum, v.group.Members())
		v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.DoViewChange, ViewStamp: proto.ViewStamp{ViewNum:v.log.LastViewNum(),OpNum:v.log.LastOpNum()}})
		return
	} else {
		// If it is the primary node, first send yourself a DO-VIEW-CHANGE message.
		v.collect(v.replicaNum, true, DoViewChange)
	}
	num := v.replicaNum
	view := true
	if m != nil {
		num = m.From
	}
	if v.group.Quorum() == v.collect(num, view, DoViewChange) {
		for num := range v.group.Replicas() {
			if num == v.replicaNum {
				continue
			}
			logger.Printf("vr: %x [oplogger view-number: %d, op-number: %d] sent START-VIEW request to %x at view-number %d",
				v.replicaNum, v.log.LastViewNum(), v.log.LastOpNum(), num, v.ViewStamp.ViewNum)
			v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartView, ViewStamp: proto.ViewStamp{ViewNum:v.log.LastViewNum(),OpNum:v.log.LastOpNum()}})
		}
		v.becomePrimary()
		v.broadcastAppend()
		return
	}
}

type callFn func(*VR, proto.Message)

func callPrimary(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Heartbeat:
		v.broadcastHeartbeat()
	case proto.Request:
		if len(m.Entries) == 0 {
			logger.Panicf("vr: %x called empty request", v.replicaNum)
		}
		v.appendEntry(m.Entries...)
		switch m.Entries[0].Type {
		case proto.Configure:
			v.broadcastStartEpoch()
		default:
			v.broadcastAppend()
		}
	case proto.PrepareOk, proto.EpochStarted:
		delay := v.group.Replica(m.From).NeedDelay()
		v.group.Replica(m.From).Update(m.ViewStamp.OpNum)
		if v.tryCommit() {
			v.broadcastAppend()
		} else if delay {
			v.sendAppend(m.From)
		}
	case proto.CommitOk:
		if v.group.Replica(m.From).Ack < v.log.LastOpNum() {
			v.sendAppend(m.From)
		}
	case proto.StartViewChange, proto.DoViewChange:
		// ignore message
		logger.Printf("vr: %x [logger-view-number: %d, op-number: %d] ignore %s from %x [op-number: %d] at view-number %d",
			v.replicaNum, v.log.LastViewNum(), v.log.LastOpNum(), m.Type, m.From, m.ViewStamp.OpNum, m.ViewStamp.ViewNum)
	case proto.StartView:
		v.becomeBackup(v.ViewStamp, m.From)
		v.status = Normal
	case proto.Recovery:
		// TODO: verify the source
		logger.Printf("vr: %x received recovery (last-op-number: %d) from %x for op-number %d to recovery response",
			v.replicaNum, m.X, m.From, m.ViewStamp.OpNum)
		if v.group.Replica(m.From).TryDecTo(m.ViewStamp.OpNum, m.Note) {
			logger.Printf("vr: %x decreased replicas of %x to [%s]", v.replicaNum, m.From, v.group.Replica(m.From))
			v.sendAppend(m.From, proto.RecoveryResponse)
		}
	case proto.GetState:
		logger.Printf("vr: %x received get state (last-op-number: %d) from %x for op-number %d to new state",
			v.replicaNum, m.X, m.From, m.ViewStamp.OpNum)
		if v.group.Replica(m.From).TryDecTo(m.ViewStamp.OpNum, m.Note) {
			logger.Printf("v: %x decreased replicas of %x to [%s], send new state", v.replicaNum, m.From, v.group.Replica(m.From))
			v.sendAppend(m.From, proto.NewState)
		}
	}
}

func callReplica(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Request:
		logger.Printf("vr: %x no primary (bus) at view-number %d; dropping request", v.replicaNum, v.ViewStamp.ViewNum)
		return
	case proto.Prepare:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleAppend(m)
		v.status = Normal
	case proto.PrepareAppliedState:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleAppliedState(m)
		v.status = Normal
	case proto.StartEpoch:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleStartEpoch(m)
		v.status = Normal
	case proto.Commit:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleHeartbeat(m)
		v.status = Normal
	case proto.StartViewChange:
		startViewChange(v, &m)
	case proto.DoViewChange:
		doViewChange(v, &m)
	case proto.StartView:
		v.becomeBackup(v.ViewStamp, m.From)
		v.status = Normal
	}
}

func callBackup(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Request:
		if v.prim == None {
			logger.Printf("vr: %x no primary (backup) at view-number %d; dropping request", v.replicaNum, v.ViewStamp.ViewNum)
			return
		}
		v.sendto(m, v.prim)
	case proto.Prepare:
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	case proto.PrepareAppliedState:
		v.pulse = 0
		v.handleAppliedState(m)
	case proto.StartEpoch:
		v.pulse = 0
		v.handleStartEpoch(m)
	case proto.Commit:
		v.pulse = 0
		v.prim = m.From
		v.handleHeartbeat(m)
	case proto.StartViewChange:
		v.status = ViewChange
		startViewChange(v, &m)
	case proto.DoViewChange:
		v.status = ViewChange
		doViewChange(v, &m)
	case proto.StartView:
		v.becomeBackup(v.ViewStamp, m.From)
		v.status = Normal
	case proto.RecoveryResponse:
		// TODO: do load balancing to reduce the pressure on the primary
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	case proto.NewState:
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	}
}

func (v *VR) appendEntry(entries ...proto.Entry) {
	lo := v.log.LastOpNum()
	for i := range entries {
		entries[i].ViewStamp.ViewNum = v.ViewStamp.ViewNum
		entries[i].ViewStamp.OpNum = lo + uint64(i) + 1
	}
	v.log.Append(entries...)
	v.group.Replica(v.replicaNum).Update(v.log.LastOpNum())
	// TODO need check return value?
	v.tryCommit()
}

func (v *VR) handleAppend(m proto.Message) {
	msgLastOpNum, ok := v.log.TryAppend(m.ViewStamp.OpNum, m.LogNum, m.CommitNum, m.Entries...)
	if ok {
		t := proto.PrepareOk
		if v.status == Transitioning {
			t = proto.EpochStarted
			v.status = Normal
		}
		v.send(proto.Message{To: m.From, Type: t, ViewStamp: proto.ViewStamp{OpNum: msgLastOpNum}})
		return
	}
	switch v.status {
	case Normal:
		logger.Printf("vr: %x normal state [logger-number: %d, op-number: %d] find [logger-number: %d, op-number: %d] from %x",
			v.replicaNum, v.log.ViewNum(m.ViewStamp.OpNum), m.ViewStamp.OpNum, m.LogNum, m.ViewStamp.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.GetState, ViewStamp: proto.ViewStamp{OpNum: m.ViewStamp.OpNum}, Note: v.log.LastOpNum()})
	case Recovering, Transitioning:
		logger.Printf("vr: %x recovering state [logger-number: %d, op-number: %d] find [logger-number: %d, op-number: %d] from %x",
			v.replicaNum, v.log.ViewNum(m.ViewStamp.OpNum), m.ViewStamp.OpNum, m.LogNum, m.ViewStamp.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.Recovery, ViewStamp: proto.ViewStamp{OpNum: m.ViewStamp.OpNum}, X: v.seq, Note: v.log.LastOpNum()})
	default:
		logger.Printf("vr: %x [logger-number: %d, op-number: %d] ignored prepare [logger-number: %d, op-number: %d] from %x",
			v.replicaNum, v.log.ViewNum(m.ViewStamp.OpNum), m.ViewStamp.OpNum, m.LogNum, m.ViewStamp.OpNum, m.From)
	}
}

func (v *VR) handleStartEpoch(m proto.Message) {
	msgLastOpNum, ok := v.log.TryAppend(m.ViewStamp.OpNum, m.LogNum, m.CommitNum, m.Entries...)
	if ok {
		v.send(proto.Message{To: m.From, Type: proto.EpochStarted, ViewStamp: proto.ViewStamp{OpNum: msgLastOpNum}})
		return
	}
	v.status = Transitioning
}

func (v *VR) handleAppliedState(m proto.Message) {
	opNum, viewNum := m.AppliedState.Applied.ViewStamp.OpNum, m.AppliedState.Applied.ViewStamp.ViewNum
	if v.recover(m.AppliedState) {
		logger.Printf("vr: %x [commit-number: %d] recovered applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, opNum, viewNum)
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, ViewStamp: proto.ViewStamp{OpNum: v.log.LastOpNum()}})
		return
	}
	logger.Printf("vr: %x [commit-number: %d] ignored applied state [op-number: %d, view-number: %d]",
		v.replicaNum, v.CommitNum, opNum, viewNum)
	v.send(proto.Message{To: m.From, Type: proto.PrepareOk, ViewStamp: proto.ViewStamp{OpNum: v.log.CommitNum}})
}

func (v *VR) handleHeartbeat(m proto.Message) {
	v.log.CommitTo(m.CommitNum)
	v.send(proto.Message{To: m.From, Type: proto.CommitOk})
}

func (v *VR) softState() *SoftState {
	return &SoftState{Prim: v.prim, Role: v.role}
}

func (v *VR) loadHardState(hs proto.HardState) {
	if hs.CommitNum < v.log.CommitNum || hs.CommitNum > v.log.LastOpNum() {
		logger.Panicf("vr: %x commit-number %d is out of range [%d, %d]",
			v.replicaNum, hs.CommitNum, v.log.CommitNum, v.log.LastOpNum())
	}
	v.log.CommitNum = hs.CommitNum
	v.ViewStamp.ViewNum = hs.ViewStamp.ViewNum
	v.CommitNum = hs.CommitNum
}

func (v *VR) initSelector(num int) {
	loadSelector(&v.selected, num)
}

func (v *VR) createReplica(num uint64) {
	if v.group.Exist(num) {
		return
	}
	v.group.Set(num,0, v.log.LastOpNum()+1)
}

func (v *VR) destroyReplica(num uint64) {
	v.group.Del(num)
}

func (v *VR) isTransitionTimeout() bool {
	delta := v.pulse - v.transitionTimeout
	if delta < 0 {
		return false
	}
	return delta > v.rand.Int()%v.transitionTimeout
}

func (v *VR) recover(as proto.AppliedState) bool {
	if as.Applied.ViewStamp.OpNum <= v.log.CommitNum {
		return false
	}
	if v.log.CheckNum(as.Applied.ViewStamp.OpNum, as.Applied.ViewStamp.ViewNum) {
		logger.Printf("vr: %x [commit-number: %d, last-op-number: %d, last-view-number: %d] skip commit to applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, v.log.LastOpNum(), v.log.LastViewNum(), as.Applied.ViewStamp.OpNum, as.Applied.ViewStamp.ViewNum)
		v.log.CommitTo(as.Applied.ViewStamp.OpNum)
		return false
	}
	logger.Printf("vr: %x [commit-number: %d, last-op-number: %d, last-view-number: %d] starts to recover applied state [op-number: %d, view-number: %d]",
		v.replicaNum, v.CommitNum, v.log.LastOpNum(), v.log.LastViewNum(), as.Applied.ViewStamp.OpNum, as.Applied.ViewStamp.ViewNum)
	v.log.Recover(as)
	return true
}

// provide the current status of the replication group
type Status struct {
	// temporary status syncing
	SoftState
	// persistent state
	proto.HardState

	Num        uint64            // current bus number
	CommitNum  uint64            // loggers that have been committed
	AppliedNum uint64            // track the status that has been applied
	//Windows    map[uint64]window // peer's window control information, subject to primary
}

// build and package status
func getStatus(vr *VR) Status {
	s := Status{Num: vr.replicaNum}
	s.HardState = vr.HardState
	s.SoftState = *vr.softState()
	s.AppliedNum = vr.log.AppliedNum
	if s.Role == Primary {
		/*
		s.Windows = make(map[uint64]window)
		for num, window := range vr.replicas {
			s.Windows[num] = *window
		}
		*/
	}
	return s
}

// serialized status data
func (s Status) String() string {
	b, err := json.Marshal(&s)
	if err != nil {
		logger.Panicf("unexpected error: %v", err)
	}
	return string(b)
}

const (
	RoundRobin = iota
)

// This paper provides round-robin (section 4, figure 2) as the primary
// selection algorithm. From the engineering point of view, we default to
// adopt the same scheme as the article, but the selection algorithm is
// open to discussion, so in the aspect of engineering implementation,
// it gives engineers more space to play and imagine.
type selectFn func(vs proto.ViewStamp, windows int, f ... func()) uint64

var selectors = []selectFn{
	roundRobin,
}

// The primary is chosen round-robin, starting with replica 1, as the
// system moves to new views.
func roundRobin(vs proto.ViewStamp, windows int, _... func()) uint64 {
	if n := vs.ViewNum % uint64(windows); n != 0 {
		return n
	}
	return 1
}

func isInvalidSelector(num int) bool {
	if 0 <= num && num < len(selectors) {
		return true
	}
	return false
}

func loadSelector(sf *selectFn, num int) {
	*sf = selectors[num]
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func IsIgnorableMessage(m proto.Message) bool {
	return m.Type == proto.Change || m.Type == proto.Heartbeat
}

func IsReplyMessage(m proto.Message) bool {
	return m.Type == proto.Reply || m.Type == proto.PrepareOk
}

func IsHardStateEqual(a, b proto.HardState) bool {
	return hardStateCompare(a, b)
}

func IsInvalidHardState(hs proto.HardState) bool {
	return IsHardStateEqual(hs, nilHardState)
}

func IsInvalidAppliedState(as proto.AppliedState) bool {
	return as.Applied.ViewStamp.OpNum == 0
}

func hardStateCompare(a, b proto.HardState) bool {
	return a.ViewStamp.ViewNum == b.ViewStamp.ViewNum && a.CommitNum == b.CommitNum
}

func limitSize(entries []proto.Entry, maxSize uint64) []proto.Entry {
	if len(entries) == 0 {
		return entries
	}
	size := len(entries[0].String())
	var limit int
	for limit = 1; limit < len(entries); limit++ {
		size += len(entries[limit].String())
		if uint64(size) > maxSize {
			break
		}
	}
	return entries[:limit]
}

type Node interface {
	Call(m proto.Message) error
	handleMessages() []proto.Message
}

func (v *VR) handleMessages() []proto.Message {
	msgs := v.messages
	v.messages = make([]proto.Message, 0)
	return msgs
}

type mock struct {
	numbers []uint64
	nodes   map[uint64]Node
	stores  map[uint64]*log.Store
	routers map[route]float64
	ignores map[proto.MessageType]bool
}

type route struct {
	from, to uint64
}

func newMock(nodes ...Node) *mock {
	n := len(nodes)
	m := &mock{
		numbers: numberBySize(n),
		nodes:   make(map[uint64]Node, n),
		stores:  make(map[uint64]*log.Store, n),
		routers: make(map[route]float64),
		ignores: make(map[proto.MessageType]bool),
	}
	for i, node := range nodes {
		if err := m.build(i, node, n); err != nil {
			panic(err)
		}
	}
	return m
}

func (m *mock) build(index int, node Node, n int) error {
	if n <= 0 {
		return fmt.Errorf("node too small: %d", n)
	}
	num := m.numbers[index]
	switch v := node.(type) {
	case *real:
		m.stores[num] = log.NewStore()
		vr := newVR(&Config{
			Num:               num,
			Peers:             m.numbers,
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             m.stores[num],
			AppliedNum:        0,
		})
		m.nodes[num] = vr
	case *faker:
		m.nodes[num] = v
	case *VR:
		v.replicaNum = num
		var peers []uint64
		for i := 0; i < n; i++ {
			peers = append(peers, m.numbers[i])
		}
		v.group = group.New(peers)
		v.reset(0)
		m.nodes[num] = v
	default:
		return fmt.Errorf("unexpecteded node type: %T", node)
	}
	return nil
}

func (m *mock) trigger(msgs ...proto.Message) {
	for len(msgs) > 0 {
		msg := msgs[0]
		peer := m.nodes[msg.To]
		peer.Call(msg)
		adds := m.handler(peer.handleMessages())
		msgs = append(msgs[1:], adds...)
	}
}

func (m *mock) peers(num uint64) *VR {
	return m.nodes[num].(*VR)
}

func (m *mock) delete(from, to uint64, percent float64) {
	m.routers[route{from, to}] = percent
}

func (m *mock) cover(one, other uint64) {
	m.delete(one, other, 1)
	m.delete(other, one, 1)
}

func (m *mock) shield(num uint64) {
	for i := 0; i < len(m.nodes); i++ {
		other := uint64(i) + 1
		if other != num {
			m.delete(num, other, 1.0)
			m.delete(other, num, 1.0)
		}
	}
}

func (m *mock) ignore(mt proto.MessageType) {
	m.ignores[mt] = true
}

func (m *mock) reset() {
	m.numbers = []uint64{}
	m.routers = make(map[route]float64)
	m.ignores = make(map[proto.MessageType]bool)
}

func (m *mock) handler(msgs []proto.Message) []proto.Message {
	ms := []proto.Message{}
	for _, msg := range msgs {
		if err := m.filter(msg, &ms); err != nil {
			panic(err)
		}
	}
	return ms
}

func (m *mock) filter(msg proto.Message, msgs *[]proto.Message) error {
	if m.ignores[msg.Type] {
		return nil
	}
	if msg.Type == proto.Change {
		return fmt.Errorf("unexpected change")
	} else {
		router := m.routers[route{msg.From, msg.To}]
		if n := rand.Float64(); n < router {
			return nil
		}
	}
	*msgs = append(*msgs, msg)
	return nil
}

type real struct {}

func (real) Call(proto.Message) error     {
	return nil
}

func (real) handleMessages() []proto.Message {
	return nil
}

var node = &real{}

type faker struct{}

func (faker) Call(proto.Message) error     {
	return nil
}

func (faker) handleMessages() []proto.Message {
	return nil
}

var hole = &faker{}

func numberBySize(size int) []uint64 {
	nums := make([]uint64, size)
	for i := 0; i < size; i++ {
		nums[i] = 1 + uint64(i)
	}
	return nums
}

func entries(viewNums ...uint64) *VR {
	s := log.NewStore()
	for i, viewNum := range viewNums {
		s.Append([]proto.Entry{{ViewStamp: proto.ViewStamp{OpNum: uint64(i + 1), ViewNum: viewNum}}})
	}
	vr := newVR(&Config{
		Num:               1,
		Peers:             nil,
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             log.NewStore(),
		AppliedNum:        0,
	})
	vr.reset(0)
	return vr
}

func safeEntries(vr *VR, s *log.Store) (entries []proto.Entry) {
	s.Append(vr.log.UnsafeEntries())
	vr.log.SafeTo(vr.log.LastOpNum(), vr.log.LastViewNum())
	entries = vr.log.SafeEntries()
	vr.log.AppliedTo(vr.log.CommitNum)
	return entries
}

func diff(a, b string) string {
	if a == b {
		return ""
	}
	alpha, beta := mustTempFile("alpha*", a), mustTempFile("beta*", b)
	defer func() {
		os.Remove(alpha)
		os.Remove(beta)
	}()
	cmd := exec.Command("diff", "-u", alpha, beta)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTempFile(pattern, data string) string {
	f, err := ioutil.TempFile("", pattern)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(data))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func stringOpLog(l *log.Log) string {
	s := fmt.Sprintf("commit-number: %d\n", l.CommitNum)
	s += fmt.Sprintf("applied-number:  %d\n", l.AppliedNum)
	for i, e := range l.TotalEntries() {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}