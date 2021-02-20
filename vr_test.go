package vr

import (
	"fmt"
	"math"
	"time"
	"bytes"
	"reflect"
	"context"
	"testing"
	"github.com/open-fsm/log"
	"github.com/open-fsm/spec/proto"
)

const (
	replicaA = iota + 1
	replicaB
	replicaC
	replicaD
	replicaE
)

var routes = [][]uint64{
	{replicaA, replicaC}, {replicaA, replicaD}, {replicaA, replicaE},
}

func initViewStampCase() {

}

func changeMessage(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Change}
}

func requestMessage(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}}
}

func heartbeatMessage(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Heartbeat}
}

func requestMessageEmptyEntries(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Request, Entries: []proto.Entry{{}}}
}

func TestPrimaryChange(t *testing.T) {
	cases := []struct {
		*mock
		role role
	}{
		{newMock(node, node, node), Primary},
		{newMock(node, hole, hole), Replica},
		{newMock(node, hole, hole, node), Replica},
		{newMock(node, node, hole), Primary},
		{newMock(node, hole, hole, node, node), Primary},
		{newMock(entries(1), node, entries(2), entries(1), node), Primary},
	}
	for i, test := range cases {
		test.trigger(changeMessage(replicaA, replicaA))
		peer := test.mock.peers(replicaA)
		if peer.role != test.role {
			t.Errorf("#%d: role = %s, expected %s", i, roleName[peer.role], roleName[test.role])
		}
		if vn := peer.ViewStamp.ViewNum; vn != 1 {
			t.Errorf("#%d: view-number = %d, expected %d", i, vn, 1)
		}
	}
}

func TestSingleReplica(t *testing.T) {
	m := newMock(node)
	m.trigger(changeMessage(replicaA, replicaA))
	peer := m.peers(1)
	if peer.role != Primary {
		t.Errorf("role = %d, expected %d", peer.role, Primary)
	}
}

func TestSingleReplicaCommit(t *testing.T) {
	m := newMock(node)
	m.trigger(changeMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	vr := m.peers(1)
	if vr.log.CommitNum != 3 {
		t.Errorf("commit-number = %d, expected %d", vr.log.CommitNum, 3)
	}
}

func TestLogReplication(t *testing.T) {
	cases := []struct {
		*mock
		messages     []proto.Message
		expCommitNum uint64
	}{
		{
			newMock(node, node, node),
			[]proto.Message{ requestMessage(replicaA, replicaA) },
			2,
		},
		{
			newMock(node, node, node),
			[]proto.Message{
				requestMessage(replicaA, replicaA), changeMessage(replicaA, replicaB),
				requestMessage(replicaA, replicaB),
			},
			4,
		},
	}
	for i, test := range cases {
		test.trigger(changeMessage(replicaA, replicaA))
		for _, m := range test.messages {
			test.trigger(m)
		}
		for j, node := range test.mock.nodes {
			peer := node.(*VR)
			if peer.log.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d: commit-number = %d, expected %d", i, j, peer.log.CommitNum, test.expCommitNum)
			}
			entries := []proto.Entry{}
			for _, e := range safeEntries(peer, test.mock.stores[j]) {
				if e.Data != nil {
					entries = append(entries, e)
				}
			}
			messages := []*proto.Message{}
			for _, m := range test.messages {
				if m.Type == proto.Request {
					messages = append(messages, &m)
				}
			}
			for k, m := range messages {
				if !bytes.Equal(entries[k].Data, m.Entries[0].Data) {
					t.Errorf("#%d.%d: data = %d, expected %d", i, j, entries[k].Data, m.Entries[0].Data)
				}
			}
		}
	}
}

func TestRequest(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		*mock
		success bool
	}{
		{newMock(node, node, node), true},
		{newMock(node, node, hole), true},
		{newMock(node, hole, hole), false},
		{newMock(node, hole, hole, node), false},
		{newMock(node, hole, hole, node, node), true},
	}
	for j, test := range cases {
		send := func(m proto.Message) {
			defer func() {
				if !test.success {
					rc := recover()
					if rc != nil {
						t.Logf("#%d: err: %s", j, rc)
					}
				}
			}()
			test.trigger(m)
		}
		data := []byte("testdata")
		send(changeMessage(replicaA, replicaA))
		send(requestMessage(replicaA, replicaA))
		expectedLog := log.New(log.NewStore())
		if test.success {
			expectedLog = &log.Log{
				Store: &log.Store{
					Entries: []proto.Entry{{}, {Data: nil, ViewStamp:v1o1}, {ViewStamp:v1o2, Data: data}},
				},
				Unsafe:    log.Unsafe{Offset: 3},
				CommitNum: 2}
		}
		base := stringOpLog(expectedLog)
		for i, node := range test.nodes {
			if peer, ok := node.(*VR); ok {
				l := stringOpLog(peer.log)
				if g := diff(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty oplog", i)
			}
		}
		peer := test.mock.peers(1)
		if vn := peer.ViewStamp.ViewNum; vn != 1 {
			t.Errorf("#%d: view-number = %d, expected %d", j, vn, 1)
		}
	}
}

func TestCommit(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		offsets []uint64
		entries []proto.Entry
		viewNum uint64
		exp     uint64
	}{
		{[]uint64{1},[]proto.Entry{{ViewStamp:v1o1}},1,1},
		{[]uint64{1},[]proto.Entry{{ViewStamp:v1o1}},2,0},
		{[]uint64{2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},2,2},
		{[]uint64{1},[]proto.Entry{{ViewStamp:v2o1}},2,1},

		{[]uint64{2, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},1,1},
		{[]uint64{2, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
		{[]uint64{2, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},2,2},
		{[]uint64{2, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},

		{[]uint64{2, 1, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},1,1},
		{[]uint64{2, 1, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
		{[]uint64{2, 1, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},1,1},
		{[]uint64{2, 1, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
		{[]uint64{2, 1, 2, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},2,2},
		{[]uint64{2, 1, 2, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
	}
	for i, test := range cases {
		store := log.NewStore()
		store.Append(test.entries)
		store.HardState = proto.HardState{ViewStamp:proto.ViewStamp{ViewNum: test.viewNum}}
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1},
			TransitionTimeout: 5,
			HeartbeatTimeout:  1,
			Store:             store,
			AppliedNum:        0,
		})
		for j := 0; j < len(test.offsets); j++ {
			vr.group.Set(uint64(j)+1, test.offsets[j], test.offsets[j]+1)
		}
		vr.tryCommit()
		if cn := vr.log.CommitNum; cn != test.exp {
			t.Errorf("#%d: commit-number = %d, expected %d", i, cn, test.exp)
		}
	}
}

func TestIsTransitionTimeout(t *testing.T) {
	cases := []struct {
		pulse       int
		probability float64
		round       bool
	}{
		{5, 0, false},
		{13, 0.3, true},
		{15, 0.5, true},
		{18, 0.8, true},
		{20, 1, false},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		vr.pulse = test.pulse
		c := 0
		for j := 0; j < 10000; j++ {
			if vr.isTransitionTimeout() {
				c++
			}
		}
		v := float64(c) / 10000.0
		if test.round {
			v = math.Floor(v*10+0.5) / 10.0
		}
		if v != test.probability {
			t.Errorf("#%d: possibility = %v, expected %v", i, v, test.probability)
		}
	}
}

func TestCallIgnoreLateViewNumMessage(t *testing.T) {
	called := false
	fakeCall := func(r *VR, m proto.Message) {
		called = true
	}
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             log.NewStore(),
		AppliedNum:        0,
	})
	vr.call = fakeCall
	vr.ViewStamp.ViewNum = 2
	vr.Call(proto.Message{Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: vr.ViewStamp.ViewNum - 1}})
	if called == true {
		t.Errorf("call function called = %v , expected %v", called, false)
	}
}

func TestHandleMessagePrepare(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		m            proto.Message
		expOpNum     uint64
		expCommitNum uint64
		expIgnore    bool
	}{
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 3, CommitNum: 3},2,0,true},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o3, LogNum: 3, CommitNum: 3},2,0,true},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o1, LogNum: 1, CommitNum: 1},2,1,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o0, LogNum: 0, CommitNum: 1, Entries: []proto.Entry{{ViewStamp:v2o1}}},1, 1,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 3, Entries: []proto.Entry{{ViewStamp:v2o3}, {ViewStamp:v2o4}}},4, 3, false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 4, Entries: []proto.Entry{{ViewStamp:v2o3}}},3, 3,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o1, LogNum: 1, CommitNum: 4, Entries: []proto.Entry{{ViewStamp:v2o2}}},2, 2,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v1o1, LogNum: 1, CommitNum: 3},2,1, false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v1o1, LogNum: 1, CommitNum: 3, Entries: []proto.Entry{{ViewStamp:v2o2}}},2,2,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 3},2,2, false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 4},2,2, false},
	}
	for i, test := range cases {
		store := log.NewStore()
		store.Append([]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}})
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             store,
			AppliedNum:        0,
		})
		vr.becomeBackup(proto.ViewStamp{ViewNum:2}, None)
		vr.handleAppend(test.m)
		if vr.log.LastOpNum() != test.expOpNum {
			t.Errorf("#%d: last op-number = %d, expected %d", i, vr.log.LastOpNum(), test.expOpNum)
		}
		if vr.log.CommitNum != test.expCommitNum {
			t.Errorf("#%d: commit-number = %d, expected %d", i, vr.log.CommitNum, test.expCommitNum)
		}
		m := vr.handleMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: message = nil, expected 1", i)
		}
		//if m[0].Ignore != test.expIgnore {
		//	t.Errorf("#%d: ignore = %v, expected %v", i, m[0].Ignore, test.expIgnore)
		//}
	}
}

func TestHandleHeartbeat(t *testing.T) {
	initViewStampCase()
	CommitNum := uint64(2)
	cases := []struct {
		m            proto.Message
		expCommitNum uint64
	}{
		{proto.Message{From: replicaB, To: replicaA, Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: 2}, CommitNum: CommitNum + 1}, CommitNum + 1},
		{proto.Message{From: replicaB, To: replicaA, Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: 2}, CommitNum: CommitNum - 1}, CommitNum},
	}
	for i, test := range cases {
		store := log.NewStore()
		store.Append([]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}, {ViewStamp:v3o3}})
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{1},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             store,
			AppliedNum:        0,
		})
		vr.becomeBackup(proto.ViewStamp{ViewNum:2}, replicaB)
		vr.log.CommitTo(CommitNum)
		vr.handleHeartbeat(test.m)
		if vr.log.CommitNum != test.expCommitNum {
			t.Errorf("#%d: commit-number = %d, expected %d", i, vr.log.CommitNum, test.expCommitNum)
		}
		m := vr.handleMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: message = nil, expected 1", i)
		}
		if m[0].Type != proto.CommitOk {
			t.Errorf("#%d: type = %v, expected heartbeat commit-number ok", i, m[0].Type)
		}
	}
}

func TestHandleCommitOk(t *testing.T) {
	initViewStampCase()
	store := log.NewStore()
	store.Append([]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}, {ViewStamp:v3o3}})
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1, 2},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	vr.becomeReplica()
	vr.becomePrimary()
	vr.log.CommitTo(vr.log.LastOpNum())
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs := vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, expected 1", len(msgs))
	}
	if msgs[0].Type != proto.Prepare {
		t.Errorf("type = %v, expected prepare", msgs[0].Type)
	}
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs = vr.handleMessages()
	if len(msgs) != 0 {
		t.Fatalf("len(messages) = %d, expected 0", len(msgs))
	}
	vr.broadcastHeartbeat()
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs = vr.handleMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %d, expected 2", len(msgs))
	}
	if msgs[0].Type != proto.Commit {
		t.Errorf("type = %v, expected heartbeat CommitNum", msgs[0].Type)
	}
	if msgs[1].Type != proto.Prepare {
		t.Errorf("type = %v, expected message prepare", msgs[1].Type)
	}
	vr.Call(proto.Message{
		From:  2,
		Type:  proto.PrepareOk,
		ViewStamp:proto.ViewStamp{OpNum: msgs[1].ViewStamp.OpNum + uint64(len(msgs[1].Entries))},
	})
	vr.handleMessages()
	vr.broadcastHeartbeat()
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs = vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, expected 1: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.Commit {
		t.Errorf("type = %v, expected heartbeat CommitNum", msgs[0].Type)
	}
}

func TestMessagePrepareOkDelayReset(t *testing.T) {
	vr := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB, replicaC},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             log.NewStore(),
		AppliedNum:        0,
	})
	vr.becomeReplica()
	vr.becomePrimary()
	vr.broadcastAppend()
	vr.handleMessages()
	vr.Call(proto.Message{
		From:  2,
		Type:  proto.PrepareOk,
		ViewStamp:proto.ViewStamp{OpNum:1},
	})
	if vr.CommitNum != 1 {
		t.Fatalf("expecteded commit-number to be 1, got %d", vr.CommitNum)
	}
	vr.handleMessages()
	vr.Call(proto.Message{
		From:    1,
		Type:    proto.Request,
		Entries: []proto.Entry{{}},
	})
	msgs := vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("expecteded 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.Prepare || msgs[0].To != 2 {
		t.Errorf("expecteded prepare to bus 2, got %s to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].ViewStamp.OpNum != 2 {
		t.Errorf("expecteded to trigger entry 2, but got %v", msgs[0].Entries)
	}
	vr.Call(proto.Message{
		From:  3,
		Type:  proto.PrepareOk,
		ViewStamp:proto.ViewStamp{OpNum:1},
	})
	msgs = vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("expecteded 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.Prepare || msgs[0].To != 3 {
		t.Errorf("expecteded message prepare to bus 3, got %s to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].ViewStamp.OpNum != 2 {
		t.Errorf("expecteded to trigger entry 2, but got %v", msgs[0].Entries)
	}
}

func TestStateTransition(t *testing.T) {
	cases := []struct {
		from       role
		to         role
		expAllow   bool
		expViewNum uint64
		expPrim    uint64
	}{
		{Backup,Backup,true,1,None},
		{Backup,Replica,true,1,None},
		{Backup,Primary,false,0,None},

		{Replica,Backup,true,0,None},
		{Replica,Replica,true,1,None},
		{Replica,Primary,true,0,1},

		{Primary,Backup,true,1,None},
		{Primary,Replica,false,1,None},
		{Primary,Primary,true,0,1},
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expAllow == true {
						t.Errorf("%d: allow = %v, expected %v", i, false, true)
					}
				}
			}()
			vr := newVR(&Config{
				Num:               1,
				Peers:             []uint64{1},
				TransitionTimeout: 10,
				HeartbeatTimeout:  1,
				Store:             log.NewStore(),
				AppliedNum:        0,
			})
			vr.role = test.from
			switch test.to {
			case Backup:
				vr.becomeBackup(proto.ViewStamp{ViewNum:test.expViewNum}, test.expPrim)
			case Replica:
				vr.becomeReplica()
			case Primary:
				vr.becomePrimary()
			}
			if vr.ViewStamp.ViewNum != test.expViewNum {
				t.Errorf("%d: view-number = %d, expected %d", i, vr.ViewStamp.ViewNum, test.expViewNum)
			}
			if vr.prim != test.expPrim {
				t.Errorf("%d: prim = %d, expected %d", i, vr.prim, test.expPrim)
			}
		}()
	}
}

func TestAllServerCallDown(t *testing.T) {
	cases := []struct {
		role       role
		expRole    role
		expViewNum uint64
		expOpNum   uint64
	}{
		{Backup,Backup,3,0},
		{Replica,Backup,3,0},
		{Primary,Backup,3,1},
	}
	msgTypes := [...]proto.MessageType{proto.Prepare}
	viewNum := uint64(3)
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1, 2, 3},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		switch test.role {
		case Backup:
			vr.becomeBackup(proto.ViewStamp{ViewNum:1}, None)
		case Replica:
			vr.becomeReplica()
		case Primary:
			vr.becomeReplica()
			vr.becomePrimary()
		}
		for j, msgType := range msgTypes {
			vr.Call(proto.Message{From: 2, Type: msgType, ViewStamp:proto.ViewStamp{ViewNum: viewNum}})

			if vr.role != test.expRole {
				t.Errorf("#%d.%d role = %v, expected %v", i, j, vr.role, test.expRole)
			}
			if vr.ViewStamp.ViewNum != test.expViewNum {
				t.Errorf("#%d.%d view-number = %v, expected %v", i, j, vr.ViewStamp.ViewNum, test.expViewNum)
			}
			if uint64(vr.log.LastOpNum()) != test.expOpNum {
				t.Errorf("#%d.%d op-number = %v, expected %v", i, j, vr.log.LastOpNum(), test.expOpNum)
			}
			if uint64(len(vr.log.TotalEntries())) != test.expOpNum {
				t.Errorf("#%d.%d len(entries) = %v, expected %v", i, j, len(vr.log.TotalEntries()), test.expOpNum)
			}
			expPrim := uint64(2)
			if vr.prim != expPrim {
				t.Errorf("#%d, vr.prim = %d, expected %d", i, vr.prim, None)
			}
		}
	}
}

func TestPrimaryPrepareOk(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		opNum        uint64
		expOffset    uint64
		expNext      uint64
		expMsgNum    int
		expOpNum     uint64
		expCommitNum uint64
	}{
		{2,2,4,2,2,2},
		{0,0,3,0,0,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		vr.log = &log.Log{
			Store:  &log.Store{Entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}},
			Unsafe: log.Unsafe{Offset: 3},
		}
		vr.becomeReplica()
		vr.becomePrimary()
		vr.handleMessages()
		vr.Call(proto.Message{From: replicaB, Type: proto.PrepareOk, ViewStamp:proto.ViewStamp{OpNum: test.opNum, ViewNum: vr.ViewStamp.ViewNum}, Note: test.opNum})
		window := vr.group.Replica(replicaB)
		if window.Ack != test.expOffset {
			t.Errorf("#%d offsets = %d, expected %d", i, window.Ack, test.expOffset)
		}
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsgNum {
			t.Errorf("#%d message number = %d, expected %d", i, len(msgs), test.expMsgNum)
		}
		for j, msg := range msgs {
			if msg.ViewStamp.OpNum != test.expOpNum {
				t.Errorf("#%d.%d op-number = %d, expected %d", i, j, msg.ViewStamp.OpNum, test.expOpNum)
			}
			if msg.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d commit-number = %d, expected %d", i, j, msg.CommitNum, test.expCommitNum)
			}
		}
	}
}

func TestPrimaryRecovery(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		opNum        uint64
		Type         proto.MessageType
		expOffset    uint64
		expNext      uint64
		expMsgNum    int
		expOpNum     uint64
		expCommitNum uint64
	}{
		{3,proto.Recovery,0,3,0,0,0},
		{2,proto.Recovery,0,2,1,1,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		vr.log = &log.Log{
			Store:  &log.Store{Entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}},
			Unsafe: log.Unsafe{Offset: 3},
		}
		vr.becomeReplica()
		vr.becomePrimary()
		vr.handleMessages()
		vr.Call(proto.Message{From: replicaB, Type: test.Type, ViewStamp:proto.ViewStamp{OpNum: test.opNum, ViewNum: vr.ViewStamp.ViewNum}, Note: test.opNum})
		window := vr.group.Replica(replicaB)
		if window.Ack != test.expOffset {
			t.Errorf("#%d offsets = %d, expected %d", i, window.Ack, test.expOffset)
		}
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsgNum {
			t.Errorf("#%d message number = %d, expected %d", i, len(msgs), test.expMsgNum)
		}
		for j, msg := range msgs {
			if msg.ViewStamp.OpNum != test.expOpNum {
				t.Errorf("#%d.%d op-number = %d, expected %d", i, j, msg.ViewStamp.OpNum, test.expOpNum)
			}
			if msg.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d commit-number = %d, expected %d", i, j, msg.CommitNum, test.expCommitNum)
			}
		}
	}
}

func TestPrimaryGetState(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		opNum        uint64
		Type         proto.MessageType
		expOffset    uint64
		expNext      uint64
		expMsgNum    int
		expOpNum     uint64
		expCommitNum uint64
	}{
		{3,proto.GetState,0,3,0,0,0},
		{2,proto.GetState,0,2,1,1,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		vr.log = &log.Log{
			Store:  &log.Store{Entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}},
			Unsafe: log.Unsafe{Offset: 3},
		}
		vr.becomeReplica()
		vr.becomePrimary()
		vr.handleMessages()
		vr.Call(proto.Message{From: replicaB, Type: test.Type, ViewStamp:proto.ViewStamp{OpNum: test.opNum, ViewNum: vr.ViewStamp.ViewNum}, Note: test.opNum})
		window := vr.group.Replica(replicaB)
		if window.Ack != test.expOffset {
			t.Errorf("#%d offsets = %d, expected %d", i, window.Ack, test.expOffset)
		}
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsgNum {
			t.Errorf("#%d message number = %d, expected %d", i, len(msgs), test.expMsgNum)
		}
		for j, msg := range msgs {
			if msg.ViewStamp.OpNum != test.expOpNum {
				t.Errorf("#%d.%d op-number = %d, expected %d", i, j, msg.ViewStamp.OpNum, test.expOpNum)
			}
			if msg.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d commit-number = %d, expected %d", i, j, msg.CommitNum, test.expCommitNum)
			}
		}
	}
}

func TestBroadcastHeartbeat(t *testing.T) {
	offset := uint64(1000)
	as := proto.AppliedState{
		Applied: proto.Applied{
			ViewStamp:proto.ViewStamp{ViewNum: 1, OpNum: offset},
			// configure nodes node
		},
	}
	store := log.NewStore()
	store.SetAppliedState(as)
	vr := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB, replicaC},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	vr.ViewStamp.ViewNum = 1
	vr.becomeReplica()
	vr.becomePrimary()
	for i := 0; i < 10; i++ {
		vr.appendEntry(proto.Entry{ViewStamp:proto.ViewStamp{OpNum: uint64(i) + 1}})
	}
	vr.group.Set(replicaB, 5, 6)
	vr.group.Set(replicaC, vr.log.LastOpNum(), vr.log.LastOpNum()+1)
	vr.Call(proto.Message{Type: proto.Heartbeat})
	msgs := vr.handleMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %v, expected 2", len(msgs))
	}
	expectedCommitMap := map[uint64]uint64{
		2: min(vr.log.CommitNum, vr.group.Replica(2).Ack),
		3: min(vr.log.CommitNum, vr.group.Replica(3).Ack),
	}
	for i, m := range msgs {
		if m.Type != proto.Commit {
			t.Fatalf("#%d: type = %v, expected = %v", i, m.Type, proto.Commit)
		}
		if m.ViewStamp.OpNum != 0 {
			t.Fatalf("#%d: prev op-number = %d, expected %d", i, m.ViewStamp.OpNum, 0)
		}
		if expectedCommitMap[m.To] == 0 {
			t.Fatalf("#%d: unexpecteded to %d", i, m.To)
		} else {
			if m.CommitNum != expectedCommitMap[m.To] {
				t.Fatalf("#%d: commit-number = %d, expected %d", i, m.CommitNum, expectedCommitMap[m.To])
			}
			delete(expectedCommitMap, m.To)
		}
		if len(m.Entries) != 0 {
			t.Fatalf("#%d: len(entries) = %d, expected 0", i, len(m.Entries))
		}
	}
}

func TestReceiveMessageHeartbeat(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		role   role
		expMsg int
	}{
		{Primary,2},
		{Replica,0},
		{Backup,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		vr.log = &log.Log{Store: &log.Store{Entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}}}
		vr.ViewStamp.ViewNum = 1
		vr.role = test.role
		switch test.role {
		case Backup:
			vr.call = callBackup
		case Replica:
			vr.call = callReplica
		case Primary:
			vr.call = callPrimary
		}
		vr.Call(proto.Message{From: 1, To: 1, Type: proto.Heartbeat})
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsg {
			t.Errorf("%d: len(messages) = %d, expected %d", i, len(msgs), test.expMsg)
		}
		for _, m := range msgs {
			if m.Type != proto.Commit {
				t.Errorf("%d: msg.type = %v, expected %v", i, m.Type, proto.Commit)
			}
		}
	}
}

func TestPrimaryIncreaseNext(t *testing.T) {
	initViewStampCase()
	prevEntries := []proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}, {ViewStamp:v1o3}}
	cases := []struct {
		offset  uint64
		next    uint64
		expNext uint64
	}{
		{1,2,uint64(len(prevEntries) + 1 + 1 + 1)},
		{0,2,2},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		vr.log.Append(prevEntries...)
		vr.becomeReplica()
		vr.becomePrimary()
		vr.group.Set(replicaB, test.offset, test.next)
		vr.Call(requestMessage(replicaA, replicaA))
		window := vr.group.Replica(replicaB)
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
	}
}

func TestRaising(t *testing.T) {
	num := uint64(1)
	cases := []struct {
		peers  []uint64
		expRai bool
	}{
		{[]uint64{replicaA},true},
		{[]uint64{replicaA, replicaB, replicaC},true},
		{nil,false},
		{[]uint64{replicaB, replicaC},false},
	}
	for i, test := range cases {
		r := newVR(&Config{
			num,
			test.peers,
			log.NewStore(),
			5,
			1,
			0,
			RoundRobin,
		})
		if rv := r.raising(); rv != test.expRai {
			t.Errorf("#%d: raising = %v, expected %v", i, rv, test.expRai)
		}
	}
}

func TestVRReplicas(t *testing.T) {
	cases := []struct {
		peers    []uint64
		expPeers []uint64
	}{
		{
			[]uint64{replicaA, replicaB, replicaC},
			[]uint64{replicaA, replicaB, replicaC},
		},
		{
			[]uint64{replicaC, replicaB, replicaA},
			[]uint64{replicaA, replicaB, replicaC},
		},
	}
	for i, test := range cases {
		r := newVR(&Config{
			Num:               replicaA,
			Peers:             test.peers,
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             log.NewStore(),
			AppliedNum:        0,
		})
		if !reflect.DeepEqual(r.group.ReplicaNums(), test.expPeers) {
			t.Errorf("#%d: replicas = %+v, expected %+v", i, r.group.Members(), test.expPeers)
		}
	}
}

func TestWindowDec(t *testing.T) {
	r := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             log.NewStore(),
		AppliedNum:        0,
	})
	r.becomeReplica()
	r.becomePrimary()
	r.group.Replica(replicaB).DelaySet(r.heartbeatTimeout*2)
	r.Call(proto.Message{From: 1, To: 1, Type: proto.Heartbeat})
	if r.group.Replica(replicaB).Delay != r.heartbeatTimeout*(2-1) {
		t.Errorf("delay = %d, expected %d", r.group.Replica(2).Delay, r.heartbeatTimeout*(2-1))
	}
}

func TestWindowDelay(t *testing.T) {
	r := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             log.NewStore(),
		AppliedNum:        0,
	})
	r.becomeReplica()
	r.becomePrimary()
	r.Call(requestMessage(replicaA, replicaA))
	r.Call(requestMessage(replicaA, replicaA))
	r.Call(requestMessage(replicaA, replicaA))
	rv := r.handleMessages()
	if len(rv) != 1 {
		t.Errorf("len(rv) = %d, expected 1", len(rv))
	}
}

func TestCannotCommitWithoutNewViewNumEntry(t *testing.T) {
	m := newMock(node, node, node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	for _, route := range routes {
		m.cover(route[0], route[1])
	}
	m.trigger(requestMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	peer := m.peers(replicaA)
	if peer.log.CommitNum != 1 {
		t.Errorf("commit-number = %d, expected %d", peer.log.CommitNum, 1)
	}
	m.reset()
	m.ignore(proto.Prepare)
	m.trigger(changeMessage(replicaB, replicaB))
	peer = m.peers(replicaB)
	if peer.log.CommitNum != 1 {
		t.Errorf("commit-number = %d, expected %d", peer.log.CommitNum, 1)
	}
	m.reset()
	m.trigger(heartbeatMessage(replicaB, replicaB))
	m.trigger(requestMessage(replicaA, replicaA))
	if peer.log.CommitNum != 5 {
		t.Errorf("commit-number = %d, expected %d", peer.log.CommitNum, 5)
	}
}

func TestCommitWithoutNewViewNumEntry(t *testing.T) {
	m := newMock(node, node, node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	for _, route := range routes {
		m.cover(route[0], route[1])
	}
	m.trigger(requestMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	peer := m.peers(replicaA)
	if peer.log.CommitNum != 1 {
		t.Errorf("commit-number = %d, expected %d", peer.log.CommitNum, 1)
	}
	m.reset()
	m.trigger(changeMessage(replicaB, replicaB))
	if peer.log.CommitNum != 4 {
		t.Errorf("commit-number = %d, expected %d", peer.log.CommitNum, 4)
	}
}

func TestLateMessages(t *testing.T) {
	initViewStampCase()
	m := newMock(node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	m.trigger(changeMessage(replicaB, replicaB))
	m.trigger(changeMessage(replicaA, replicaA))
	m.trigger(proto.Message{From: replicaB, To: replicaA, Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: 2}, Entries: []proto.Entry{{ViewStamp:v2o3}}})
	m.trigger(proto.Message{From: replicaA, To: replicaA, Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}})
	log := &log.Log{
		Store: &log.Store{
			Entries: []proto.Entry{
				{}, {Data: nil, ViewStamp:v1o1},
				{Data: nil, ViewStamp:v2o2}, {Data: nil, ViewStamp:v3o3},
				{Data: []byte("testdata"), ViewStamp:v3o4},
			},
		},
		Unsafe:    log.Unsafe{Offset: 5},
		CommitNum: 4,
	}
	base := stringOpLog(log)
	for i, p := range m.nodes {
		if sm, ok := p.(*VR); ok {
			l := stringOpLog(sm.log)
			if g := diff(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestLazyReplicaRestore(t *testing.T) {
	m := newMock(node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	m.shield(replicaC)
	for j := 0; j <= 100; j++ {
		m.trigger(requestMessageEmptyEntries(replicaA, replicaA))
	}
	prim := m.peers(replicaA)
	safeEntries(prim, m.stores[1])
	//m.stores[1].CreateAppliedState(prim.log.appliedNum, nil, nil) // need to configure
	//m.stores[1].Archive(prim.log.appliedNum)
	m.reset()
	m.trigger(requestMessageEmptyEntries(replicaA, replicaA))
	backup := m.peers(replicaC) // temp fix 2, skip test
	m.trigger(requestMessageEmptyEntries(replicaA, replicaA))
	if backup.log.CommitNum != prim.log.CommitNum {
		t.Errorf("backup.commit-number = %d, expected %d", backup.log.CommitNum, prim.log.CommitNum)
	}
}

func TestBusCall(t *testing.T) {
	for i, mtn := range proto.MessageType_name {
		b := &bus{
			requestC: make(chan proto.Message, 1),
			receiveC: make(chan proto.Message, 1),
		}
		mt := proto.MessageType(i)
		b.Call(context.TODO(), proto.Message{Type: mt})
		if mt == proto.Request {
			select {
			case <-b.requestC:
			default:
				t.Errorf("%d: cannot receive %s on request chan", mt, mtn)
			}
		} else {
			if mt == proto.Heartbeat || mt == proto.Change {
				select {
				case <-b.receiveC:
					t.Errorf("%d: step should ignore %s", mt, mtn)
				default:
				}
			} else {
				select {
				case <-b.receiveC:
				default:
					t.Errorf("%d: cannot receive %s on receive chan", mt, mtn)
				}
			}
		}
	}
}

func TestBusCallNonblocking(t *testing.T) {
	b := &bus{
		requestC: make(chan proto.Message),
		doneC:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopFunc := func() {
		close(b.doneC)
	}
	cases := []struct {
		unblock func()
		err     error
	}{
		{stopFunc, ErrStopped},
		{cancel, context.Canceled},
	}
	for i, test := range cases {
		errC := make(chan error, 1)
		go func() {
			err := b.Call(ctx, proto.Message{Type: proto.Request})
			errC <- err
		}()
		test.unblock()
		select {
		case err := <-errC:
			if err != test.err {
				t.Errorf("#%d: err = %v, expected %v", i, err, test.err)
			}
			if ctx.Err() != nil {
				ctx = context.TODO()
			}
			select {
			case <-b.doneC:
				b.doneC = make(chan struct{})
			default:
			}
		case <-time.After(time.Millisecond * 100):
			t.Errorf("#%d: failed to unblock call", i)
		}
	}
}

func TestBusRequest(t *testing.T) {
	msgs := []proto.Message{}
	appendCall := func(r *VR, m proto.Message) {
		msgs = append(msgs, m)
	}
	b := newBus()
	store := log.NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	go b.cycle(vr)
	b.Change(context.TODO())
	for {
		f := <-b.Tuple()
		store.Append(f.PersistentEntries)
		if f.SoftState.Prim == vr.replicaNum {
			vr.call = appendCall
			b.Advance()
			break
		}
		b.Advance()
	}
	b.Call(context.TODO(), proto.Message{
		Type:    proto.Request,
		Entries: []proto.Entry{{Data: []byte("testdata")}}},
	)
	b.Stop()
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, expected %d", len(msgs), 1)
	}
	if msgs[0].Type != proto.Request {
		t.Errorf("msg type = %d, expected %d", msgs[0].Type, proto.Request)
	}
	if !reflect.DeepEqual(msgs[0].Entries[0].Data, []byte("testdata")) {
		t.Errorf("data = %v, expected %v", msgs[0].Entries[0].Data, []byte("testdata"))
	}
}

func TestBusClock(t *testing.T) {
	b := newBus()
	bs := log.NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	})
	go b.cycle(vr)
	pulse := vr.pulse
	b.Clock()
	b.Stop()
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
}

func TestBusStop(t *testing.T) {
	b := newBus()
	bs := log.NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	})
	doneC := make(chan struct{})

	go func() {
		b.cycle(vr)
		close(doneC)
	}()

	pulse := vr.pulse
	b.Clock()
	b.Stop()

	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for node to stop!")
	}
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
	b.Clock()
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
	b.Stop()
}

func TestReadyPreCheck(t *testing.T) {
	cases := []struct {
		f     Tuple
		check bool
	}{
		{Tuple{}, false},
		{Tuple{SoftState: &SoftState{Prim: 1}}, true},
		{Tuple{PersistentEntries: make([]proto.Entry, 1, 1)}, true},
		{Tuple{ApplicableEntries: make([]proto.Entry, 1, 1)}, true},
		{Tuple{Messages: make([]proto.Message, 1, 1)}, true},
	}
	for i, test := range cases {
		if rv := test.f.PreCheck(); rv != test.check {
			t.Errorf("#%d: precheck = %v, expected %v", i, rv, test.check)
		}
	}
}

func TestBusRestart(t *testing.T) {
	entries := []proto.Entry{
		{ViewStamp: proto.ViewStamp{ViewNum: 1, OpNum: 1}},
		{ViewStamp: proto.ViewStamp{ViewNum: 1, OpNum: 2}, Data: []byte("foo")},
	}
	hs := proto.HardState{ViewStamp: proto.ViewStamp{ViewNum: 1}, CommitNum: 1}
	f := Tuple{
		HardState:         hs,
		ApplicableEntries: entries[:hs.CommitNum],
	}
	s := log.NewStore()
	s.SetHardState(hs)
	s.Append(entries)
	n := Restart(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	if g := <-n.Tuple(); !reflect.DeepEqual(g, f) {
		t.Errorf("g = %+v,\n f %+v", g, f)
	}
	n.Advance()
	select {
	case f := <-n.Tuple():
		t.Errorf("unexpecteded Tuple: %+v", f)
	case <-time.After(time.Millisecond):
	}
}

func TestBusAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := log.NewStore()
	r := Start(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	r.Change(ctx)
	<-r.Tuple()
	r.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Data: []byte("foo")}}})
	var f Tuple
	select {
	case f = <-r.Tuple():
		t.Fatalf("unexpecteded ready before advance: %+v", f)
	case <-time.After(time.Millisecond):
	}
	s.Append(f.PersistentEntries)
	r.Advance()
	select {
	case <-r.Tuple():
	case <-time.After(time.Millisecond):
		t.Errorf("expected ready after advance, but there is no ready available")
	}
}

func TestSoftStateEqual(t *testing.T) {
	cases := []struct {
		ss       *SoftState
		expEqual bool
	}{
		{&SoftState{}, true},
		{&SoftState{Prim: 1}, false},
		{&SoftState{Role: Primary}, false},
	}
	for i, test := range cases {
		if rv := test.ss.equal(&SoftState{}); rv != test.expEqual {
			t.Errorf("#%d, equal = %v, expected %v", i, rv, test.expEqual)
		}
	}
}

func TestIsHardStateEqual(t *testing.T) {
	cases := []struct {
		hs       proto.HardState
		expEqual bool
	}{
		{nilHardState, true},
		{proto.HardState{CommitNum: 1}, false},
		{proto.HardState{ViewStamp: proto.ViewStamp{ViewNum: 1}}, false},
	}
	for i, test := range cases {
		if IsHardStateEqual(test.hs, nilHardState) != test.expEqual {
			t.Errorf("#%d, equal = %v, expected %v", i, IsHardStateEqual(test.hs, nilHardState), test.expEqual)
		}
	}
}

func testRoundRobin() error {
	cases := []struct {
		vs      proto.ViewStamp
		peers   []uint64
		expNum  uint64
	}{
		{proto.ViewStamp{ViewNum:replicaD},[]uint64{replicaA}, replicaA},
		{proto.ViewStamp{ViewNum:replicaC},[]uint64{replicaA, replicaB, replicaC}, replicaA},
		{proto.ViewStamp{ViewNum:replicaB},[]uint64{replicaA, replicaD}, replicaA},
		{proto.ViewStamp{ViewNum:replicaC},[]uint64{replicaA, replicaC}, replicaA},
	}
	for i, test := range cases {
		windows := len(test.peers)
		if rv := roundRobin(test.vs, windows); rv != test.expNum {
			return fmt.Errorf("#%d: round_robin = %v, expected %v", i, rv, test.expNum)
		}
	}
	return nil
}

func TestRoundRobinSelector(t *testing.T) {
	if err := testRoundRobin(); err != nil {
		t.Error(err)
	}
}

func TestJudgeInvalidSelector(t *testing.T) {
	const (
		SelectorUnknown = iota - 1
		SelectorA
		SelectorB
	)
	cases := []struct {
		elector    int
		expResult bool
	}{
		{SelectorUnknown,false},
		{SelectorA,true},
		{SelectorB,false},
	}
	for i, test := range cases {
		if rv := isInvalidSelector(test.elector); rv != test.expResult {
			t.Errorf("#%d: is_invalid_selector = %v, expected %v", i, rv, test.expResult)
		}
	}
}

// Proof: section 3
// State machine replication requires that replicas start in
// the same initial state, and that operations be deterministic.
func TestSameInitialState(t *testing.T) {

}

// Proof: section 3
// VR uses a primary bus to order client requests; the
// other replicas are backups that simply accept the order
// selected by the primary.
func TestPrimaryDeterminesLogOrder(t *testing.T) {

}

// Proof: section 3
// The backups monitor the primary, and if it appears to be
// faulty, they carry out a view change protocol to select
// a new primary.
func TestSelectNewPrimary(t *testing.T) {

}

// Proof: section 4, figure 2
// The replicas are numbered based on their IP addresses:
// the bus with the smallest IP address is bus 1.
// The primary is chosen round-robin, starting with bus 1,
// as the system moves to new views. The status indicates what
// sub-protocol the bus is engaged in.
func TestRoundRobinChosen(t *testing.T) {
	if err := testRoundRobin(); err != nil {
		t.Error(err)
	}
}

// Proof: section 4.1
// Members only process normal protocol messages containing
// a view-number that matches the view-number they know.
// If the sender is behind, the receiver routers the message.
func TestSenderBehindDropsMessage(t *testing.T) {
	roles := []role{Replica, Primary, Backup}
	for _, role := range roles {
		testSenderBehindDropsMessage(t, role)
	}
}

// Proof: section 4.1
// If the sender is ahead, the bus performs a state
// transfer: it requests information it is missing from the
// other replicas and uses this information to bring itself
// up to date before processing the message.
func TestSenderAheadPerformsStateTransfer(t *testing.T) {
	roles := []role{Replica, Primary, Backup}
	for _, role := range roles {
		testSenderAheadPerformsStateTransfer(t, role)
	}
}

func testSenderBehindDropsMessage(t *testing.T, s role) {
	r := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1, 2, 3},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             log.NewStore(),
		AppliedNum:        0,
	})
	switch s {
	case Replica:
		r.becomeReplica()
	case Backup:
		vs := proto.ViewStamp{ViewNum:1}
		r.becomeBackup(vs, 2)
	case Primary:
		r.becomeReplica()
		r.becomePrimary()
	}
	r.Call(proto.Message{Type: proto.Prepare, ViewStamp: proto.ViewStamp{ViewNum: 2}})
	if r.ViewStamp.ViewNum != 2 {
		t.Errorf("view-number = %d, expected %d", r.ViewStamp.ViewNum, 2)
	}
	if r.role != Backup {
		t.Errorf("s = %v, expected %v", r.role, Backup)
	}
}

func testSenderAheadPerformsStateTransfer(t *testing.T, s role) {

}

// Proof: section 4.1
// The primary advances op-number, adds the request to the end
// of the log, Then it sends a <PREPARE v, m, n, k> message to
// the other replicas, where v is the current view-number, m is
// the message it received from the client, n is the op-number
// it assigned to the request, and k is the CommitNum-number.
// The primary waits for f PREPARE_OK messages from different
// backups; at this point it considers the operation (and all
// earlier ones) to be committed. Then, after it has executed
// all earlier operations (those assigned smaller op-numbers),
// the primary executes the operation by making an up-call to
// the service code, and increments its CommitNum-number.
func TestPrimarySyncPrepareToBackups(t *testing.T) {
	testPrimarySendPrepare()
	testPrimaryWaitsPrepareOk()
}

func testPrimarySendPrepare() {

}

func testPrimaryWaitsPrepareOk() {

}

// Backups process PREPARE messages in order: a backup won’t accept
// a prepare with op-number n until it has entries for all earlier
// requests in its log. When a backup i receives a PREPARE message,
// it waits until it has entries in its log for all earlier requests
// (doing state transfer if necessary to get the missing information).
// Then it increments its op-number, adds the request to the end of
// its log, updates the client’s information in the client-table, and
// sends a <PREPARE_OK v, n, i> message to the primary to indicate
// that this operation and all earlier ones have prepared locally
func TestBackupsProcessPrepareFromPrimary(t *testing.T) {

}

// Normally the primary informs backups about the CommitNum when it
// sends the next PREPARE message; this is the purpose of the
// CommitNum-number in the PREPARE message. However, if the primary
// does not receive a new client request in a timely way, it instead
// informs the backups of the latest CommitNum by sending them a <COMMIT
// v, k> message, where k is CommitNum-number (note that in this case
// CommitNum-number = op-number).
func TestPrimaryHeartbeatToBackups(t *testing.T) {

}

// When a backup learns of a CommitNum, it waits until it has the request
// in its log (which may require state transfer) and until it has
// executed all earlier operations. Then it executes the operation by
// performing the up-call to the service code, increments its CommitNum-number,
// updates the client’s entry in the client-table, but does not send the
// reply to the client.
func TestBackupCommitLogApplyToStore(t *testing.T) {

}

// Proof: section 4.2
// If a timeout expires without a communication from the primary, the
// replicas carry out a view change to switch to a new primary.
func TestBackupsTriggerTransitionToPrimary(t *testing.T) {
	// phase 1
	// phase 2
	// phase 3
	// phase 4
	// phase 5
}

// Proof: section 4.2
// up-calls occur only for committed operations. This means that the old
// primary must have received at least f PREPARE_OK messages from other
// replicas, and this in turn implies that the operation is recorded in
// the logs of at least f + 1 replicas (the old primary and the f backups
// that sent the PREPARE_OK messages).
func TestLogCommittedByReplicas(t *testing.T) {

}

// Proof: section 4.2
// Suppose an operation is still in the preparing stage and has not been
// written to the majority log. At this time, a view change occurs. If
// each bus in the new view has not received the op, the op will be
// lost. There is no correctness problem.
func TestLostPreparingLogInViewChange(t *testing.T) {

}

// Proof: section 4.2
// Is it possible that two different operations have the same op-number?
// Possibly, these two operations must belong to two different views, so they
// have different view numbers. The larger view number is required. The VR
// protocol can ensure that this situation will not occur under the same view
// number.
func TestOpNumberDuplicateProblem(t *testing.T) {

}

// Proof: section 4.3
// When a bus recovers after a crash it cannot participate in request
// processing and view changes until it has a state at least as recent as
// when it failed. If it could participate sooner than this, the system can
// fail.
func TestFailedNodeRejoinsCluster(t *testing.T) {
	// phase 1
	// phase 2
	// phase 3
}

// Proof: section 4.3
// A failed node cannot participate in view change before recovery is complete.
func TestRecoveryReplicaIgnoreViewChange(t *testing.T) {

}

// Proof: section 5.2
// State transfer is used to track data from backward replicas in non-crash
// scenarios. There are two cases.

// In the current view, you are behind. In this case, you only need to fill
// in the log after your op-number.
func TestSyncBackwardLogs(t *testing.T) {
	// phase 1
	// phase 2
	// phase 3
}

// A view change has occurred, and you are no longer in the new view. In this
// case, you need to truncate your log to CommitNum-number (because the following
// op may be rewritten in the new view), and then find other replicas to pull
// the log.
func TestTruncateAndSyncLatestLog(t *testing.T) {

}

func applyToStore([]proto.Entry)      {}
func sendMessages([]proto.Message)     {}
func saveStateToDisk(proto.HardState)  {}
func saveEntriesToDisk([]proto.Entry) {}

func ExampleReplicator() {
	replicator := Start(&Config{
		Num:               1,
		Peers:             nil,
		TransitionTimeout: 0,
		HeartbeatTimeout:  0,
		Store:             nil,
		AppliedNum:        0,
	})
	var prev proto.HardState
	for {
		tp := <-replicator.Tuple()
		if !IsHardStateEqual(prev, tp.HardState) {
			saveStateToDisk(tp.HardState)
			prev = tp.HardState
		}
		saveEntriesToDisk(tp.PersistentEntries)
		go applyToStore(tp.ApplicableEntries)
		sendMessages(tp.Messages)
	}
}
