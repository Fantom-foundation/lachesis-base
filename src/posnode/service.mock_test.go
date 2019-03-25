// Automatically generated by MockGen. DO NOT EDIT!
// Source: wire/service.pb.go

package posnode

import (
	"github.com/Fantom-foundation/go-lachesis/src/posnode/wire"
	gomock "github.com/golang/mock/gomock"
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Mock of NodeClient interface
type MockNodeClient struct {
	ctrl     *gomock.Controller
	recorder *_MockNodeClientRecorder
}

// Recorder for MockNodeClient (not exported)
type _MockNodeClientRecorder struct {
	mock *MockNodeClient
}

func NewMockNodeClient(ctrl *gomock.Controller) *MockNodeClient {
	mock := &MockNodeClient{ctrl: ctrl}
	mock.recorder = &_MockNodeClientRecorder{mock}
	return mock
}

func (_m *MockNodeClient) EXPECT() *_MockNodeClientRecorder {
	return _m.recorder
}

func (_m *MockNodeClient) SyncEvents(ctx context.Context, in *wire.KnownEvents, opts ...grpc.CallOption) (*wire.KnownEvents, error) {
	_s := []interface{}{ctx, in}
	for _, _x := range opts {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "SyncEvents", _s...)
	ret0, _ := ret[0].(*wire.KnownEvents)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockNodeClientRecorder) SyncEvents(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SyncEvents", _s...)
}

func (_m *MockNodeClient) GetEvent(ctx context.Context, in *wire.EventRequest, opts ...grpc.CallOption) (*wire.Event, error) {
	_s := []interface{}{ctx, in}
	for _, _x := range opts {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "GetEvent", _s...)
	ret0, _ := ret[0].(*wire.Event)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockNodeClientRecorder) GetEvent(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetEvent", _s...)
}

func (_m *MockNodeClient) GetPeerInfo(ctx context.Context, in *wire.PeerRequest, opts ...grpc.CallOption) (*wire.PeerInfo, error) {
	_s := []interface{}{ctx, in}
	for _, _x := range opts {
		_s = append(_s, _x)
	}
	ret := _m.ctrl.Call(_m, "GetPeerInfo", _s...)
	ret0, _ := ret[0].(*wire.PeerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockNodeClientRecorder) GetPeerInfo(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	_s := append([]interface{}{arg0, arg1}, arg2...)
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetPeerInfo", _s...)
}

// Mock of NodeServer interface
type MockNodeServer struct {
	ctrl     *gomock.Controller
	recorder *_MockNodeServerRecorder
}

// Recorder for MockNodeServer (not exported)
type _MockNodeServerRecorder struct {
	mock *MockNodeServer
}

func NewMockNodeServer(ctrl *gomock.Controller) *MockNodeServer {
	mock := &MockNodeServer{ctrl: ctrl}
	mock.recorder = &_MockNodeServerRecorder{mock}
	return mock
}

func (_m *MockNodeServer) EXPECT() *_MockNodeServerRecorder {
	return _m.recorder
}

func (_m *MockNodeServer) SyncEvents(_param0 context.Context, _param1 *wire.KnownEvents) (*wire.KnownEvents, error) {
	ret := _m.ctrl.Call(_m, "SyncEvents", _param0, _param1)
	ret0, _ := ret[0].(*wire.KnownEvents)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockNodeServerRecorder) SyncEvents(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SyncEvents", arg0, arg1)
}

func (_m *MockNodeServer) GetEvent(_param0 context.Context, _param1 *wire.EventRequest) (*wire.Event, error) {
	ret := _m.ctrl.Call(_m, "GetEvent", _param0, _param1)
	ret0, _ := ret[0].(*wire.Event)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockNodeServerRecorder) GetEvent(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetEvent", arg0, arg1)
}

func (_m *MockNodeServer) GetPeerInfo(_param0 context.Context, _param1 *wire.PeerRequest) (*wire.PeerInfo, error) {
	ret := _m.ctrl.Call(_m, "GetPeerInfo", _param0, _param1)
	ret0, _ := ret[0].(*wire.PeerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockNodeServerRecorder) GetPeerInfo(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "GetPeerInfo", arg0, arg1)
}
