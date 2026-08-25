package rpc

import (
	"context"
	"testing"

	"air_tguserbot/internal/telegram"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testCallAPI struct {
	events chan telegram.CallEvent
}

func (a *testCallAPI) InitiateOutgoingCall(context.Context, uint32, string) (*telegram.Call, error) {
	return nil, nil
}
func (a *testCallAPI) SubscribeCallEvents(context.Context, uint32, string, uint64) (<-chan telegram.CallEvent, error) {
	return a.events, nil
}
func (a *testCallAPI) HangupCall(uint32, string) error { return nil }

func TestStartOutgoingCallValidatesRequest(t *testing.T) {
	s := NewServer(&testCallAPI{}, nil)
	_, err := s.StartOutgoingCall(context.Background(), &StartOutgoingCallRequest{UserId: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestSubscribeCallEventsForwardsRealtimeEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan telegram.CallEvent, 1)
	events <- telegram.CallEvent{CallID: "123", Sequence: 1, Type: "response_done", Text: "готово"}
	close(events)
	s := NewServer(&testCallAPI{events: events}, nil)
	stream := &collectingStream{ctx: ctx}
	if err := s.SubscribeCallEvents(&SubscribeCallEventsRequest{UserId: 1, CallId: "123"}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.events) != 1 || stream.events[0].GetText() != "готово" {
		t.Fatalf("events = %#v", stream.events)
	}
	if stream.events[0].GetProvider() != CallProvider_CALL_PROVIDER_TELEGRAM {
		t.Fatalf("provider = %v", stream.events[0].GetProvider())
	}
}

func TestHangupCallValidatesRequest(t *testing.T) {
	s := NewServer(&testCallAPI{}, nil)
	_, err := s.HangupCall(context.Background(), &HangupCallRequest{UserId: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v", status.Code(err))
	}
}

type collectingStream struct {
	ctx    context.Context
	events []*CallEvent
}

func (s *collectingStream) SetHeader(metadata.MD) error  { return nil }
func (s *collectingStream) SendHeader(metadata.MD) error { return nil }
func (s *collectingStream) SetTrailer(metadata.MD)       {}
func (s *collectingStream) Context() context.Context     { return s.ctx }
func (s *collectingStream) Send(event *CallEvent) error {
	s.events = append(s.events, event)
	return nil
}
func (s *collectingStream) SendMsg(any) error { return nil }
func (s *collectingStream) RecvMsg(any) error { return nil }
