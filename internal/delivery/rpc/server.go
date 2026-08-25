package rpc

import (
	"context"
	"net"
	"strings"

	"air_tguserbot/internal/telegram"

	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CallAPI interface {
	InitiateOutgoingCall(context.Context, uint32, string) (*telegram.Call, error)
	SubscribeCallEvents(context.Context, uint32, string, uint64) (<-chan telegram.CallEvent, error)
	HangupCall(uint32, string) error
}

type DB interface {
	GetActiveProvider(userID uint32) (comdom.ProviderType, error)
}

type Server struct {
	UnimplementedCallsServer
	api CallAPI
	db  DB
}

func NewServer(api CallAPI, db DB) *Server { return &Server{api: api, db: db} }

func (s *Server) StartOutgoingCall(ctx context.Context, req *StartOutgoingCallRequest) (*StartOutgoingCallResponse, error) {
	logger.Debug("RPC StartOutgoingCall received")
	if req == nil || req.GetUserId() == 0 || strings.TrimSpace(req.GetTarget()) == "" {
		logger.Error("RPC StartOutgoingCall rejected: invalid user_id or target")
		return nil, status.Error(codes.InvalidArgument, "user_id and target are required")
	}
	logger.Error("RPC StartOutgoingCall: user_id=%d target=%s", req.GetUserId(), strings.TrimSpace(req.GetTarget()))
	call, err := s.api.InitiateOutgoingCall(context.WithoutCancel(ctx), req.GetUserId(), strings.TrimSpace(req.GetTarget()))
	if err != nil {
		logger.Error("RPC StartOutgoingCall failed: %v", err)
		return nil, err
	}
	provider, err := s.db.GetActiveProvider(req.GetUserId())
	if err != nil {
		logger.Error("RPC StartOutgoingCall failed: %v", err)
	}
	logger.Debug("RPC StartOutgoingCall started: user_id=%d call_id=%s", req.GetUserId(), call.ID())
	return &StartOutgoingCallResponse{CallId: call.ID(), Status: "starting", AiProvider: provider.String()}, nil
}

func (s *Server) SubscribeCallEvents(req *SubscribeCallEventsRequest, stream grpc.ServerStreamingServer[CallEvent]) error {
	if req == nil || req.GetUserId() == 0 || strings.TrimSpace(req.GetCallId()) == "" {
		return status.Error(codes.InvalidArgument, "user_id and call_id are required")
	}
	events, err := s.api.SubscribeCallEvents(stream.Context(), req.GetUserId(), strings.TrimSpace(req.GetCallId()), req.GetAfterSequence())
	if err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type == "token_usage" {
				continue
			}
			protoEvent := toProtoEvent(req.GetUserId(), event)
			if err := stream.Send(protoEvent); err != nil {
				return err
			}
		}
	}
}

func toProtoEvent(_ uint32, event telegram.CallEvent) *CallEvent {
	result := &CallEvent{CallId: event.CallID, Sequence: event.Sequence, TimestampUnixMs: event.Timestamp.UnixMilli(), Provider: CallProvider_CALL_PROVIDER_TELEGRAM, Type: callEventType(event.Type, event.Text, event.Delta), Delta: event.Delta, Text: event.Text, ResponseId: event.ResponseID}
	if event.Err != nil {
		result.Error = event.Err.Error()
	}
	return result
}

func callEventType(eventType, text, delta string) CallEventType {
	if value, ok := map[string]CallEventType{
		"call_started":           CallEventType_CALL_STARTED,
		"realtime_starting":      CallEventType_REALTIME_STARTING,
		"realtime_started":       CallEventType_REALTIME_STARTED,
		"realtime_subscribed":    CallEventType_REALTIME_SUBSCRIBED,
		"audio_bridge_started":   CallEventType_AUDIO_BRIDGE_STARTED,
		"call_connected":         CallEventType_CALL_CONNECTED,
		"input_transcript_delta": CallEventType_INPUT_TRANSCRIPT_DELTA,
		"transcript_delta":       CallEventType_INPUT_TRANSCRIPT_DELTA,
		"input_transcript_done":  CallEventType_INPUT_TRANSCRIPT_DONE,
		"transcript":             CallEventType_INPUT_TRANSCRIPT_DONE,
		"response_started":       CallEventType_RESPONSE_STARTED,
		"response_text_delta":    CallEventType_RESPONSE_TEXT_DELTA,
		"response_done":          CallEventType_RESPONSE_DONE,
		"error":                  CallEventType_ERROR,
		"call_ended":             CallEventType_CALL_ENDED}[eventType]; ok {
		return value
	}
	if text != "" || delta != "" {
		return CallEventType_RESPONSE_TEXT_DELTA
	}
	return CallEventType_RESPONSE_DONE
}

func (s *Server) HangupCall(_ context.Context, req *HangupCallRequest) (*HangupCallResponse, error) {
	logger.Debug("RPC HangupCall received")
	if req == nil || req.GetUserId() == 0 || strings.TrimSpace(req.GetCallId()) == "" {
		logger.Error("RPC HangupCall rejected: invalid user_id or call_id")
		return nil, status.Error(codes.InvalidArgument, "user_id and call_id are required")
	}
	logger.Debug("RPC HangupCall: user_id=%d call_id=%s", req.GetUserId(), req.GetCallId())
	if err := s.api.HangupCall(req.GetUserId(), strings.TrimSpace(req.GetCallId())); err != nil {
		logger.Error("RPC HangupCall failed: %v", err)
		return nil, err
	}
	logger.Debug("RPC HangupCall accepted: user_id=%d call_id=%s", req.GetUserId(), req.GetCallId())
	return &HangupCallResponse{CallId: req.GetCallId(), Status: "hangup_requested"}, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	RegisterCallsServer(grpcServer, s)
	go func() { <-ctx.Done(); grpcServer.GracefulStop() }()
	logger.Info("Telegram call gRPC server started")
	return grpcServer.Serve(listener)
}
