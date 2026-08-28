package rpc

import (
	"context"
	"encoding/json"
	"net"
	"strings"

	"air_tguserbot/internal/telegram"

	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
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
			protoEvent := toProtoEvent(req.GetUserId(), event)
			if err := stream.Send(protoEvent); err != nil {
				return err
			}
		}
	}
}

func toProtoEvent(_ uint32, event telegram.CallEvent) *CallEvent {
	result := &CallEvent{CallId: event.CallID, Sequence: event.Sequence, TimestampUnixMs: event.Timestamp.UnixMilli(), Provider: CallProvider_CALL_PROVIDER_TELEGRAM, Delta: event.Delta, Text: event.Text, ResponseId: event.ResponseID, Reason: event.Reason}
	if event.Type == "call_connected" {
		result.Type, result.Phase = "call", "connected"
	} else if event.Type == "call_ended" {
		result.Type, result.Phase = "call", "ended"
	} else {
		normalized := model.NormalizeRealtimeEvent(model.RealtimeEvent{Type: event.Type, Text: event.Text, Delta: event.Delta, ResponseID: event.ResponseID, Err: event.Err})
		result.Type, result.Role, result.Phase = normalized.Type, normalized.Role, normalized.Phase
		result.Delta, result.Text, result.ResponseId = normalized.Delta, normalized.Text, normalized.ResponseID
		if normalized.Error != "" {
			result.Error = normalized.Error
		}
		if normalized.Usage != nil {
			result.Usage = structValue(normalized.Usage)
		}
	}
	if event.Err != nil {
		result.Error = event.Err.Error()
	}
	return result
}

func structValue(value any) *structpb.Struct {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var fields map[string]any
	if json.Unmarshal(data, &fields) != nil {
		return nil
	}
	result, err := structpb.NewStruct(fields)
	if err != nil {
		return nil
	}
	return result
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
