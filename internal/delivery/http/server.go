package http

import (
	"air_tguserbot/internal/metrics"
	"context"
	stdhttp "net/http"
	"time"

	"github.com/ikermy/air-logger/v2/pkg/logger"
)

type TelegaAppHandlers interface {
	AuthWebSocketHandler(stdhttp.ResponseWriter, *stdhttp.Request)
	HandlerGetContactsWS(stdhttp.ResponseWriter, *stdhttp.Request)
	AvailableHandler(stdhttp.ResponseWriter, *stdhttp.Request)
	StartBot(stdhttp.ResponseWriter, *stdhttp.Request)
	StopBotsHandler(stdhttp.ResponseWriter, *stdhttp.Request)
	RestartBot(stdhttp.ResponseWriter, *stdhttp.Request)
	GetBotName(stdhttp.ResponseWriter, *stdhttp.Request)
	CallHangupHandler(stdhttp.ResponseWriter, *stdhttp.Request)
}

type Server struct {
	h          TelegaAppHandlers
	httpServer *stdhttp.Server
}

func NewServer(handlers TelegaAppHandlers) *Server { return &Server{h: handlers} }

func enableCORS(next stdhttp.HandlerFunc) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == stdhttp.MethodOptions {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/tguser/ws", metrics.HTTPMiddleware("/ws", stdhttp.HandlerFunc(s.h.AuthWebSocketHandler)))
	mux.Handle("/tguser/contacts/ws", metrics.HTTPMiddleware("/contacts/ws", stdhttp.HandlerFunc(s.h.HandlerGetContactsWS)))
	mux.Handle("/tguser/available", metrics.HTTPMiddleware("/available", enableCORS(s.h.AvailableHandler)))
	mux.Handle("/tguser/getname", metrics.HTTPMiddleware("/getname", enableCORS(s.h.GetBotName)))
	mux.Handle("/tguser/enable", metrics.HTTPMiddleware("/enable", enableCORS(s.h.StartBot)))
	mux.Handle("/tguser/disable", metrics.HTTPMiddleware("/disable", enableCORS(s.h.StopBotsHandler)))
	mux.Handle("/tguser/restart", metrics.HTTPMiddleware("/restart", enableCORS(s.h.RestartBot)))
	mux.Handle("/tguser/call/hangup", metrics.HTTPMiddleware("/call/hangup", enableCORS(s.h.CallHangupHandler)))
	return mux
}

func (s *Server) ListenAndServe(addr string) error {
	logger.Info("Сервер аутентификации запущен")
	s.httpServer = &stdhttp.Server{Addr: addr, Handler: s.Handler()}
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown() {
	if s.httpServer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.httpServer.Shutdown(ctx)
}
