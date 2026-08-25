package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registerOnce sync.Once

	MessagesReceived          = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "messages_received_total", Help: "Total number of incoming tguserbot messages received by the service."}, []string{"bot_id", "message_type"})
	MessagesIgnored           = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "messages_ignored_total", Help: "Total number of incoming tguserbot messages ignored by reason."}, []string{"bot_id", "reason"})
	MessagesProcessed         = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "messages_processed_total", Help: "Total number of tguserbot messages processed by status."}, []string{"bot_id", "status"})
	BotLifecycleEvents        = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "bot_lifecycle_total", Help: "Total number of bot lifecycle events by action and status."}, []string{"bot_id", "action", "status"})
	CRMRequests               = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "crm_requests_total", Help: "Total number of CRM requests by direction and status."}, []string{"bot_id", "direction", "status"})
	CRMRequestDuration        = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "tguserbot", Name: "crm_request_duration_seconds", Help: "Duration of CRM requests in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id", "direction"})
	TgUserBotAppSend          = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "tguserbot_send_total", Help: "Total number of outgoing tguserbot sends by status."}, []string{"bot_id", "status"})
	TgUserBotAppSendDuration  = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "tguserbot", Name: "tguserbot_send_duration_seconds", Help: "Duration of outgoing tguserbot sends in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id"})
	MessageProcessingDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "tguserbot", Name: "message_processing_duration_seconds", Help: "Duration of tguserbot message processing in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id", "stage"})
	UserChannelInitDuration   = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "tguserbot", Name: "user_channel_init_duration_seconds", Help: "Duration of user channel initialization in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id", "status"})
	DecryptErrors             = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "decrypt_errors_total", Help: "Total number of decrypt or session-related errors."}, []string{"bot_id", "category"})
	Reconnects                = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "reconnects_total", Help: "Total number of connect and reconnect attempts by result."}, []string{"bot_id", "result"})
	HTTPSRequests             = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "http_requests_total", Help: "Total number of HTTP requests handled by the service."}, []string{"method", "route", "status"})
	HTTPRequestDuration       = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "tguserbot", Name: "http_request_duration_seconds", Help: "Duration of HTTP requests handled by the service.", Buckets: prometheus.DefBuckets}, []string{"method", "route"})
	ActiveSessions            = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "tguserbot", Name: "active_sessions", Help: "Current number of active tguserbot sessions by state."}, []string{"bot_id", "state"})
	ActiveDialogs             = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "tguserbot", Name: "active_dialogs", Help: "Current number of active dialogs tracked in memory."}, []string{"bot_id"})
	OperatorModeDialogs       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "tguserbot", Name: "operator_mode_dialogs", Help: "Current number of dialogs in operator mode."}, []string{"bot_id"})
	AuthSessions              = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "tguserbot", Name: "auth_sessions", Help: "Current number of active web auth sessions."}, []string{"state"})
	CallLifecycleEvents       = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "call_lifecycle_total", Help: "Total number of call lifecycle events by direction, action and status."}, []string{"bot_id", "direction", "action", "status"})
	WebSocketEvents           = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "tguserbot", Name: "websocket_events_total", Help: "Total number of websocket events by route and status."}, []string{"route", "status"})
)

func Register() {
	registerOnce.Do(func() {
		prometheus.DefaultRegisterer = prometheus.WrapRegistererWithPrefix("", prometheus.DefaultRegisterer)
		registerCollector(collectors.NewGoCollector())
		registerCollector(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		registerCollector(MessagesReceived)
		registerCollector(MessagesIgnored)
		registerCollector(MessagesProcessed)
		registerCollector(BotLifecycleEvents)
		registerCollector(CRMRequests)
		registerCollector(CRMRequestDuration)
		registerCollector(TgUserBotAppSend)
		registerCollector(TgUserBotAppSendDuration)
		registerCollector(MessageProcessingDuration)
		registerCollector(UserChannelInitDuration)
		registerCollector(DecryptErrors)
		registerCollector(Reconnects)
		registerCollector(HTTPSRequests)
		registerCollector(HTTPRequestDuration)
		registerCollector(ActiveSessions)
		registerCollector(ActiveDialogs)
		registerCollector(OperatorModeDialogs)
		registerCollector(AuthSessions)
		registerCollector(CallLifecycleEvents)
		registerCollector(WebSocketEvents)
	})
}

func registerCollector(collector prometheus.Collector) {
	if err := prometheus.Register(collector); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return
		}
		panic(err)
	}
}

func Handler() http.Handler {
	Register()
	return promhttp.Handler()
}

func ObserveDuration(observer prometheus.Observer, startedAt time.Time) {
	observer.Observe(time.Since(startedAt).Seconds())
}

func NormalizeRoute(path string) string {
	if path == "" {
		return "unknown"
	}
	if path == "/metrics" {
		return path
	}
	trimmed := strings.TrimSuffix(path, "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

func BotLabel(userID uint32) string {
	return strconv.FormatUint(uint64(userID), 10)
}

func ObserveMessageReceived(userID uint32, messageType string) {
	MessagesReceived.WithLabelValues(BotLabel(userID), normalizeLabel(messageType, "unknown")).Inc()
}

func ObserveMessageIgnored(userID uint32, reason string) {
	MessagesIgnored.WithLabelValues(BotLabel(userID), normalizeLabel(reason, "unknown")).Inc()
}

func ObserveMessageProcessed(userID uint32, status string) {
	MessagesProcessed.WithLabelValues(BotLabel(userID), normalizeLabel(status, "unknown")).Inc()
}

func ObserveBotLifecycle(userID uint32, action, status string) {
	BotLifecycleEvents.WithLabelValues(BotLabel(userID), normalizeLabel(action, "unknown"), normalizeLabel(status, "unknown")).Inc()
}

func ObserveCRMRequest(userID uint32, direction, status string) {
	CRMRequests.WithLabelValues(BotLabel(userID), normalizeLabel(direction, "unknown"), normalizeLabel(status, "unknown")).Inc()
}

func ObserveCRMRequestDuration(userID uint32, direction string, startedAt time.Time) {
	ObserveDuration(CRMRequestDuration.WithLabelValues(BotLabel(userID), normalizeLabel(direction, "unknown")), startedAt)
}

func ObserveMessageProcessingStage(userID uint32, stage string, startedAt time.Time) {
	ObserveDuration(MessageProcessingDuration.WithLabelValues(BotLabel(userID), normalizeLabel(stage, "unknown")), startedAt)
}

func ObserveUserChannelInit(userID uint32, status string, startedAt time.Time) {
	ObserveDuration(UserChannelInitDuration.WithLabelValues(BotLabel(userID), normalizeLabel(status, "unknown")), startedAt)
}

func ObserveDecryptError(userID uint32, category string) {
	DecryptErrors.WithLabelValues(BotLabel(userID), normalizeLabel(category, "unknown")).Inc()
}

func ObserveReconnect(userID uint32, result string) {
	Reconnects.WithLabelValues(BotLabel(userID), normalizeLabel(result, "unknown")).Inc()
}

func SetActiveSessions(userID uint32, state string, count int) {
	ActiveSessions.WithLabelValues(BotLabel(userID), normalizeLabel(state, "unknown")).Set(float64(count))
}

func TrackActiveDialogs(userID uint32, count int) {
	ActiveDialogs.WithLabelValues(BotLabel(userID)).Set(float64(count))
}

func TrackOperatorModeDialogs(userID uint32, count int) {
	OperatorModeDialogs.WithLabelValues(BotLabel(userID)).Set(float64(count))
}

func SetAuthSessions(state string, count int) {
	AuthSessions.WithLabelValues(normalizeLabel(state, "unknown")).Set(float64(count))
}

func ObserveCallLifecycle(userID uint32, direction, action, status string) {
	CallLifecycleEvents.WithLabelValues(BotLabel(userID), normalizeLabel(direction, "unknown"), normalizeLabel(action, "unknown"), normalizeLabel(status, "unknown")).Inc()
}

func ObserveWebSocketEvent(route, status string) {
	WebSocketEvents.WithLabelValues(normalizeLabel(route, "unknown"), normalizeLabel(status, "unknown")).Inc()
}

func normalizeLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
