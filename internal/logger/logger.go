package logger

// Logger defines the standard interface for logging across the application.
// This allows replacing the underlying logging implementation
// without changing the core business logic.
type Logger interface {
	Debug(msg any, keyvals ...any)
	Info(msg any, keyvals ...any)
	Warn(msg any, keyvals ...any)
	Error(msg any, keyvals ...any)
	Fatal(msg any, keyvals ...any)
}
