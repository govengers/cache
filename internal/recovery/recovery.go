// Package recovery keeps a panic in caller-supplied or background code from crashing the process.
package recovery

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// Run calls work and logs a panic in it to logger as an error naming operation, instead of
// letting it unwind further.
func Run(logger *slog.Logger, operation string, work func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Error(operation+" panicked", "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
		}
	}()
	work()
}
