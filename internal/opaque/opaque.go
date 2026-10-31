// Package opaque turns errors of third-party libraries into errors of this module.
package opaque

import (
	"context"
	"errors"
	"fmt"
)

// Error keeps the message of a go-redis or msgpack error but not its chain, so callers cannot
// come to depend on those libraries' error types (LIB-010). Context cancellation and deadlines
// stay matchable with errors.Is.
func Error(operation string, err error) error {
	for _, contextError := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, contextError) {
			return fmt.Errorf("%s: %w", operation, contextError)
		}
	}
	return fmt.Errorf("%s: %s", operation, err.Error())
}
