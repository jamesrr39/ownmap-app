package errorsx

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
)

// some common error objects to wrap
var (
	ObjectNotFound = errors.New("ObjectNotFound")
)

type kvPairsMapType map[interface{}]interface{}

type Error interface {
	Error() string
	Stack() []byte
}

type Err struct {
	err     error
	kvPairs kvPairsMapType
	stack   []byte
}

func (err *Err) Stack() []byte {
	return err.stack
}

func (err *Err) Error() string {
	var s = err.err.Error()
	var kvStrings []string
	for key, val := range err.kvPairs {
		kvStrings = append(kvStrings, fmt.Sprintf("%s=%#v", key, val))
	}
	if len(kvStrings) > 0 {
		sort.Slice(kvStrings, func(i, j int) bool {
			return kvStrings[i] < kvStrings[j]
		})
		s += fmt.Sprintf(" [%s]", strings.Join(kvStrings, ", "))
	}
	return s
}

// ErrWithStack returns a std-lib error with the stack trace, if an error was passed in.
func ErrWithStack(err Error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("Error: %s\nStack:\n%s\n", err.Error(), err.Stack())
}

// GoString implements the GoStringer interface,
// and so is printed with the %#v fmt directive.
// See https://golang.org/pkg/fmt/ for more details.
func (err *Err) GoString() string {
	return fmt.Sprintf("Error: %q\nStack:\n%s\n", err.Error(), err.Stack())
}

func Errorf(message string, args ...interface{}) Error {
	return &Err{
		fmt.Errorf(message, args...),
		make(kvPairsMapType),
		debug.Stack(),
	}
}

func Wrap(err error, kvPairs ...interface{}) Error {
	if err == nil {
		return nil
	}

	kvPairsMap := make(kvPairsMapType)
	for i := 0; i < len(kvPairs); i = i + 2 {
		k := kvPairs[i]

		var v interface{}
		if len(kvPairs) >= i+2 {
			v = kvPairs[i+1]
		} else {
			v = "[empty]"
		}
		kvPairsMap[k] = v
	}

	errType, ok := err.(*Err)
	if !ok {
		return &Err{
			err,
			kvPairsMap,
			debug.Stack(),
		}
	}

	// merge in kv map
	for k, v := range kvPairsMap {
		errType.kvPairs[k] = v
	}

	return errType
}

// Cause fetches the underlying cause of the error
// this should be used with errors wrapped from errors.New()
func Cause(err error) error {
	errErr, ok := err.(*Err)
	if ok {
		return Cause(errErr.err)
	}

	return err
}
