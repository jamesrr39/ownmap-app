package errorsx

import "log"

// ExitIfErr prints an error message and stack trace, and then exits the application if an error is passed in
func ExitIfErr(err Error) {
	if err != nil {
		log.Fatalf("error: %s\nStack trace:\n%s\n", err.Error(), err.Stack())
	}
}
