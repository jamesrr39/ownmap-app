package excludesmatcher

import (
	"bufio"
	"io"
	"strings"

	"github.com/gobwas/glob"
)

// ExcludesMatcher is a type that matches file names against excluded names
type ExcludesMatcher struct {
	globs    []glob.Glob
	dirGlobs []glob.Glob
}

// NewExcludesMatcherFromReader creates a new ExcludesMatcher from a reader
func NewExcludesMatcherFromReader(reader io.Reader) (*ExcludesMatcher, error) {
	var matcherPatterns []glob.Glob
	var dirGlobs []glob.Glob

	bufScanner := bufio.NewScanner(reader)
	for bufScanner.Scan() {
		err := bufScanner.Err()
		if nil != err {
			return nil, err
		}
		pattern := strings.TrimSpace(bufScanner.Text())
		if pattern == "" {
			continue
		}

		if strings.HasPrefix(pattern, "#") {
			// line is a comment
			continue
		}

		matcher, err := glob.Compile(pattern)
		if nil != err {
			return nil, err
		}

		if strings.HasSuffix(pattern, "*") {
			dirGlobs = append(dirGlobs, matcher)
		}

		matcherPatterns = append(matcherPatterns, matcher)
	}

	return &ExcludesMatcher{
		globs:    matcherPatterns,
		dirGlobs: dirGlobs,
	}, nil
}

// Matches tests whether a line matches one of the patterns to be excluded
func (e *ExcludesMatcher) Matches(path string) bool {
	for _, matcherGlob := range e.globs {
		doesMatch := matcherGlob.Match(string(path))

		if doesMatch {
			return true
		}
	}
	return false
}
