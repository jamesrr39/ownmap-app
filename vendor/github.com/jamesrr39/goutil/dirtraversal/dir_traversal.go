package dirtraversal

import (
	"path/filepath"
	"strings"
)

// IsTryingToTraverse detects if a filepath is trying to traverse up into a parent directory
// note, it does not detect for paths beginning with '/' example: '/etc', just paths trying to go up with ..
func IsTryingToTraverseUp(s string) bool {
	sep := string(filepath.Separator)

	for _, fragment := range strings.Split(s, sep) {
		if ".." == fragment {
			return true
		}
	}
	return false
}
