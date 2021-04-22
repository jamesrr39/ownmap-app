package excludesmatcher

import (
	"strings"
)

type SimplePrefixMatcher struct {
	Prefix string
}

func NewSimplePrefixMatcher(prefix string) *SimplePrefixMatcher {
	return &SimplePrefixMatcher{prefix}
}

func (m *SimplePrefixMatcher) Matches(relativePath string) bool {
	return strings.HasPrefix(relativePath, m.Prefix)
}
