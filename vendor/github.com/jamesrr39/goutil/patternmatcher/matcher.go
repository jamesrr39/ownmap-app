package patternmatcher

type Matcher interface {
	Matches(path string) bool
}
