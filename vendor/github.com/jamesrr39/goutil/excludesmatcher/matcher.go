package excludesmatcher

type Matcher interface {
	Matches(path string) bool
}
