package orchid_sync

import (
	"strings"
	"unicode"
)

// Analyzer processes raw text into indexable tokens.
type Analyzer struct {
	stopWords map[string]bool
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{
		stopWords: map[string]bool{
			"the": true, "is": true, "at": true, "which": true, "on": true,
			"and": true, "a": true, "an": true, "in": true, "of": true,
		},
	}
}

// Tokenize splits strings, normalizes them, and filters stop words.
func (a *Analyzer) Tokenize(text string) []string {
	// Split by anything that isn't a letter or number
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	rawTokens := strings.FieldsFunc(text, f)
	
	var tokens []string
	for _, t := range rawTokens {
		clean := strings.ToLower(t)
		// Filter out stop words and single-character garbage
		if !a.stopWords[clean] && len(clean) > 1 {
			tokens = append(tokens, clean)
		}
	}
	return tokens
}
