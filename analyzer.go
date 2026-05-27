package orchid_sync

import (
	"strings"
	"unicode"
)

// Analyzer processes raw text into indexable tokens.
// It is completely stateless and safe for concurrent use across multiple indexing goroutines.
type Analyzer struct {
	stopWords map[string]bool
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{
		stopWords: map[string]bool{
			"the": true, "is": true, "at": true, "which": true, "on": true,
			"and": true, "a": true, "an": true, "in": true, "of": true,
			"to": true, "for": true, "with": true, "by": true, "as": true,
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
	
	// Performance Optimization: Pre-allocate the slice capacity to prevent 
	// expensive array re-allocations and memory copying during the append loop.
	tokens := make([]string, 0, len(rawTokens))
	
	for _, t := range rawTokens {
		clean := strings.ToLower(t)
		// Filter out stop words and single-character garbage
		if !a.stopWords[clean] && len(clean) > 1 {
			tokens = append(tokens, clean)
		}
	}
	
	return tokens
}
