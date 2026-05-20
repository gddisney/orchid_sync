package orchid_sync

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// -------------------------------------------------------------------------
// Analyzer Tests
// -------------------------------------------------------------------------

func TestAnalyzer_Tokenize(t *testing.T) {
	analyzer := NewAnalyzer()

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Basic Sentence",
			input:    "The quick brown fox.",
			expected: []string{"quick", "brown", "fox"}, // 'The' is lowercased to 'the' and removed as a stop word
		},
		{
			name:     "Punctuation and Numbers",
			input:    "Hello, world! Welcome to 2026...",
			expected: []string{"hello", "world", "welcome", "to", "2026"}, // 'to' is not in default stop words map
		},
		{
			name:     "Single Character Garbage",
			input:    "a b c d e f g word",
			expected: []string{"word"}, // Single characters and 'a' (stop word) should be dropped
		},
		{
			name:     "Case Sensitivity",
			input:    "WHICH IS THE AND AT",
			expected: nil, // All are stop words when converted to lowercase
		},
		{
			name:     "Complex Delimiters",
			input:    "hyphenated-word and under_score_word",
			expected: []string{"hyphenated", "word", "under", "score", "word"}, // 'and' is a stop word
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.Tokenize(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Tokenize(%q)\nExpected: %v\nGot:      %v", tt.input, tt.expected, result)
			}
		})
	}
}

// -------------------------------------------------------------------------
// BM25Scorer Tests
// -------------------------------------------------------------------------

func TestBM25Scorer_Score(t *testing.T) {
	scorer := NewBM25Scorer()

	t.Run("Standard Scoring", func(t *testing.T) {
		// Document contains term twice, length is average.
		score := scorer.Score(2.0, 100.0, 100.0, 1000, 10)
		if score <= 0 {
			t.Errorf("Expected positive score, got %f", score)
		}
	})

	t.Run("IDF Flooring", func(t *testing.T) {
		// If term appears in almost all documents, IDF would naturally go negative.
		// Our implementation should floor it to 0.01.
		totalDocs := 1000
		docFreq := 999
		tf := 5.0

		score := scorer.Score(tf, 100.0, 100.0, totalDocs, docFreq)

		// Let's manually calculate the expected minimum score based on an IDF of 0.01
		tfNorm := (tf * (scorer.k1 + 1)) / (tf + scorer.k1*(1-scorer.b+scorer.b*(100.0/100.0)))
		expectedMinScore := 0.01 * tfNorm

		// Allow small floating point precision differences
		if math.Abs(score-expectedMinScore) > 1e-6 {
			t.Errorf("Expected floored score around %f, got %f", expectedMinScore, score)
		}
	})

	t.Run("Zero Term Frequency", func(t *testing.T) {
		// If a term doesn't appear in the document, TF is 0, score should be 0.
		score := scorer.Score(0.0, 100.0, 100.0, 1000, 10)
		if score != 0 {
			t.Errorf("Expected score of 0 for TF=0, got %f", score)
		}
	})
}

// -------------------------------------------------------------------------
// Engine Integration Tests
// -------------------------------------------------------------------------

func TestEngine_Initialization(t *testing.T) {
	// Create a temporary directory for the embedded ultimate_db files
	tempDir, err := os.MkdirTemp("", "consummate_test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	// Assign an actual file path inside the temporary directory
	dbPath := filepath.Join(tempDir, "test_engine.db")

	// Assign a test port for the secure_network node
	testPort := 9999

	// Initialize the Engine using the database file path
	engine, err := NewEngine(dbPath, testPort)
	if err != nil {
		t.Fatalf("Failed to initialize Engine: %v", err)
	}

	if engine.db == nil {
		t.Error("Expected engine.db to be initialized, got nil")
	}
	if engine.netNode == nil {
		t.Error("Expected engine.netNode to be initialized, got nil")
	}
	if engine.analyzer == nil {
		t.Error("Expected engine.analyzer to be initialized, got nil")
	}
	if engine.scorer == nil {
		t.Error("Expected engine.scorer to be initialized, got nil")
	}
}

func TestEngine_IndexMock(t *testing.T) {
	// Setup dummy engine
	engine := &Engine{
		analyzer: NewAnalyzer(),
		scorer:   NewBM25Scorer(),
	}

	err := engine.Index("doc-123", "The quick brown fox jumps over the lazy dog.")
	if err != nil {
		t.Errorf("Index method returned an error: %v", err)
	}
	// Currently Index returns nil without fully hitting DB (based on provided code).
	// Once DB writes are implemented, this test will need the tempDir DB setup like above.
}
