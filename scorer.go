package orchid_sync

import "math"

// BM25Scorer implements the Okapi BM25 ranking function.
// This scorer is cluster-safe and designed for distributed
// shard aggregation inside FabricStack.
type BM25Scorer struct {
	k1 float64
	b  float64
}

// NewBM25Scorer creates a scorer using Lucene-compatible defaults.
func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{
		k1: 1.2,
		b:  0.75,
	}
}

// Score calculates the BM25 relevance score.
//
// Parameters:
//
//	tf         -> term frequency inside document
//	docLen     -> total token count for document
//	avgDocLen  -> average token count across corpus
//	totalDocs  -> total indexed documents
//	docFreq    -> number of docs containing term
//
// BM25 Formula:
//
//	IDF * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgDocLen))))
func (s *BM25Scorer) Score(
	tf float64,
	docLen float64,
	avgDocLen float64,
	totalDocs int,
	docFreq int,
) float64 {

	// -------------------------------
	// Safety Guards
	// -------------------------------

	if tf <= 0 {
		return 0
	}

	if totalDocs <= 0 {
		return 0
	}

	if docFreq <= 0 {
		docFreq = 1
	}

	if avgDocLen <= 0 {
		avgDocLen = 1
	}

	if docLen <= 0 {
		docLen = 1
	}

	// -------------------------------
	// Inverse Document Frequency (IDF)
	//
	// Lucene-Compatible:
	//
	// log(1 + ((N - n + 0.5)/(n + 0.5)))
	//
	// Prevents negative IDF values and
	// behaves better for distributed indexes.
	// -------------------------------

	numerator := float64(totalDocs-docFreq) + 0.5
	denominator := float64(docFreq) + 0.5

	idf := math.Log(1 + (numerator / denominator))

	// -------------------------------
	// Term Frequency Normalization
	// -------------------------------

	lengthNorm := 1 - s.b + s.b*(docLen/avgDocLen)

	tfNorm := (tf * (s.k1 + 1)) /
		(tf + s.k1*lengthNorm)

	// -------------------------------
	// Final Score
	// -------------------------------

	return idf * tfNorm
}
