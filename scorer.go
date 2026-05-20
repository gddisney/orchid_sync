package orchid_sync

import "math"

// BM25Scorer holds the tuning parameters for the Okapi BM25 algorithm.
type BM25Scorer struct {
	k1 float64 // Term frequency saturation parameter
	b  float64 // Length normalization parameter
}

func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{
		k1: 1.2, // Standard Elasticsearch defaults
		b:  0.75,
	}
}

// Score calculates the relevance of a term to a specific document.
// tf        = term frequency in this document
// docLen    = length of this document (token count)
// avgDocLen = average length of all documents in the index
// totalDocs = total number of documents in the index
// docFreq   = number of documents containing the term
func (s *BM25Scorer) Score(tf, docLen, avgDocLen float64, totalDocs, docFreq int) float64 {
	// 1. Calculate Inverse Document Frequency (IDF)
	num := float64(totalDocs - docFreq) + 0.5
	den := float64(docFreq) + 0.5
	idf := math.Log10(num / den)
	
	// Floor the IDF to avoid negative scores for extremely common words
	if idf < 0.01 {
		idf = 0.01 
	}

	// 2. Calculate Term Frequency (TF) Normalization
	tfNorm := (tf * (s.k1 + 1)) / (tf + s.k1*(1-s.b+s.b*(docLen/avgDocLen)))

	// 3. Final BM25 Score
	return idf * tfNorm
}
