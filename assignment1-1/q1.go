package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"
	content, err := ioutil.ReadFile(path)

	if err != nil {
		log.Fatal(err)
	}

	texto := string(content)
	textoLowel := strings.ToLower(texto)
	reg := regexp.MustCompile(`[^0-9a-zA-Z\n ]+`)
	textReg := reg.ReplaceAllString(textoLowel, "${1}")
	res := strings.Fields(textReg)

	wordCountList := []WordCount{}
	wordCountMap := map[string]int{}

	for _, v := range res {
		if val, ok := wordCountMap[v]; ok {
			wordCountMap[v] = val + 1
		} else {
			if len(v) >= charThreshold {
				wordCountMap[v] = 1
			}
		}
	}

	for key, count := range wordCountMap {

		wordCountList = append(wordCountList, WordCount{
			Word:  key,
			Count: count,
		})
	}

	sortWordCounts(wordCountList)

	return wordCountList[:numWords]

}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
