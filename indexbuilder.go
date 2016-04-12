// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codesearch

import (
	"fmt"
	"log"
)

var _ = log.Println

const ngramSize = 3

type searchableString struct {
	// lower cased data.
	data []byte

	// Bit vector describing where we found uppercase letters
	caseBits []byte

	// offset of the content
	offset uint32
}

func (e *searchableString) end() uint32 {
	return e.offset + uint32(len(e.data))
}

func newSearchableString(data []byte, startOff uint32, postings map[string][]uint32) *searchableString {
	dest := searchableString{
		offset: startOff,
	}
	dest.data, dest.caseBits = splitCase(data)
	for i := range dest.data {
		if i+ngramSize > len(dest.data) {
			break
		}
		ngram := string(dest.data[i : i+ngramSize])
		postings[ngram] = append(postings[ngram], startOff+uint32(i))
	}
	return &dest
}

// IndexBuilder builds a single index shard.
type IndexBuilder struct {
	contentEnd uint32
	nameEnd    uint32

	files     []*searchableString
	fileNames []*searchableString

	// ngram => posting.
	contentPostings map[string][]uint32

	// like postings, but for filenames
	namePostings map[string][]uint32
}

func (m *candidateMatch) String() string {
	return fmt.Sprintf("%d:%d", m.file, m.offset)
}

// NewIndexBuilder creates a fresh IndexBuilder.
func NewIndexBuilder() *IndexBuilder {
	return &IndexBuilder{
		contentPostings: make(map[string][]uint32),
		namePostings:    make(map[string][]uint32),
	}
}

// AddFile adds a file. This is the basic ordering for search results,
// so if possible the most important files should be added first.
func (b *IndexBuilder) AddFile(name string, content []byte) {
	b.files = append(b.files, newSearchableString(content, b.contentEnd, b.contentPostings))
	b.fileNames = append(b.fileNames, newSearchableString([]byte(name), b.nameEnd, b.namePostings))
	b.contentEnd += uint32(len(content))
	b.nameEnd += uint32(len(name))
}
