package zoekt

import (
	"encoding/binary"
)

// convert will create the equivalent IndexBuilder for d. Writing this file
// out should result in the same on shard.
//
// This function is a stepping stone to merging.
func convert(d *indexData) (*IndexBuilder, error) {
	ib, err := NewIndexBuilder(d.Repository())
	if err != nil {
		return nil, err
	}

	for docID := 0; docID+1 < len(d.boundaries); docID++ {
		content, err := d.readContents(uint32(docID))
		if err != nil {
			return nil, err
		}
		ib.contentStrings = append(ib.contentStrings, &searchableString{content})

		fileName := d.fileNameContent[d.fileNameIndex[docID]:d.fileNameIndex[docID+1]]
		ib.nameStrings = append(ib.nameStrings, &searchableString{fileName})

		// docSections offsets are relative to the document. So we should be able
		// to just directly copy the sections here and avoid cost of
		// unmarshal/marshal.
		docSections, _, err := d.readDocSections(uint32(docID), nil)
		if err != nil {
			return nil, err
		}
		ib.docSections = append(ib.docSections, docSections)
	}

	ib.contentPostings.runeOffsets = d.runeOffsets
	ib.contentPostings.endRunes = d.fileEndRunes
	ib.namePostings.runeOffsets = d.fileNameRuneOffsets
	ib.namePostings.endRunes = d.fileNameEndRunes

	for ngram, sec := range d.ngrams {
		ib.contentPostings.postings[ngram], err = d.readSectionBlob(sec)
		if err != nil {
			return nil, err
		}
	}

	for ngram, offsets := range d.fileNameNgrams {
		ib.namePostings.postings[ngram] = toDeltas(offsets)
		if err != nil {
			return nil, err
		}
	}

	ib.languageMap = d.metaData.LanguageMap
	ib.checksums = d.checksums
	ib.languages = d.languages
	ib.subRepos = d.subRepos
	ib.branchMasks = d.fileBranchMasks
	ib.fileEndSymbol = d.fileEndSymbol
	ib.runeDocSections = d.runeDocSections

	// symbols data is tricky.
	symbolsSingleton := []*Symbol{nil}
	symbolsCount := len(d.symbols.symMetaData) / (4 * 4)
	for i := 0; i < symbolsCount; i++ {
		symbolsSingleton[0] = d.symbols.data(uint32(i))
		ib.addSymbols(symbolsSingleton)
	}

	return ib, nil
}

func toDeltas(offsets []uint32) []byte {
	var enc [8]byte

	deltas := make([]byte, 0, len(offsets)*2)

	var last uint32
	for _, p := range offsets {
		delta := p - last
		last = p

		m := binary.PutUvarint(enc[:], uint64(delta))
		deltas = append(deltas, enc[:m]...)
	}
	return deltas
}
