package zoekt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func ConvertTest(src string) error {
	dstF, err := ioutil.TempFile(filepath.Dir(src), filepath.Base(src)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(dstF.Name())
	defer dstF.Close()

	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcF.Close()

	srcIF, err := NewIndexFile(srcF)
	if err != nil {
		return err
	}
	defer srcIF.Close()

	srcSearcher, err := NewSearcher(srcIF)
	if err != nil {
		return err
	}
	srcD := srcSearcher.(*indexData)

	builder, err := convert(srcD)
	if err != nil {
		return err
	}
	builder.indexTime = srcD.metaData.IndexTime
	Version = srcD.metaData.ZoektVersion

	err = builder.Write(dstF)
	if err != nil {
		return err
	}

	srcF.Close()
	srcF, err = os.Open(src)
	if err != nil {
		return err
	}
	defer srcF.Close()

	dstF.Seek(0, 0)

	a := make([]byte, 64000)
	b := make([]byte, 64000)
	for {
		n1, err1 := srcF.Read(a)
		a = a[:n1]
		n2, err2 := dstF.Read(b)
		b = b[:n2]
		if err1 != err2 {
			return fmt.Errorf("different errors: %v and %v", err1, err2)
		}
		if !bytes.Equal(a, b) {
			return fmt.Errorf("files are different")
		}
		if err1 == io.EOF {
			break
		}
	}

	return nil
}

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
	ib.contentPostings.isPlainASCII = d.metaData.PlainASCII
	ib.namePostings.runeOffsets = d.fileNameRuneOffsets
	ib.namePostings.endRunes = d.fileNameEndRunes
	ib.namePostings.isPlainASCII = d.metaData.PlainASCII

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
