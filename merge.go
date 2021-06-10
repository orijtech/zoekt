package zoekt

import "fmt"

func merge(ds ...*indexData) (*IndexBuilder, error) {
	if len(ds) == 0 {
		return nil, fmt.Errorf("need 1 or more indexData to merge")
	}

	ib, err := NewIndexBuilder(ds[0].Repository())
	if err != nil {
		return nil, err
	}

	for _, d := range ds {
		for docID := uint32(0); int(docID) < len(d.fileBranchMasks); docID++ {
			doc := Document{
				Name: string(d.fileName(docID)),
				// Content set below since it can return an error
				// Branches set below since it requires lookups
				SubRepositoryPath: d.subRepoPaths[d.subRepos[docID]],
				Language:          d.languageMap[d.languages[docID]],
				// SkipReason not set, will be part of content from original indexer.
				// TODO Symbols
				// TODO SymbolsMetaData
			}

			if doc.Content, err = d.readContents(docID); err != nil {
				return nil, err
			}

			// calculate branches
			{
				mask := d.fileBranchMasks[docID]
				id := uint32(1)
				for mask != 0 {
					if mask&0x1 != 0 {
						doc.Branches = append(doc.Branches, d.branchNames[uint(id)])
					}
					id <<= 1
					mask >>= 1
				}
			}

			if err := ib.Add(doc); err != nil {
				return nil, err
			}
		}
	}

	return ib, nil
}
