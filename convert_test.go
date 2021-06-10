package zoekt

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

func TestConvert(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		b := testIndexBuilder(t, nil,
			Document{
				Name:    "f2",
				Content: []byte("to carry water in the no later bla"),
			},
			Document{
				Name:    "f3/f3",
				Content: []byte("hey this one actually\nblá\nhas\nnew lines"),
			})

		var buf bytes.Buffer
		b.Write(&buf)
		testConvert(t, &memSeeker{buf.Bytes()})
	})

	shards, err := filepath.Glob("testdata/shards/*_v16.*.zoekt")
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range shards {
		name := strings.Split(filepath.Base(p), "_")[0]
		t.Run(name, func(t *testing.T) {
			f, err := os.Open(p)
			if err != nil {
				t.Fatal(err)
			}

			origin, err := NewIndexFile(f)
			if err != nil {
				t.Fatal(err)
			}

			testConvert(t, origin)
		})
	}
}

func testConvert(t *testing.T, origin IndexFile) {
	searcher, err := NewSearcher(origin)
	if err != nil {
		t.Fatal(err)
	}

	b2, err := convert(searcher.(*indexData))
	if err != nil {
		t.Fatal(err)
	}
	b2.indexTime = searcher.(*indexData).metaData.IndexTime

	var buf2 bytes.Buffer
	b2.Write(&buf2)

	assertShardEqual(t, origin, &memSeeker{buf2.Bytes()})
}

func assertShardEqual(t *testing.T, a, b IndexFile) {
	rdA := &reader{r: a}
	rdB := &reader{r: b}

	var tocA, tocB indexTOC
	if err := rdA.readTOC(&tocA); err != nil {
		t.Fatal("readTOC a", t)
	}
	if err := rdB.readTOC(&tocB); err != nil {
		t.Fatal("readTOC b", t)
	}

	tocT := reflect.TypeOf(tocA)

	for i := 0; i < tocT.NumField(); i++ {
		field := tocT.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			valA := getUnexportedField(&tocA, i)
			valB := getUnexportedField(&tocB, i)
			switch valA.(type) {
			case simpleSection:
				if diff := cmp.Diff(readSimpleSection(t, a, valA.(simpleSection)), readSimpleSection(t, b, valB.(simpleSection))); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			case compoundSection:
				if diff := cmp.Diff(readCompoundSection(t, a, valA.(compoundSection)), readCompoundSection(t, b, valB.(compoundSection))); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			case lazyCompoundSection:
				if diff := cmp.Diff(readCompoundSection(t, a, valA.(lazyCompoundSection).compoundSection), readCompoundSection(t, b, valB.(lazyCompoundSection).compoundSection)); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			default:
				t.Fatal("unexpected field type", field.Type)
			}
		})
	}

	// final check that bytes are equal, in case section code misses something.
	readAll := func(f IndexFile) []byte {
		sz, err := f.Size()
		if err != nil {
			t.Fatal(err)
		}
		b, err := f.Read(0, sz)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	if !bytes.Equal(readAll(a), readAll(b)) {
		t.Fatal("bytes for a and b are not equal")
	}
}

func getUnexportedField(p interface{}, fieldIndex int) interface{} {
	field := reflect.ValueOf(p).Elem().Field(fieldIndex)
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func readSimpleSection(t *testing.T, f IndexFile, sec simpleSection) []byte {
	t.Helper()
	b, err := f.Read(sec.off, sec.sz)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readCompoundSection(t *testing.T, f IndexFile, sec compoundSection) [][]byte {
	var b [][]byte
	for i := 0; i < len(sec.offsets); i++ {
		var next uint32
		if i+1 == len(sec.offsets) {
			next = sec.data.off + sec.data.sz
		} else {
			next = sec.offsets[i+1]
		}
		b = append(b, readSimpleSection(t, f, simpleSection{
			off: sec.offsets[i],
			sz:  next - sec.offsets[i],
		}))
	}
	return b
}
