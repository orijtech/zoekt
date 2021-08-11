// Copyright 2021 Google Inc. All rights reserved.
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

package zoekt // import "github.com/google/zoekt"

import (
	"bytes"
	"flag"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	ngramDataDir = flag.String("ngramdir", "", "directory containing testdata with files with one word per line")
	docCount     = flag.Int("docs", 0, "number of docs to load, (default 0 for all)")
	hasherNum    = flag.Int("hasher", 0, "index of the hasher to test")
	loadPerc     = flag.String("load", "50", "space-separated lists of target load percentages")
	trigramFpr   = flag.Bool("tri", false, "compute FPR for trigram-based filtering")
)

func TestBloomHasher(t *testing.T) {
	b := makeBloomFilterEmpty()
	hashCount := len(b.hasher([]byte("testing")))
	expected := 3 * len(strings.Split("test testi testin testing esti estin esting stin sting ting", " "))
	if hashCount != expected {
		t.Errorf("hasher(\"testing\") produced %d hashes instead of %d", hashCount, expected)
	}

	inpA := []byte("some inputs to the bloom filter hashing")
	inpB := []byte("SOME inputs to the bloom filter hashing a b cd")
	if !reflect.DeepEqual(b.hasher(inpA), b.hasher(inpB)) {
		t.Errorf("hash(%v) => %v != hash(%v) => %v", inpA, b.hasher(inpA), inpB, b.hasher(inpB))
	}
}

func TestBloomBasic(t *testing.T) {
	b := makeBloomFilterEmpty()

	// Edge case: empty bloom filter resizing
	b1 := b.shrinkToSize(0.9999)
	if b1.Len() != 8 {
		t.Error("Empty bloom filter didn't resize to 1B")
	}

	// Edge case: nearly empty bloom filter resizing
	b.addBytes([]byte("some"))
	b2 := b.shrinkToSize(0.999)
	if b2.Len() != 8 {
		t.Error("Nearly empty bloom filter didn't resize to 1B")
	}

	// these test strings are carefully selected to not collide
	// with the default hash functions.
	inp := []byte(`some different test words that will definitely be present
	within the bloom filter`)
	missed := []byte("somehow another sequences falsified probabilisitically")

	b.addBytes(inp)

	for i := 0; i < 90; i += 5 {
		bi := b.shrinkToSize(float64(i) * .01)
		t.Logf("target %d%% load: shrink %d=>%d bytes, load factor %.07f%% => %.02f%%",
			i, len(b.bits), len(bi.bits), b.load()*100, bi.load()*100)

		for _, w := range bytes.Split(inp, []byte{' '}) {
			if !bi.maybeHasBytes(w) {
				t.Errorf("%d filter should contain %q but doesn't", i, string(w))
			}
		}

		for _, w := range bytes.Split(missed, []byte{' '}) {
			if bi.maybeHasBytes(w) {
				t.Errorf("%d filter shouldn't contain %q but does", i, string(w))
			}
		}
	}
}

func randWord(min, max int, rng *rand.Rand) []byte {
	length := rng.Intn(max-min) + max
	out := make([]byte, length)
	for i := 0; i < length; i++ {
		out[i] = "abcdefghijklmnopqrstuvwxyz0123456789"[rng.Intn(36)]
	}
	return out
}

type customSort struct {
	len  func() int
	less func(i, j int) bool
	swap func(i, j int)
}

func (c *customSort) Len() int           { return c.len() }
func (c *customSort) Less(i, j int) bool { return c.less(i, j) }
func (c *customSort) Swap(i, j int)      { c.swap(i, j) }

func TestBloomFalsePositiveRate(t *testing.T) {
	rng := rand.New(rand.NewSource(123))

	var wg sync.WaitGroup
	var lock sync.Mutex
	cpuCount := runtime.NumCPU()

	var hasher bloomHash
	var hname string

	switch *hasherNum {
	case 0:
		hasher = bloomHasherCRC
		hname = "crc"
	case 1:
		hasher = bloomHasherSipHashK1
		hname = "siphash"
	case 2:
		hasher = bloomHasherSipHashK2
		hname = "siphashk2"
	case 3:
		hasher = bloomHasherSipHashK8
		hname = "siphashk8"
	case 4:
		hasher = bloomHasherSipHashBlockedK1
		hname = "siphashblock512k1"
	case 5:
		hasher = bloomHasherSipHashBlockedK2
		hname = "siphashblock512k2"
	case 6:
		hasher = bloomHasherSipHashBlockedK8
		hname = "siphashblock512k8"
	case 7:
		hasher = bloomHasherCRCBlocked512B
		hname = "crcblock512"
	case 8:
		hasher = bloomHasherCRCBlocked256B
		hname = "crcblock256"
	case 9:
		hasher = bloomHasherCRCBlocked1024B
		hname = "crcblock256"
	case 10:
		hasher = bloomHasherCRCBlocked64B
		hname = "crcblock64"
	case 11:
		hasher = bloomHasherCRCBlocked64B8
		hname = "crcblock64_8"
	case 12:
		hasher = bloomHasherCRCBlocked64B7
		hname = "crcblock64_7"
	case 13:
		hasher = bloomHasherCRCBlocked64B6
		hname = "crcblock64_6"
	case 14:
		hasher = bloomHasherCRCBlocked64B5
		hname = "crcblock64_5"
	case 15:
		hasher = bloomHasherCRCBlocked64B9
		hname = "crcblock64_9"
	case 16:
		hasher = bloomHasher8SipHashBlock64K1
		hname = "siphashblock64k1_8"
	case 17:
		hasher = bloomHasher8SipHashBlock64K2
		hname = "siphashblock64k2_8"
	case 18:
		hasher = bloomHasher8SipHashBlock64K3
		hname = "siphashblock64k3_8"
	case 19:
		hasher = bloomHasher8SipHashBlock64K4
		hname = "siphashblock64k4_8"
	case 20:
		hasher = bloomHasherCRCBlocked64B8K2
		hname = "crcblock64k2_8"
	case 21:
		hasher = bloomHasherCRCBlocked64B8K3
		hname = "crcblock64k3_8"
	case 22:
		hasher = bloomHasherCRCBlocked64B8K4
		hname = "crcblock64k4_8"
	case 23:
		hasher = bloomHasherCRCBlocked64B8K5
		hname = "crcblock64k5_8"
	case 24:
		hasher = bloomHasherCRCBlocked64B8K6
		hname = "crcblock64k6_8"
	case 25:
		hasher = bloomHasherCRCBlocked128B8
		hname = "crcblock128_8"
	case 26:
		hasher = bloomHasherCRCBlocked128B8K2
		hname = "crcblock128k2_8"
	case 27:
		hasher = bloomHasherCRCBlocked128B8K3
		hname = "crcblock128k3_8"
	case 28:
		hasher = bloomHasherCRCBlocked128B8K4
		hname = "crcblock128k4_8"
	case 29:
		hasher = bloomHasherCRCBlocked128B8K5
		hname = "crcblock128k5_8"
	case 30:
		hasher = bloomHasherCRCBlocked128B8K6
		hname = "crcblock128k6_8"
	}
	t.Log("hasher:", hname)

	targetRate := []int{}
	for _, n := range strings.Split(*loadPerc, " ") {
		tr, err := strconv.Atoi(n)
		if err != nil {
			t.Fatal(err)
		}
		targetRate = append(targetRate, tr)
	}
	if len(targetRate) == 0 {
		targetRate = append(targetRate, 50)
	}
	t.Log("load percentage targets:", targetRate)
	totsize := make([]int, len(targetRate))
	docsize := 0

	docs := [][]byte{}
	docNames := []string{}
	blooms := [][]bloom{}

	addDoc := func(name string, doc []byte, parallel bool) ([]bloom, int) {
		b := makeBloomFilterWithHasher(hasher)
		b.addBytes(doc)
		bs := []bloom{}
		for _, r := range targetRate {
			bs = append(bs, b.shrinkToSize(float64(r)*0.01))
		}

		if parallel {
			lock.Lock()
		}
		docNames = append(docNames, name)
		blooms = append(blooms, bs)
		i := len(blooms)
		docs = append(docs, doc)
		docsize += len(doc)
		for i, b := range bs {
			totsize[i] += b.Len()
		}
		if parallel {
			lock.Unlock()
		}
		return bs, i
	}

	if *ngramDataDir != "" {
		dirents, err := os.ReadDir(*ngramDataDir)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(dirents, func(i, j int) bool {
			return dirents[i].Name() < dirents[j].Name()
		})

		if *docCount > 0 {
			dirents = dirents[:*docCount]
		}

		work := make(chan fs.DirEntry)

		for i := 0; i < cpuCount; i++ {
			go func() {
				for dirent := range work {
					doc, err := os.ReadFile(path.Join(*ngramDataDir, dirent.Name()))
					if err != nil {
						t.Error(err)
						return
					}
					b, i := addDoc(dirent.Name(), doc, true)
					if i%100 == 0 {
						fmt.Println(i, bytes.Count(doc, []byte{'\n'}), b[0].Len(), b[0].load(),
							dirent.Name())
					}
					wg.Done()
				}
			}()
		}
		for _, dirent := range dirents {
			if dirent.IsDir() {
				continue
			}
			wg.Add(1)
			work <- dirent
		}
		close(work)
		wg.Wait()
	} else {
		if *docCount == 0 {
			*docCount = 10
		}
		for i := 0; i < *docCount; i++ {
			wordCount := 100 + rng.Intn(100)*rng.Intn(100)
			doc := []byte{}
			for j := 0; j < wordCount; j++ {
				doc = append(doc, randWord(4, 7, rng)...)
				doc = append(doc, '\n')
			}
			b, l := addDoc(fmt.Sprintf("%04d", i), doc, false)
			fmt.Println(l, wordCount, b[0].Len(), b[0].load())
		}
	}

	// sort docs by name for more deterministic output
	sort.Sort(&customSort{
		len:  func() int { return len(docs) },
		less: func(i, j int) bool { return docNames[i] < docNames[j] },
		swap: func(i, j int) {
			docNames[i], docNames[j] = docNames[j], docNames[i]
			docs[i], docs[j] = docs[j], docs[i]
			blooms[i], blooms[j] = blooms[j], blooms[i]
		},
	})

	t.Logf("loaded %d docs (%d MB / avg %d KB)", len(docs), docsize/1024/1024, docsize/len(docs)/1024)

	probes := [][]byte{}
	probeHashes := [][]uint32{}
	for _, doc := range docs {
		ws := bytes.Split(doc, []byte{'\n'})
		n := 0
		for _, w := range ws {
			if len(w) == 0 || len(w) > 20 {
				continue
			}
			if w[0] < '0' || w[0] > '9' {
				ws[n] = w
				n++
			}
		}
		ws = ws[:n]

		rng.Shuffle(len(ws), func(i, j int) {
			ws[i], ws[j] = ws[j], ws[i]
		})
		if len(ws) > 100 {
			ws = ws[:100]
		}
		if len(docs) > 1000 && len(ws) > 10 {
			ws = ws[:10]
		}
		probes = append(probes, ws...)
		for _, w := range ws {
			probeHashes = append(probeHashes, blooms[0][0].hasher(w))
		}
	}
	t.Logf("created %d probes", len(probes))

	fpCount := make([][]int, len(docs)) // false positive
	tpCount := make([][]int, len(docs)) // true positive
	tnCount := make([][]int, len(docs)) // true negative
	// false negative is impossible in bloom filters

	for i := 0; i < len(docs); i++ {
		fpCount[i] = make([]int, len(targetRate)+1)
		tpCount[i] = make([]int, len(targetRate)+1)
		tnCount[i] = make([]int, len(targetRate)+1)
	}

	work := make(chan int)

	for n := 0; n < cpuCount; n++ {
		wg.Add(1)
		go func() {
			for i := range work {
				// compute all ngrams that might be tested to reduce probing
				// time complexity from O(mn) to O(mlogn+nlogn)
				gram := make([]string, 0, len(docs[i])/8)
				// also compute trigrams for FPR baseling
				trigrams := map[string]bool{}
				for _, s := range bytes.Split(docs[i], []byte{'\n'}) {
					for i := 0; i <= len(s)-4; i++ {
						if '0' <= s[i] && s[i] <= '9' {
							continue
						}
						for j := i + 4; j < i+20 && j <= len(s); j++ {
							gram = append(gram, string(s[i:j]))
						}
						if *trigramFpr {
							trigrams[string(s[i:i+3])] = true
							trigrams[string(s[i+1:i+4])] = true
						}
					}
				}
				sort.Strings(gram)

				for j, w := range probes {
					gidx := -1
					trueValue := false

					if *trigramFpr {
						maybeTrigrams := true
						for wo := 0; wo < len(w)-3; wo++ {
							if !trigrams[string(w[wo:wo+3])] {
								maybeTrigrams = false
								break
							}
						}
						if maybeTrigrams {
							gidx = sort.SearchStrings(gram, string(w))
							trueValue = gidx >= 0 && gidx < len(gram) && gram[gidx] == string(w)
							if trueValue {
								tpCount[i][len(targetRate)]++
							} else {
								fpCount[i][len(targetRate)]++
							}
						} else {
							tnCount[i][len(targetRate)]++
						}
					}

					for bn, b := range blooms[i] {
						maybeHas := b.maybeHas(probeHashes[j])
						if maybeHas {
							if gidx == -1 {
								gidx = sort.SearchStrings(gram, string(w))
								trueValue = gidx >= 0 && gidx < len(gram) && gram[gidx] == string(w)
								if gidx >= 0 && gidx < len(gram) && !trueValue && false {
									lock.Lock()
									nb := makeBloomFilterWithHasher(b.hasher)
									fmt.Println(gidx, gram[gidx], string(w), trueValue)
									haveProbes := map[uint32]bool{}
									for _, x := range b.hasher(w) {
										haveProbes[x%uint32(len(b.bits)*8)] = true
									}
									// fmt.Println(haveProbes)
									fmt.Printf("COLLIDERS FOR %q: ", w)
									for _, g := range gram {
										coll := false
										for _, x := range b.hasher([]byte(g)) {
											k := x % uint32(len(b.bits)*8)
											if haveProbes[k] {
												coll = true
												delete(haveProbes, k)
											}
										}
										if coll {
											nb.addBytes([]byte(g))
											fmt.Printf("%q ", g)
											// fmt.Printf("%v ", b.hasher([]byte(g)))
										}

									}
									fmt.Printf("=> %v\n", nb.maybeHasBytes(w))
									// fmt.Println(b.hasher(w), b.hasher([]byte(gram[gidx])))
									lock.Unlock()
								}
							}
							if trueValue {
								tpCount[i][bn]++
							} else {
								fpCount[i][bn]++
							}
						} else {
							tnCount[i][bn]++
						}
					}
				}
			}
			wg.Done()
		}()
	}

	for i := 0; i < len(docs); i++ {
		work <- i
	}
	close(work)
	wg.Wait()

	summer := make([]kahanSummer, len(targetRate)+1)
	for i := 0; i < len(docs); i++ {
		for bn := 0; bn < len(fpCount[i]); bn++ {
			fpr := float64(fpCount[i][bn]) / float64(fpCount[i][bn]+tnCount[i][bn])
			if fpr > 0.1 && false {
				t.Errorf("false positive rate %.04f > 0.01", fpr)
			}
			summer[bn].add(fpr)
		}
		// t.Logf("doc: %d bits: %8d tp: %6d tn: %6d fp: %v", i, blooms[i][0].Len(), tpCount[i][0], tnCount[i][0], fpCount[i])
	}
	fmt.Printf("hash=%s\n", hname)
	fmt.Println("load,fpr,avg size")
	for bn, rate := range targetRate {
		fmt.Printf("%d, %.03f, %d\n", rate, 100*summer[bn].avg(), totsize[bn]/8/len(docs))
	}
	if *trigramFpr {
		fmt.Printf("trigram fpr: %.03f\n", 100*summer[len(targetRate)].avg())
	}
}

type kahanSummer struct { // Kahan Summation
	sum float64
	c   float64
	n   int
}

func (k *kahanSummer) add(x float64) {
	y := x - k.c
	t := k.sum + y
	k.c = (t - k.sum) - y
	k.sum = t
	k.n++
}

func (k *kahanSummer) avg() float64 {
	return k.sum / float64(k.n)
}
