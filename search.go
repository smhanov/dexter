package dexter

import (
	"container/heap"
	encoder "encoding/gob"
	"errors"
	"log"
	"math"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/reiver/go-porterstemmer"
)

type Index interface {
	Add(id int, doc string, data interface{})
	Remove(id int)
	RemoveMany(id []int)
	Update(id int, data interface{})
	UpdateMany(items map[int]interface{})
	Search(ptrISlice interface{}, query string, n int) []float64
	Print()
	Close()
}

type DocId uint32
type DocNum uint32

const recordLength = 1 + 4 + 4
const useJournal = false

type termWeight struct {
	term   uint64
	weight uint8
	word   string
}

type docWeight struct {
	docNum DocNum
	weight uint8
}

type SearchTerm struct {
	// The document index #s that the search term is in. This might contain removed
	// documents. They are encoded in a compressed form.
	docs []byte

	// The last doc encoded, needed to support compression
	lastInd DocNum

	// The actual number of documents that the search term is in, that have not
	// been removed.
	count uint32
}

type searchIndex struct {
	// mapping from hash of stemmed word to the list of documents that
	// use that word
	terms map[uint64]*SearchTerm

	// sequence of five bytes per doc: docid followed by length
	// (wordcount of document). If the document has been removed, the
	// entry still exists but the length is set to 0. The length is
	// used for the relevance calculation.
	// The numbers are packed together this way to avoid structure padding.
	docs []byte

	// The total number of indexed documents that have not been removed
	numDocs float64

	tokenRe *regexp.Regexp
	mutex   sync.RWMutex

	debugWords map[uint64]string

	dataFile *os.File
}

// The QueryDoc is used to keep track of results while running a query.
type QueryDoc struct {
	docid     DocId
	relevance float64

	// Number of terms from the query that are in this document
	numTerms int

	// The length normalization of the document 1/sqrt(doc length)
	norm float64

	// The position in the data file where to get the document contents
	pos int64
}

// This struct lets us use golang heap and sort.
type QueryDocs struct {
	docs []*QueryDoc
}

const (
	ADD    = 0
	UPDATE = 1
	REMOVE = 2
)

type JournalCommand struct {
	Op      byte
	Id      DocId
	FilePos int64
}

func appendId(b []byte, docid DocNum, lastid DocNum, weight uint8) []byte {
	// encoding:
	// term weight, followed by:
	// high 2 bits: 0: the difference is entirely in the lower portion of this
	// byte
	// high 2 bits: 1: the difference is in the lower portion of this byte plus
	// 1 more byte
	// high 2 bits: 2: the difference is in the lower portion of this byte plus
	// 2 more bytes
	// high 2 bits: 3: the lower potion tells how many bytes to read to get
	// whole docid.
	diff := docid - lastid
	if diff < 0x3f {
		b = append(b, byte(weight), byte(diff))
	} else if diff < 0x3fff {
		b = append(b, byte(weight), byte(0x40|(diff>>8)), byte(diff))

	} else if diff < 0x3fffff {
		b = append(b, byte(weight), byte(0x80|(diff>>16)), byte(diff>>8), byte(diff))

	} else {
		temp := DocNum(0xffffffff)
		size := uint(0)
		for ; size < 5; size += 1 {
			if docid&temp == 0 {
				break
			}
			temp = temp << 8
		}

		b = append(b, byte(weight), byte(0xc0|size))
		for ; size != 0; size -= 1 {
			b = append(b, byte(docid>>((size-1)*8)))
		}
	}

	return b
}

func extractIds(b []byte) []docWeight {
	// Allocate the maximum could possibly need
	ids := make([]docWeight, 0, len(b))
	id := DocNum(0)
	for i := 0; i < len(b); {
		weight := uint8(b[i])
		i += 1

		control := b[i] >> 6
		if control == 0 {
			id += DocNum(b[i]) & 0x3f
			i += 1
		} else if control == 1 {
			id += DocNum(b[i])&0x3f<<8 | DocNum(b[i+1])
			i += 2
		} else if control == 2 {
			id += DocNum(b[i])&0x3f<<16 | DocNum(b[i+1])<<8 | DocNum(b[i+2])
			i += 3
		} else if control == 3 {
			count := b[i] & 0x3f
			id = 0
			i += 1
			for count != 0 {
				id = id<<8 | DocNum(b[i])
				count -= 1
				i += 1
			}
		}
		ids = append(ids, docWeight{id, weight})
	}

	return ids
}

func fnv1a(s string) uint64 {
	const fnvOffset = 0xcbf29ce484222325
	const fnvPrime = 0x100000001b3

	hash := uint64(fnvOffset)
	for _, ch := range []byte(s) {
		hash = hash ^ uint64(ch)
		hash = hash * uint64(fnvPrime)
	}

	return hash
}

func splitWord(wordWithWeight string) (string, uint8) {
	length := len(wordWithWeight)
	if length >= 2 && wordWithWeight[length-2] == ':' {
		digit := wordWithWeight[length-1]
		if digit >= '0' && digit <= '9' {
			return wordWithWeight[:length-2], uint8(digit - '0')
		}
	}

	return wordWithWeight, 1
}

func (idx *searchIndex) stem(doc string) []termWeight {
	results := make([]termWeight, 0, 20)
	doc = strings.ToLower(doc)
	have := map[string]int{}

	for _, word := range idx.tokenRe.FindAllString(doc, -1) {
		if word == "" {
			continue
		}

		word, weight := splitWord(word)

		var stemmed string
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered; cannot stem %s", word)
					stemmed = word
				}
			}()
			stemmed = porterstemmer.StemString(word)
		}()

		index, ok := have[stemmed]
		if ok {
			// avoid duplicate words in a doc; we assume they don't exist in
			// such short documents.
			if weight > results[index].weight {
				results[index].weight = weight
			}
		} else {
			have[stemmed] = len(results)
			results = append(results, termWeight{fnv1a(stemmed), weight, word})
		}
		if idx.debugWords != nil {
			idx.debugWords[fnv1a(stemmed)] = stemmed
		}
	}
	return results
}

func NewIndex(name string) (Index, error) {
	f, err := os.OpenFile(name+".dex", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	index := &searchIndex{
		terms:      make(map[uint64]*SearchTerm),
		tokenRe:    regexp.MustCompile(`[A-Za-z]+:\d|[A-Za-z]+|[0-9]+`),
		debugWords: make(map[uint64]string),
		dataFile:   f,
	}

	//index.restoreJournal()

	return index, nil
}

func (idx *searchIndex) Close() {
	idx.dataFile.Close()
}

func (idx *searchIndex) getDoc(docnum DocNum) (DocId, uint8, int64) {
	l := DocNum(recordLength)

	id := DocId(idx.docs[l*docnum])<<24 |
		DocId(idx.docs[l*docnum+1])<<16 |
		DocId(idx.docs[l*docnum+2])<<8 |
		DocId(idx.docs[l*docnum+3])

	length := uint8(idx.docs[l*docnum+4])

	pos := int64(idx.docs[l*docnum+5])<<24 |
		int64(idx.docs[l*docnum+6])<<16 |
		int64(idx.docs[l*docnum+7])<<8 |
		int64(idx.docs[l*docnum+8])

	return id, length, pos
}

func (idx *searchIndex) addDoc(docid DocId, length uint8, pos int64) {
	idx.docs = append(idx.docs, byte(docid>>24), byte(docid>>16),
		byte(docid>>8), byte(docid), byte(length),
		byte(pos>>24), byte(pos>>16), byte(pos>>8), byte(pos))
}

func (idx *searchIndex) Update(id int, data interface{}) {
	idx.UpdateMany(map[int]interface{}{id: data})
}

func (idx *searchIndex) getKeywords(pos int64) string {
	idx.dataFile.Seek(pos, os.SEEK_SET)
	decoder := encoder.NewDecoder(idx.dataFile)

	var keywords string
	if useJournal {
		var cmd JournalCommand
		decoder.Decode(&cmd) // always add or update
	}

	err := decoder.Decode(&keywords)
	if err != nil {
		panic(err)
	}

	return keywords
}

func (idx *searchIndex) UpdateMany(items map[int]interface{}) {

	type Work struct {
		docnum   DocNum
		docid    DocId
		keywords string
		data     interface{}
	}

	updates := make([]Work, 0, len(items))
	togo := len(items)

	numDocs := DocNum(len(idx.docs) / recordLength)
	var i DocNum
	for i = 0; i < numDocs && togo > 0; i += 1 {
		id, _, pos := idx.getDoc(i)
		if data, ok := items[int(id)]; ok {
			updates = append(updates, Work{i, id, idx.getKeywords(pos), data})
			togo -= 1
		}
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	l := recordLength
	for _, work := range updates {
		pos, _ := idx.dataFile.Seek(0, os.SEEK_END)
		enc := encoder.NewEncoder(idx.dataFile)

		if useJournal {
			enc.Encode(JournalCommand{UPDATE, work.docid, pos})
		}
		enc.Encode(work.keywords)
		enc.Encode(work.data)

		i := int(work.docnum)
		idx.docs[i*l+5] = byte(pos >> 24)
		idx.docs[i*l+6] = byte(pos >> 16)
		idx.docs[i*l+7] = byte(pos >> 8)
		idx.docs[i*l+8] = byte(pos)
	}
}

// restore a journal
func (idx *searchIndex) restoreJournal() {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	decoder := encoder.NewDecoder(idx.dataFile)

	for {
		var cmd JournalCommand
		var text string
		var any interface{}

		if err := decoder.Decode(&cmd); err != nil {
			break
		}

		if cmd.Op != REMOVE {
			if err := decoder.Decode(&text); err != nil {
				log.Panic(err)
				break
			}

			if err := decoder.Decode(&any); err != nil {
				log.Panic(err)
				break
			}
		}

		switch cmd.Op {

		case ADD:
			idx.add(cmd.Id, text, cmd.FilePos)
			break

		case REMOVE:
			idx.Remove(int(cmd.Id))
			break

		case UPDATE:
			panic("not implemented")

		default:
			panic("Error!!!")
		}
	}
	log.Printf("Successfully restored from journal")
}

// Add a document to the index.
func (idx *searchIndex) Add(id int, doc string, data interface{}) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	docid := DocId(id)

	pos, _ := idx.dataFile.Seek(0, os.SEEK_END)
	enc := encoder.NewEncoder(idx.dataFile)
	if useJournal {
		enc.Encode(JournalCommand{ADD, docid, pos})
	}
	enc.Encode(doc)
	enc.Encode(data)

	idx.add(docid, doc, pos)
}

// Add the document but don't update the journal file.
// mutex must be locked already
func (idx *searchIndex) add(docid DocId, doc string, journalPosition int64) {
	docnum := DocNum(len(idx.docs) / recordLength)
	idx.numDocs += 1.0
	stemmed := idx.stem(doc)
	for _, termWeight := range stemmed {
		term, ok := idx.terms[termWeight.term]
		if ok {
			if len(term.docs) == 0 || term.lastInd != docnum {
				term.docs = appendId(term.docs, docnum, term.lastInd,
					termWeight.weight)
				term.lastInd = docnum
				term.count += 1
			}
		} else {
			term = &SearchTerm{
				docs:    appendId([]byte{}, docnum, 0, termWeight.weight),
				lastInd: docnum,
				count:   1,
			}

			idx.terms[termWeight.term] = term
		}
	}

	length := len(stemmed)
	if length > 255 {
		length = 255
	}

	idx.addDoc(docid, uint8(length), journalPosition)
}

func (idx *searchIndex) Remove(id int) {
	idx.RemoveMany([]int{id})
}

func (idx *searchIndex) RemoveMany(idsIn []int) {
	ids := map[DocId]bool{}

	// convert ids to a mapping
	for _, id := range idsIn {
		ids[DocId(id)] = true
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// go through the documents
	var docNum DocNum
	for docNum = 0; docNum < DocNum(len(idx.docs)/recordLength); docNum++ {
		// find out what docid we have reached
		id, length, pos := idx.getDoc(docNum)

		// if the doc is not deleted and we are supposed to remove it,
		if length > 0 && ids[id] {
			// read the doc keywords
			keywords := idx.getKeywords(pos)

			// for each stemmed keyword,
			for _, tw := range idx.stem(keywords) {
				// decrement its count, removing term record if there are no
				// more documents with that term.
				term := idx.terms[tw.term]
				if term == nil {
					// should not happen
					log.Printf("keywords for docid %d were: %s", id, keywords)
					log.Panicf("Term %d (%s) does not exist in the index",
						tw.term, tw.word)
				}

				term.count -= 1
				if term.count == 0 {
					// no more documents contain this term.
					delete(idx.terms, tw.term)
				}
			}

			// mark as removed by setting length to 0
			idx.docs[docNum*recordLength+4] = 0
			idx.numDocs -= 1
		}
	}
}

func (idx *searchIndex) Print() {
	for word, term := range idx.terms {
		relevance := math.Log(idx.numDocs / float64(term.count))
		log.Printf("term %s relevance %g", idx.debugWords[word], relevance)
		for _, dw := range extractIds(term.docs) {
			log.Printf("    doc %d weight %d", dw.docNum, dw.weight)
		}
	}

	for i := 0; i < len(idx.docs); i += recordLength {
		id, length, pos := idx.getDoc(DocNum(i / recordLength))
		log.Printf("doc id %v has length %v stored at %v",
			id, length, pos)
	}
}

// Search the document, returning a maximum of n document ids. They are ordered
// in decreasing order of relevance.
func (idx *searchIndex) Search(ptrISlice interface{}, query string, n int) []float64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	ptrType := reflect.TypeOf(ptrISlice)
	if ptrType.Kind() != reflect.Ptr {
		panic(errors.New("Must pass in a pointer"))
	}

	slicePtrValue := reflect.ValueOf(ptrISlice)
	sliceValue := reflect.Indirect(slicePtrValue)
	sliceValueType := sliceValue.Type()
	sliceElemType := sliceValueType.Elem()
	if sliceValueType.Kind() != reflect.Slice {
		panic(errors.New("Must pass in a ptr to slice"))
	}

	//log.Printf("slicePtrValue=%v sliceValue=%v sliceValueType=%v, sliceElemType=%v",
	//	slicePtrValue, sliceValue, sliceValueType, sliceElemType)

	maxdocs := DocNum(len(idx.docs) / recordLength)
	docmap := make([]int, maxdocs, maxdocs)

	//docmap := make(map[DocId]int)
	var docs []*QueryDoc
	minTerms := 0

	stemmed := idx.stem(query)
	if len(stemmed) <= 2 {
		minTerms = len(stemmed)
	} else {
		minTerms = int(math.Floor(float64(len(stemmed))*0.9 + 0.5))
	}

	numQueryTerms := len(stemmed)

	var relevance []float64
	if numQueryTerms == 0 {
		return relevance
	}

	// for each stemmed word,
	for _, tw := range stemmed {
		// search for the docs it is in.
		term, ok := idx.terms[tw.term]

		// if no docs, continue
		if !ok {
			minTerms -= 1
			continue
		}

		// calculate relevance score. Here we have IDF^2
		relevance := math.Log(idx.numDocs / float64(term.count+1))

		if idx.debugWords != nil {
			log.Printf("Search term %v has doc frequency %v, used in %v of %v docs (%v)", idx.debugWords[tw.term],
				relevance, term.count, idx.numDocs, idx.numDocs/float64(term.count+1))
		}

		relevance += 1.0
		relevance *= relevance

		// for each doc,
		for _, docweight := range extractIds(term.docs) {
			id, length, pos := idx.getDoc(docweight.docNum)

			// Skip removed docs
			if length == 0 {
				continue
			}

			//docIndex, ok := docmap[id]
			docIndex := docmap[docweight.docNum]
			ok := false
			if docIndex > 0 {
				ok = true
				docIndex -= 1
			}

			if idx.debugWords != nil {
				//log.Printf("Found term %s in doc %d", idx.debugWords[tw.term], id)
			}

			// if it is a doc that we have not previously encountered,
			if !ok {
				// create a QueryDoc for it
				docIndex = len(docs)
				norm := 1.0 / math.Sqrt(float64(length))
				docs = append(docs, &QueryDoc{
					docid:     id,
					relevance: relevance * norm * float64(docweight.weight),
					numTerms:  1,
					norm:      norm,
					pos:       pos,
				})
				docmap[docweight.docNum] = docIndex + 1
			} else {
				// update the query doc for it.
				doc := docs[docIndex]
				doc.relevance += relevance * doc.norm * float64(docweight.weight)
				doc.numTerms += 1
			}
		}
	}

	queryDocs := new(QueryDocs)
	log.Printf("Found %d docs", len(docs))

	// Now we have a list of every doc that has at least one of the search
	// terms.
	// Find the top N that meet the requirement and sort by relevance.
	for _, queryDoc := range docs {
		if false && queryDoc.numTerms < minTerms {
			continue
		}

		// Coordination factor
		coordinationFactor := float64(queryDoc.numTerms) / float64(numQueryTerms)
		queryDoc.relevance *= coordinationFactor

		if queryDocs.Len() == n {
			if queryDoc.relevance > queryDocs.docs[0].relevance {
				heap.Pop(queryDocs)
				heap.Push(queryDocs, queryDoc)
			}
		} else {
			heap.Push(queryDocs, queryDoc)
		}
	}

	// return results
	//for i := 0; i < queryDocs.Len(); i++ {
	for i := queryDocs.Len() - 1; i >= 0; i -= 1 {
		queryDoc := heap.Pop(queryDocs).(*QueryDoc)
		idx.dataFile.Seek(queryDoc.pos, os.SEEK_SET)
		decoder := encoder.NewDecoder(idx.dataFile)
		item := reflect.New(sliceElemType)

		// decode journal entry and discard
		if useJournal {
			var cmd JournalCommand
			if err := decoder.Decode(&cmd); err != nil {
				panic(err)
			}
		}

		// first decode the keywords and discard
		var keywords string
		err := decoder.Decode(&keywords)
		if err != nil {
			panic(err)
		}

		// Now decode the item of interest
		err = decoder.Decode(item.Interface())
		if err != nil {
			panic(err)
		}

		sliceValue = reflect.Append(sliceValue, reflect.Indirect(item))
		relevance = append(relevance, queryDoc.relevance)
	}

	reverseSlice(sliceValue.Interface())
	reverseSlice(relevance)

	slicePtrValue.Elem().Set(sliceValue)
	return relevance
}

//panic if s is not a slice
func reverseSlice(s interface{}) {
	size := reflect.ValueOf(s).Len()
	swap := reflect.Swapper(s)
	for i, j := 0, size-1; i < j; i, j = i+1, j-1 {
		swap(i, j)
	}
}
func (self *QueryDocs) Len() int {
	return len(self.docs)
}

func (self *QueryDocs) Less(i, j int) bool {
	return self.docs[i].relevance < self.docs[j].relevance
}

func (self *QueryDocs) Swap(i, j int) {
	self.docs[i], self.docs[j] = self.docs[j], self.docs[i]
}

func (self *QueryDocs) Push(x interface{}) {
	self.docs = append(self.docs, x.(*QueryDoc))
}

func (self *QueryDocs) Pop() interface{} {
	a := self.docs[len(self.docs)-1]
	self.docs = self.docs[:len(self.docs)-1]
	return a
}

func (idx *searchIndex) NumTerms() int {
	return len(idx.terms)
}

func main() {

}
