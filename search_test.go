package dexter

import (
	"os"
	"testing"
)

type Product struct {
	Id    int
	Name  string
	Price int
}

var docs = []Product{
	Product{0, "the quick brown fox jumped over the lazy dog", 1},
	Product{1, "canon dslr 13mp", 2},
	Product{2, "canon rebel-x dslr 8mp", 3},
	Product{3, "apple ipad case", 4},
	Product{4, "apple ipad:2 32gb internal 4g with retina display", 5},
}

func Test_Search(t *testing.T) {

	b := appendId([]byte{}, 0, 0, 4)
	b = appendId(b, 1, 0, 1)
	b = appendId(b, 1, 1, 1)
	b = appendId(b, 2, 1, 1)
	b = appendId(b, 5, 2, 1)
	b = appendId(b, 4, 5, 1)
	b = appendId(b, 0x12345678, 4, 1)

	t.Logf("Encoding 0, 1, 1, 2, 5, 4, 0x12345678 to: %v", b)

	ids := extractIds(b)
	t.Logf("Extracted ids %v", ids)
	if ids[0].docNum != 0 ||
		ids[1].docNum != 1 ||
		ids[2].docNum != 1 ||
		ids[3].docNum != 2 ||
		ids[4].docNum != 5 ||
		ids[5].docNum != 4 ||
		ids[6].docNum != 0x12345678 {
		t.Errorf("Did not extract proper ids from encoding")
	}

	if ids[0].weight != 4 {
		t.Errorf("Did not extract proper ids from encoding")
	}

	if len(ids) != 7 {
		t.Errorf("Extracted too many ids from encoding")
	}

	// ---------------------------------
	b = appendId([]byte{}, 2, 0, 1)

	t.Logf("Encoding 2 to: %v", b)

	ids = extractIds(b)
	t.Logf("Extracted ids %v", ids)
	if ids[0].docNum != 2 {
		t.Errorf("Did not extract proper ids from encoding")
	}

	if len(ids) != 1 {
		t.Errorf("Extracted too many ids from encoding")
	}

	// ---------------------------------
	b = appendId([]byte{}, 500000, 0, 1)

	t.Logf("Encoding 500000 to: %v", b)

	ids = extractIds(b)
	t.Logf("Extracted ids %v", ids)
	if ids[0].docNum != 500000 {
		t.Errorf("Did not extract proper ids from encoding")
	}

	if len(ids) != 1 {
		t.Errorf("Extracted too many ids from encoding")
	}

	index, err := NewIndex("test")
	if err != nil {
		t.Errorf("Got error: %v", err)
	}

	for _, doc := range docs {
		index.Add(doc.Id, doc.Name, &doc)
	}

	index.Print()

	count := 0
	var results []Product
	index.Search(&results, "canon dslr", 10)
	for _, doc := range results {
		t.Logf("Got doc %v", doc.Id)
		if doc.Id == 0 {
			t.Errorf("Returned document does not contain search term.")
		}
		count += 1
	}
	t.Logf("Test returned %d results", count)

	if count != 2 {
		t.Errorf("Search did not return expected number of documents.")
	}

	index.Remove(1)
	count = 0
	results = results[:0]
	index.Search(&results, "canon dslr", 10)
	for _, doc := range results {
		t.Logf("Got doc %v", doc)
		if doc.Id == 0 {
			t.Errorf("Returned document does not contain search term.")
		} else if doc.Id == 1 {
			t.Errorf("Search returned a removed document.")
		}
		count += 1
	}

	t.Logf("Test returned %d results", count)

	if count != 1 {
		t.Errorf("Search did not return expected number of documents.")
	}

	docs[0].Price = 67
	index.Update(0, &docs[0])

	count = 0
	results = results[:0]
	index.Search(&results, "fox", 10)
	for _, doc := range results {
		if doc.Id == 0 && doc.Price != 67 {
			t.Errorf("Update had no effect")
		}
		count += 1
	}

	if count != 1 {
		t.Errorf("Search should return 1 result")
	}

	// Search for ipad should return docid 4 then 3 since ipad has a higher
	// weight in the longer doc.
	results = results[:0]
	index.Search(&results, "ipad", 10)
	if len(results) != 2 {
		t.Errorf("Search for ipad should return 2 documents")
	}

	if results[0].Id != 4 {
		t.Errorf("First doc in ipad search should be actual ipad")
	}

	if results[1].Id != 3 {
		t.Errorf("Second doc in ipad search should be ipad case")
	}

	// Search for ipad case should return docid 3 then 4
	results = results[:0]
	index.Search(&results, "case for apple ipad", 10)
	if len(results) != 2 {
		t.Errorf("Search for ipad case should return 2 documents")
	}

	if results[0].Id != 3 {
		t.Errorf("First doc in ipad case search should be ipad case")
	}

	if results[1].Id != 4 {
		t.Errorf("Second doc in ipad case search should be ipad")
	}

	var presults []*Product
	index.Search(&presults, "ipad", 10)
	if len(results) == 0 {
		t.Errorf("Passing a slice of pointers should work too")
	}

	//t.Fail()
	os.Remove("test.dex")
}
