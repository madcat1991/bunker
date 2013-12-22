package main

import  (
	"log"
	"math"
	"time"
)

type Info struct {
	OrigCountry int
	ReleaseYear int
	PropertyValues *IntSet
	Categories *IntSet
	Genres *IntSet
	Persons *IntSet
}

func InfoFromVector(vector *CatalogueVector) *Info {
	info := &Info{}
	info.OrigCountry = vector.OrigCountry
	info.ReleaseYear = vector.ReleaseYear
	info.PropertyValues = NewIntSet(vector.PropertyValues)
	info.Categories = NewIntSet(vector.Categories)
	info.Genres = NewIntSet(vector.Genres)
	info.Persons = NewIntSet(vector.Persons)
	return info
}

type Catalogue struct {
	id_to_title map[int]string
	ids_by_genre map[int][]int
	id_to_info map[int]*Info
}

func CatalogueFromVectors(vectors []*CatalogueVector) *Catalogue {
	catalogue := &Catalogue{}
	catalogue.id_to_title = make(map[int]string, len(vectors))
	catalogue.id_to_info = make(map[int]*Info, len(vectors))
	catalogue.ids_by_genre = make(map[int][]int)

	for _, vector := range(vectors) {
		catalogue.id_to_title[vector.CatalogueId] = vector.Title
		catalogue.id_to_info[vector.CatalogueId] = InfoFromVector(vector)
		for _, genre_id := range(vector.Genres) {
			ids := catalogue.ids_by_genre[genre_id]
			catalogue.ids_by_genre[genre_id] = append(ids, vector.CatalogueId)
		}
	}
	return catalogue
}

func (self *Catalogue) Size() int {
	return len(self.id_to_title)
}

type RecoResult struct {
	metric float64
	catalogue_id int
	explanation string
}

func NewRecoResult(metric float64, catalogue_id int) *RecoResult {
	rr := &RecoResult{}
	rr.metric = metric
	rr.catalogue_id = catalogue_id
	return rr
}

func (self *RecoResult) Score() int {
	return -int(self.metric * 1000)
}

func (self *Catalogue) FindSimiliarForCatalogueId(c1 int, limit int) []*RecoResult {
	c1_info, _ := self.id_to_info[c1]
	ids_to_exclude := NewIntSet([]int{c1})
	return self.FindSimiliarForInfo(c1_info, ids_to_exclude, limit)
}

func (self *Catalogue) FindSimiliarForInfo(c1_info *Info, ids_to_exclude *IntSet, limit int) []*RecoResult {
	ids_to_scan_set := NewIntSet([]int{})
	for _, genre_id := range(c1_info.Genres.AsArray()) {
		ids_to_scan_set.Update(self.ids_by_genre[genre_id])
	}

	ids_to_scan := ids_to_scan_set.AsArray()

	w_country := 1.0
	w_category := 1.0
	w_genre := 3.0
	w_property := 0.5
	w_people := 0.3
	w_date := 1.0

	results := NewPriorityQueue()

	Similiarity := func (c2_info *Info) float64 {
		var score float64 = 0.0
		if c1_info.OrigCountry != 0 && c2_info.OrigCountry == c1_info.OrigCountry {
			score += w_country
		}
		if c1_info.ReleaseYear > 0 && c2_info.ReleaseYear > 0 {
			score += w_date * math.Sqrt(1.0 / (1.0 + math.Abs(float64(c1_info.ReleaseYear - c2_info.ReleaseYear))))
		}
		score += w_genre * c1_info.Genres.WeightedIntersect(c2_info.Genres, nil)
		score += w_property * c1_info.PropertyValues.WeightedIntersect(c2_info.PropertyValues, nil)
		score += w_category * c1_info.Categories.WeightedIntersect(c2_info.Categories, nil)
		score += w_people * c1_info.Persons.WeightedIntersect(c2_info.Persons, nil)
		return score
	};

	norm_total := Similiarity(c1_info)
	if norm_total == 0 {
		return []*RecoResult{}
	}

	for _, c2 := range(ids_to_scan) {
		if ids_to_exclude != nil && ids_to_exclude.Contains(c2) {
			continue
		}
		c2_info, _ := self.id_to_info[c2]
		score := Similiarity(c2_info)

		if score > 1.0 {
			score = score / norm_total
			rr := NewRecoResult(score, c2)
			results.Push(rr)
		}
	}

	results_arr := make([]*RecoResult, 0, limit)
	for {
		if len(results_arr) > limit || results.Count() == 0 {
			break
		}
		rr := results.Pop().(*RecoResult)
		results_arr = append(results_arr, rr)
	}

	return results_arr
}

func (self *Catalogue) ScanAll() {
	n := 0
	t0 := time.Now()
	for c1 := range(self.id_to_title) {
		self.FindSimiliarForCatalogueId(c1, 15)
		n += 1
		if n % 100 == 0 {
			t1 := time.Now()
			speed := float64(n) / float64(t1.Unix() - t0.Unix())
			log.Printf("processed %d catalogue items. Speed: %.1f items / s", n, speed)
		}
	}
}

func (self *Catalogue) PrintResults(catalogue_ids []int, results []*RecoResult) {
	for _, c1 := range(catalogue_ids) {
		title, _ := self.id_to_title[c1]
		log.Printf("Similiar to %d %s", c1, title)
	}
	log.Printf("------")
	for _, rr := range(results) {
		title, _ := self.id_to_title[rr.catalogue_id]
		log.Printf("%.3f\t%d\t%s", rr.metric, rr.catalogue_id, title)
	}

}
