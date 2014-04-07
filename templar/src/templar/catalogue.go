package main

import  (
	"math/rand"
)


type Catalogue struct {
	qid_to_vector map[int]*CatalogueVector
	all_qids []int
}

type Query struct {
	Subsite int
	AppVersion int
	Ids []int
	Template string
	QueryVector *CatalogueVector
	ResultVectors []*CatalogueVector
	AlternateResultVectors []*CatalogueVector
	ResultHeaderText string
}

func CatalogueFromVectors(vectors []*CatalogueVector) *Catalogue {
	catalogue := &Catalogue{}
	catalogue.qid_to_vector = make(map[int]*CatalogueVector, len(vectors))
	catalogue.all_qids = make([]int, 0, len(vectors))

	for _, vector := range(vectors) {
		vector.Init()
		catalogue.qid_to_vector[vector.Qid] = vector
		catalogue.all_qids = append(catalogue.all_qids, vector.Qid)
	}
	return catalogue
}

func (self *Catalogue) Size() int {
	return len(self.qid_to_vector)
}

func (self *Catalogue) GetByQid(qid int) *CatalogueVector {
	return self.qid_to_vector[qid]
}

func (self *Catalogue) Query(query *Query) []*CatalogueVector {
	result := make([]*CatalogueVector, 0)
	for _, id := range(query.Ids) {
		vec := self.GetByQid(id)
		if vec == nil {
			continue
		}
		if !vec.AvailableOn(query.Subsite, query.AppVersion) {
			continue
		}
		result = append(result, vec)
	}
	return result
}

func (self *Catalogue) AddRandomAlternateResults(query *Query) {
	result_vector_qids := make(map[int]bool)
	for _, v := range(query.ResultVectors) {
		result_vector_qids[v.Qid] = true
	}

	result := make([]*CatalogueVector, 0, len(query.ResultVectors))
	tries := 50 * len(query.ResultVectors)
	for tries > 0 && len(result) < len(query.ResultVectors) {
		i := rand.Intn(len(self.all_qids))
		qid := self.all_qids[i]
		vector := self.qid_to_vector[qid]
		tries--

		if vector.Poster() == "" {
			continue
		}
		if result_vector_qids[qid] {
			continue
		}
		if !vector.AvailableOn(query.Subsite, query.AppVersion) {
			continue
		}
		result = append(result, vector)
	}

	query.AlternateResultVectors = result
}
