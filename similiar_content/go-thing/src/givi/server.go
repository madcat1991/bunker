package main

import (
	"net/http"
	"log"
	"fmt"
	"strconv"
	"encoding/json"
	"time"
)

type GiviServer struct {
	catalogue *Catalogue
}

func NewGiviServer(cat *Catalogue) *GiviServer {
	s := &GiviServer{}
	s.catalogue = cat
	return s
}

func (self *GiviServer) Serve() {
	http.HandleFunc("/similar-content/simple/", self.SimpleSimilarContent)
	address := fmt.Sprintf(":%d", *listen_port)
	log.Printf("Givi server listening at %s", address)
	http.ListenAndServe(address, nil)
}

func (self *GiviServer) SimpleSimilarContent(w http.ResponseWriter, r *http.Request) {
	s_catalogue_id := r.FormValue("q")
	s_count := r.FormValue("n")
	if s_count == "" {
		s_count = "15"
	}
	catalogue_id, err := strconv.Atoi(s_catalogue_id)
	if err != nil {
		http.Error(w, "`q` parameter (catalogue_id) must be provided and must be an integer", 400)
		return
	}
	count, err := strconv.Atoi(s_count)
	if err != nil || count <= 0{
		http.Error(w, "`n` parameter (result count) must be a positive integer", 400)
		return
	}
	log.Printf("requesting simple similiar content for %d (n=%d)", catalogue_id, count)

	_, ok := self.catalogue.id_to_title[catalogue_id]
	if !ok {
		http.Error(w, "404: specified catalogue_id not found", 404)
		return
	}

	t0 := time.Now()
	sims := self.catalogue.FindSimiliarForCatalogueId(catalogue_id, count)
	dt := float64(time.Since(t0)) / float64(time.Second)
	self.FormatSims(w, r, catalogue_id, sims, dt)
}

type GiviJsonResult struct {
	For GiviJsonContentInfo `json:"for"`
	TimeTaken float64 `json:"time_taken"`
	Recommendations []*GiviJsonRecoResult `json:"recommendations"`
}

type GiviJsonRecoResult struct {
	CatalogueId int `json:"catalogue_id"`
	Title string `json:"title"`
	Score float64 `json:"float64"`
}

type GiviJsonContentInfo struct {
	CatalogueId int `json:"catalogue_id"`
	Title string `json:"title"`
}

func (self *GiviServer) FormatSims(w http.ResponseWriter, r *http.Request, _id int, sims []*RecoResult, time_taken float64) {
	res := &GiviJsonResult{}
	res.TimeTaken = time_taken
	res.For.CatalogueId = _id
	res.For.Title = self.catalogue.id_to_title[_id]
	res.Recommendations = make([]*GiviJsonRecoResult, len(sims))
	for i, sim := range(sims) {
		jsim := &GiviJsonRecoResult{}
		jsim.CatalogueId = sim.catalogue_id
		jsim.Score = sim.metric
		jsim.Title, _ = self.catalogue.id_to_title[sim.catalogue_id]
		res.Recommendations[i] = jsim
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		http.Error(w, fmt.Sprintf("While serializing to json: %s", err), 500)
		return
	}
	w.Write(encoded)
}
