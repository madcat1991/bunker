package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
)

type GiviClient struct {
}

type ParsedGiviResult struct {
	For string
	Ids []int
}

type JsonGiviResult struct {
	For struct {
		Title string `json:"title"`
	} `json:"for"`
	Recommendations []*struct {
		CatalogueId int `json:"catalogue_id"`
	} `json:"recommendations"`
}

func (self *GiviClient) GetSimilar(qid int) (*ParsedGiviResult, error) {
	url := fmt.Sprintf("http://localhost:3644/similar-content/simple/?q=%d&n=10", qid)

	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	json_res := &JsonGiviResult{}
	err = json.Unmarshal(body, &json_res)
	if err != nil {
		return nil, err
	}

	result := &ParsedGiviResult{}
	result.For = json_res.For.Title
	result.Ids = make([]int, 0)
	for _, s := range(json_res.Recommendations) {
		result.Ids = append(result.Ids, s.CatalogueId)
	}
	return result, nil
}

