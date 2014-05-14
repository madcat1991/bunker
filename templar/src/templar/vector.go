package main

import (
	"fmt"
    "github.com/vmihailenco/pg"
	/*"io/ioutil"*/
)

const (
	KIND_CONTENT = 1
	KIND_COMPILATION = 2
)

type CatalogueVector struct {
    Qid int
    Title string
    Content_paid_types []string
    Is_active bool
    Subsite_ids []int
    Versions_with_allowed_formats []int
    Hru string
    Posters []string
	mapSubsiteIds map[int]bool
	mapVersions map[int]bool
	IsPaid bool
}

func (this *CatalogueVector) Init() {
	this.mapSubsiteIds = IntarrayToSet(this.Subsite_ids)
	this.mapVersions = IntarrayToSet(this.Versions_with_allowed_formats)

	this.IsPaid = true
	for _, c_p_t := range(this.Content_paid_types) {
		if c_p_t == "AVOD" {
			this.IsPaid = false
		}
	}
}

func (this *CatalogueVector) IdAndKind() (int, int) {
	id := this.Qid / 10
	kind := this.Qid % 10 + 1
	return id, kind
}

func (this *CatalogueVector) IviWatchLink() string {
	id, kind := this.IdAndKind()
	if kind == KIND_COMPILATION {
		return "/watch/" + this.Hru
	}
	return fmt.Sprintf("/watch/%d", id)
}

func (this *CatalogueVector) Poster() string {
	if len(this.Posters) > 0 {
		return this.Posters[0]
	}
	return ""
}

func (this *CatalogueVector) ThumbsIviPoster() string {
	p := this.Poster()
	if p != "" {
		p = "//thumbs.ivi.ru/" + p
	}
	return p
}

func (this *CatalogueVector) AvailableOn(subsite int, appversion int) bool {
	if !this.Is_active {
		return false
	}
	if appversion != 0 && !this.mapVersions[appversion] {
		return false
	}
	if subsite != 0 && !this.mapSubsiteIds[subsite] {
		return false
	}
	if appversion == 0 && subsite == 0 {
		return true
	}
	return true
}

type CatalogueVectors struct {
    Values []*CatalogueVector
}

func (f *CatalogueVectors) New() interface{} {
    u := &CatalogueVector{}
    f.Values = append(f.Values, u)
    return u
}

func GetCatalogueVectors(db *pg.DB) ([]*CatalogueVector, error) {
	/*query, err := ioutil.ReadFile("catalogue_query.sql")*/
	/*if err != nil {*/
		/*return nil, err*/
	/*}*/
    result := &CatalogueVectors{}
    /*_, err = db.Query(result, string(query))*/
    _, err := db.Query(result, `
    	SELECT
    		qid,
    		title,
    		is_active,
    		versions_with_allowed_formats,
    		content_paid_types,
    		subsite_ids,
    		posters,
    		hru
    	FROM catalogue_table`)

    if err != nil {
        return nil, err
    }
    return result.Values, nil
}
