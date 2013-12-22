package main

import (
    "github.com/vmihailenco/pg"
)

type CatalogueVector struct {
	CatalogueId int
	OrigCountry int
	Title string
	ReleaseYear int
	PropertyValues []int
	Categories []int
	Genres []int
	Persons []int
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
    result := &CatalogueVectors{}
    _, err := db.Query(result,
		"SELECT catalogue_id, orig_country, title, release_year, property_values, categories, genres, persons FROM catalogue_vector")
    if err != nil {
        return nil, err
    }
    return result.Values, nil
}
