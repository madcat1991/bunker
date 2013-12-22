package main

import (
    "log"

    "github.com/vmihailenco/pg"
)
func LoadCatalogue() *Catalogue {
    db := pg.Connect(&pg.Options{
        User: "da",
        Database: "da_prod",
        Host: "localhost",
    })
    defer db.Close()

    items, err := GetCatalogueVectors(db)
	if err != nil {
		log.Fatal("Failed to load catalogue: ", err)
	}
	catalogue := CatalogueFromVectors(items)
	return catalogue
}

func main() {
	log.Printf("givi starting up")
	cat := LoadCatalogue()
	log.Printf("loaded %d catalogue items", cat.Size())

	_id := 70290
	sims := cat.FindSimiliarForCatalogueId(_id, 15)
	cat.PrintResults([]int{_id}, sims)
	cat.ScanAll()
}
