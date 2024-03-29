package main

import (
    "log"
    "flag"
    "github.com/vmihailenco/pg"
)

var (
	pg_db = flag.String("pg-db", "da_prod", "postgres database")
	pg_host = flag.String("pg-host", "localhost", "postgres hostname")
	listen_port = flag.Int("port", 3644, "port to listen on")
)

func LoadCatalogue() *Catalogue {
    db := pg.Connect(&pg.Options{
        User: "da",
        Database: *pg_db,
        Host: *pg_host,
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
	flag.Parse()

	log.Printf("givi starting up")
	cat := LoadCatalogue()
	log.Printf("loaded %d catalogue items", cat.Size())

	_id := 70290
	sims := cat.FindSimiliarForCatalogueId(_id, 15)
	cat.PrintResults([]int{_id}, sims)

	server := NewGiviServer(cat)
	server.Serve()
}
