package main

import (
    "log"
    "flag"
    "github.com/vmihailenco/pg"
)

var (
	pg_db = flag.String("pg-db", "da", "postgres database")
	pg_host = flag.String("pg-host", "localhost", "postgres hostname")
	listen_port = flag.Int("port", 3655, "port to listen on")
	hydra_url = flag.String(
		"recsys",
		"http://192.168.142.109:8080/non_pers/movie/recommendations/",
		"recommender system url")
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

	log.Printf("Templar starting up")
	cat := LoadCatalogue()
	log.Printf("loaded %d catalogue items", cat.Size())

	server := NewTemplarServer(cat, *hydra_url)
	server.Serve()
}
