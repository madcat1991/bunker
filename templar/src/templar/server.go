package main

import (
	"net/http"
	"log"
	"strconv"
	"io/ioutil"
	"fmt"
	"html/template"
)

type TemplarServer struct {
	catalogue *Catalogue
	givi_client *GiviClient
}

func NewTemplarServer(cat *Catalogue) *TemplarServer {
	s := &TemplarServer{}
	s.catalogue = cat
	s.givi_client = &GiviClient{}
	return s
}

func (self *TemplarServer) Serve() {
	http.HandleFunc("/plaintext/", self.PlaintextDump)
	http.HandleFunc("/render/", self.Render)
	http.HandleFunc("/givi/", self.GiviProxy)
	address := fmt.Sprintf(":%d", *listen_port)
	log.Printf("Templar server listening at %s", address)
	http.ListenAndServe(address, nil)
}

func (self *TemplarServer) ParseCommonParams(r *http.Request) (*Query, error) {
	query := &Query{}

	err := r.ParseForm()
	if err != nil {
		return nil, err
	}
	ids_str := r.Form["id"]
	ids, err := StringArrayToIntArray(ids_str)
	if err != nil {
		return nil, err
	}
	query.Ids = ids

	intOrDefault := func(name string, _default int) (int, error) {
		str_val := r.FormValue(name)
		if str_val != "" {
			val, err := strconv.ParseInt(str_val, 10, 32)
			if err != nil {
				return 0, err
			}
			return int(val), nil
		}
		return _default, nil
	}

	query.Subsite, err = intOrDefault("subsite", 0)
	if err != nil {
		return nil, err
	}
	query.AppVersion, err = intOrDefault("app_version", 0)
	if err != nil {
		return nil, err
	}
	query.Template = r.FormValue("template")
	return query, nil
}

func (self *TemplarServer) PlaintextDump(w http.ResponseWriter, r *http.Request) {
	query, err := self.ParseCommonParams(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	vectors := self.catalogue.Query(query)
	for _, vec := range(vectors) {
		id, kind := vec.IdAndKind()
		fmt.Fprintf(w, "%d %d  | %s\n", id, kind, vec.Title)
		fmt.Fprintf(w, "%s\n", vec.ThumbsIviPoster())
		fmt.Fprintf(w, "\n")
	}
}

func (self *TemplarServer) LoadTemplate(template_name string) (string, error) {
	if template_name == "" {
		template_name = "basic"
	}
	bytes, err := ioutil.ReadFile("templates/" + template_name + ".html")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (self *TemplarServer) Render(w http.ResponseWriter, r *http.Request) {
	query, err := self.ParseCommonParams(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	vectors := self.catalogue.Query(query)
	query.ResultVectors = vectors
	self.RespondWithRenderedTemplate(w, query)
}

func (self *TemplarServer) GiviProxy(w http.ResponseWriter, r *http.Request) {
	catalog_id, err := strconv.ParseInt(r.FormValue("q"), 10, 32)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	query, err := self.ParseCommonParams(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if query.Template == "" {
		query.Template = "givi"
	}

	givi_result, err := self.givi_client.GetSimilar(int(catalog_id))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	query.Ids = givi_result.Ids
	query.ResultHeaderText = givi_result.For
	query.QueryVector = self.catalogue.GetByQid(int(catalog_id))

	vectors := self.catalogue.Query(query)
	query.ResultVectors = vectors
	self.catalogue.AddRandomAlternateResults(query)
	self.RespondWithRenderedTemplate(w, query)
}

func (self *TemplarServer) RespondWithRenderedTemplate(w http.ResponseWriter, query *Query) {
	template_content, err := self.LoadTemplate(query.Template)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	templ, err := template.New(query.Template).Parse(template_content)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	err = templ.Execute(w, query)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}
