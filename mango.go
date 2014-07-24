package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)
var status string
type Product struct {
	Id string
	Sku string
	ProductTitle string
	WebItemDescription string
	ProductNotes string
}

type Status struct {
	Status string
}

func main() {

	http.HandleFunc("/", indexAction)
	http.HandleFunc("/create", createAction)
	http.HandleFunc("/read", readAction)
	//http.HandleFunc("/update", updateAction)
	http.HandleFunc("/delete", deleteAction)

	err := http.ListenAndServe(":6900", nil)
	if err != nil {fmt.Println(err)}

}

func insertRecord(c *mgo.Collection, p *Product) {
	//err := c.Insert(&Person{"Ale", "+55 53 8116 9639"},&Person{"Cla", "+55 53 8402 8510"})
	err := c.Insert(p)
	if err != nil {fmt.Println(err)}
}

func getRecord(c *mgo.Collection,field string, value string) (Product) {
	result := Product{}
	err := c.Find(bson.M{field:value}).One(&result)
	if err != nil {fmt.Println(err)}
	return result
}

func indexAction(res http.ResponseWriter, req *http.Request) {
	session, err := mgo.Dial("localhost:27017")
	if err != nil {panic(err)}
	defer session.Close()
	c := session.DB("tt_products").C("products")

	//insertRecord(c,&Product{"12","122333455","Some Title","Some product description"})
	caralho,_ := json.Marshal(getRecord(c,"id","12"))
	res.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(res,string(caralho))
}

func createAction(res http.ResponseWriter, req *http.Request) {
	session, err := mgo.Dial("localhost:27017")
	if err != nil {panic(err)}
	defer session.Close()
	c := session.DB("tt_products").C("products")

	decoder := json.NewDecoder(req.Body)
	var p Product
	err = decoder.Decode(&p)
	if err != nil {
		fmt.Println(err)
		status = "ERROR: INVALID JSON"
	} else {
		err = c.Insert(&Product{p.Id,p.Sku,p.ProductTitle,p.WebItemDescription,p.ProductNotes})
		res.Header().Set("Content-Type", "application/json")
		if err != nil {
			status = "ERROR"
			fmt.Println(err)
		} else {
			status = "SUCCESS"
		}
	}
	b,_ := json.Marshal(&Status{Status:status})
	fmt.Fprintf(res,string(b))
}

func readAction(res http.ResponseWriter, req *http.Request) {
	session, err := mgo.Dial("localhost:27017")
	if err != nil {panic(err)}
	defer session.Close()
	c := session.DB("tt_products").C("products")

	decoder := json.NewDecoder(req.Body)
	var p Product
	err = decoder.Decode(&p)
	if err != nil {fmt.Println(err)}

	caralho,_ := json.Marshal(getRecord(c,"sku",p.Sku))
	res.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(res,string(caralho))
}

func deleteAction(res http.ResponseWriter, req *http.Request) {
	session, err := mgo.Dial("localhost:27017")
	if err != nil {panic(err)}
	defer session.Close()
	c := session.DB("tt_products").C("products")

	decoder := json.NewDecoder(req.Body)
	var p Product
	err = decoder.Decode(&p)
	if err != nil {
		fmt.Println(err)
		status = "ERROR: INVALID JSON"
	} else {
		err = c.Remove(bson.M{"sku":p.Sku})
		res.Header().Set("Content-Type", "application/json")
		if err != nil {
			status = "ERROR"
			fmt.Println(err)
		} else {
			status = "SUCCESS"
		}
	}
	b,_ := json.Marshal(&Status{Status:status})
	fmt.Fprintf(res,string(b))
}
