package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Product struct {
	Id string
	Sku string
	ProductTitle string
	WebItemDescription string
	ProductNotes string
	PadIcon string
	SubDirectory string
	DefaultDirectory2 string
	Family  string
	Kit string
}

func main() {

	http.HandleFunc("/", indexAction)
	http.HandleFunc("/create", createAction)

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


func createAction(res http.ResponseWriter, req *http.Request) {
//	session, err := mgo.Dial("localhost:27017")
//	if err != nil {panic(err)}
//	defer session.Close()
//	c := session.DB("tt_products").C("products")

	decoder := json.NewDecoder(req.Body)
	var p Product
	err := decoder.Decode(&p)
	if err != nil {fmt.Println(err)}

	//insertRecord(c,&Product{"12","122333455","Some Title","Some product description"})
	fmt.Println(p)
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
	//fmt.Fprintf(res,"oof")
}
