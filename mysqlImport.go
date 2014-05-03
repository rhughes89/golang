package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"os"
	"database/sql"
		_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"encoding/json"
)

func worker(handle *sql.DB,ch chan []string){
	for m := range ch{
		_, err := handle.Exec("insert into testTable (ITEM_ID,WM_DEPT_NUM,WM_ITEM_NUM,WM_HOST_DESCRIPTION,UPC,PRIMARY_SHELF_ID,IS_BASE_ITEM,VARIANT_ITEMS_NUM,BASE_ITEM_ID) values (?,?,?,?,?,?,?,?,?)",m[0],m[1],m[2],m[3],m[4],m[5],m[6],m[7],m[8])
		if err != nil {panic(err)}
	}
}


type JSONData struct {
    username string `json:username`
    password string `json:password`
    database string `json:database`
    table string `json:table`
}

func (q *JSONData) FromJSON(file string) error {
    J, err := ioutil.ReadFile(file)
    if err != nil {panic(err)}
    var data = &q
    return json.Unmarshal(J, data)
}

func main () {

    JSONStruct := &JSONData{}
    err := JSONStruct.FromJSON("config.json")
    if err != nil { panic(err) }

	// CLI ARGUMENTS
	csvFile := os.Args[1]
	workerCount, err := strconv.Atoi(os.Args[2])
	if err != nil {panic(err)}

	// OPEN THE CSV FILE
	file, err := os.Open(csvFile)
	if err != nil {panic(err)}
	defer file.Close()

	// CONNECT TO THE DATABASE
	con, err := sql.Open("mysql",JSONStruct.username+":"+JSONStruct.password+"@unix(/tmp/mysql.sock)/test?loc=Local")
	if err != nil {panic(err)}
	defer con.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(file)
	reader.Comma = ','

	// CREATE THE CHANNEL
	records_ch := make(chan []string)

	// DO WORKER
	for i := 0; i < workerCount; i++ {
		go worker(con,records_ch)
	}

	// ITERATE THROUGH THE FILE
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", err)
			return
		}

		// LETS POPULATE THAT CHANNEL
		records_ch <- record
	}
	fmt.Println(JSONStruct.username)
}
