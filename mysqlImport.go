package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"os"
	"database/sql"
		_ "github.com/go-sql-driver/mysql"
)

func worker(handle *sql.DB,ch chan []string){
	for m := range ch{

	_, err := handle.Exec("insert into test (ITEM_ID,WM_DEPT_NUM,WM_ITEM_NUM,WM_HOST_DESCRIPTION,UPC,PRIMARY_SHELF_ID,IS_BASE_ITEM,VARIANT_ITEMS_NUM,BASE_ITEM_ID) values (?,?,?,?,?,?,?,?,?)",m[0],m[1],m[2],m[3],m[4],m[5],m[6],m[7],m[8])
		if err != nil {
			fmt.Println("INSERT ERROR:",err)		
		}
	}
}

func main () {

	// CLI ARGUMENTS
	csvFile := os.Args[1]
	workerCount := os.Args[2]
	iWorker, err := strconv.Atoi(workerCount)
	if err != nil {
		fmt.Println(iWorker)
	}

	// OPEN THE CSV FILE
	file, err := os.Open(csvFile)
	if err != nil {
		fmt.Println("CSV OPEN ERROR:", err)
		return
	}
	defer file.Close()

	// CONNECT TO THE DATABASE
	con, err := sql.Open("mysql","root:@unix(/tmp/mysql.sock)/fct?loc=Local")
	if err != nil {
		fmt.Println("DB CONNECT ERROR:", err)
		return		
	}
	defer con.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(file)
	reader.Comma = ','
	lineCount := 0

	// CREATE THE CHANNEL
	records_ch := make(chan []string)

	// DO WORKER
	for i := 0; i < iWorker; i++ {
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
		lineCount ++
	}
	fmt.Println("PROCESSED",lineCount,"RECORDS")
}
