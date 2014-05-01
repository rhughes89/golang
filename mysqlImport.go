package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"database/sql"
		_ "github.com/go-sql-driver/mysql"
)

func main () {

	// OPEN THE CSV FILE
	file, err := os.Open("wm.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// CONNECT TO THE DATABASE
	con, err := sql.Open("mysql", "root:@/fct")
	if err != nil {
		fmt.Println("MySQL connect ERROR: ", err)
		return		
	}
	defer con.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(file)
	reader.Comma = ','
	lineCount := 0

	// ITERATE THROUGH THE FILE
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return
		}
		lineCount += 1

		// INSERT INTO THE DATABASE
		_, err = con.Exec("insert into oof (ITEM_ID,WM_DEPT_NUM,WM_ITEM_NUM,WM_HOST_DESCRIPTION,UPC,PRIMARY_SHELF_ID,IS_BASE_ITEM,VARIANT_ITEMS_NUM,BASE_ITEM_ID) values (?,?,?,?,?,?,?,?,?)",record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8])
		if err != nil {
			fmt.Println("ERROR: ",err)		
		}
	
		// LETS GET THAT CHANNEL
		//records := make(chan []string)
		//go func() { records <- record }()
		//data := <-records

	}



	//fmt.Println("PROCESSED: ",lineCount," files")
}
