package main

import (
	"fmt"
	"database/sql"
		_ "github.com/go-sql-driver/mysql"
	"github.com/garyburd/redigo/redis"
	"os"
	"io"
	"encoding/csv"
)

func main () {

	// Connect to Redis
	redisConn, err := redis.Dial("tcp", ":6379")
	if err != nil {fmt.Println("ERROR: Cannot connect to Redis")}

	// Connect to mysqln
	dbConn,err := sql.Open("mysql","jrobles:jasmineo212@tcp(i.db1-slave.realtimeprocess.net:3306)/realtime_cpwm")
	if err != nil {fmt.Println("ERROR:","Cannot connect to database")}
	defer dbConn.Close()

	// Get data from DB and add to Redis
	getProducts(dbConn,redisConn)
	getMetaFields(dbConn,redisConn)
	getMetaData(dbConn,redisConn)

	// add blank fields to sku HASH
	skus,_ := redis.Strings(redisConn.Do("SMEMBERS", "skus"))
	metaFields,_ := Map(redisConn.Do("HGETALL","metaFields"))
	for _,v := range skus {
		for k,_ := range metaFields {
			redis.Strings(redisConn.Do("HSET","sku:"+v,k,""))
			redis.Strings(redisConn.Do("HSET","CPWM:sku:"+v,k,""))
		}
	}

	// append DB values to sku HASH
	for _,sku := range skus {
		updateSkuHash(sku,redisConn)
	}

	readDTC("DTC_20140703060001.csv",redisConn)
	readWarehouse("DCAvl_20140703053047.csv",redisConn)
}

func getProducts (dbConn *sql.DB,redisConn redis.Conn) {

	rows,err := dbConn.Query("SELECT id,upc,title,description FROM tt_products WHERE account_id = 4")
	if err != nil {fmt.Println("ERROR:","Cannot connect SELECT products")}

	// Get the columns
	cols, err := rows.Columns()
	if err != nil {fmt.Println(err)}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {fmt.Println("Failed to scan row", err)}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		// Create the redis SET with upc's and the sku HASH
		redis.Strings(redisConn.Do("SADD","skus",result[1]))
		redis.Strings(redisConn.Do("HMSET","sku:"+result[1],"id",result[0],"sku",result[1],"Product Title",result[2],"Web Item Description",result[3]))
	}
}


func getMetaFields (dbConn *sql.DB,redisConn redis.Conn) {

	rows,err := dbConn.Query("SELECT id,field_name FROM tt_meta_fields")
	if err != nil {fmt.Println("ERROR:","Cannot connect SELECT meta fields")}

	// Get the columns
	cols, err := rows.Columns()
	if err != nil {fmt.Println(err)}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {fmt.Println("Fnailed to scan row", err)}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		redis.Strings(redisConn.Do("HSET","metaFields",result[1],result[0]))
	}
}

func Map(do_result interface{}, err error) (map[string] string, error){
	result := make(map[string] string, 0)
	a, err := redis.Values(do_result, err)
	if err != nil {
		return result, err
	}
	for len(a) > 0 {
		var key string
		var value string
		a, err = redis.Scan(a, &key, &value)
		if err != nil {
			return result, err
		}
		result[key] = value
	}
	return result, nil
}

func updateSkuHash(sku string,redisConn redis.Conn) {
	metaData,_ := Map(redisConn.Do("HGETALL","metaData:"+getProductID(sku,redisConn)))
	for k,v := range metaData {
		redis.Strings(redisConn.Do("HSET","sku:"+sku,k,v))
	}
}

func getProductID(sku string,redisConn redis.Conn)(string) {
	productID,_ := Map(redisConn.Do("HGETALL","sku:"+sku))
	return productID["id"]
}

func getMetaData(dbConn *sql.DB,redisConn redis.Conn) {
	caralho,_ := dbConn.Query("SELECT object_id,field_name,value FROM tt_meta_field_datas JOIN tt_meta_fields ON tt_meta_fields.id = tt_meta_field_datas.field_id")

	// Get the columns
	cols, err := caralho.Columns()
	if err != nil {fmt.Println(err)}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for caralho.Next() {
		err = caralho.Scan(dest...)
		if err != nil {fmt.Println("Failed to scan row", err)}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		redis.Strings(redisConn.Do("HSET","metaData:"+result[0],result[1],result[2]))
	}
}

func readDTC(file string,redisConn redis.Conn) {
	// OPEN THE CSV FILE
	DTCfile, err := os.Open(file)
	if err != nil {panic(err)}
	defer DTCfile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(DTCfile)
	reader.Comma = ','

	// get all skus we have in RT db
	skus,_ := Map(redisConn.Do("SMEMBERS","skus"))

	// ITERATE THROUGH THE FILE
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", err)
			return
		}
		for _,v := range skus {
			if v == record[0] {
				fmt.Println(record)
				redis.Strings(redisConn.Do("HMSET","CPWM:sku:"+v,"Web Item Description",record[1],"Country of Origin",record[3],"Selling Qty",record[4],"Ecom Style #",record[2],"Pad Icon",record[5],"Web Live Date",record[6],"Product Title",record[7]))
			}
		}
	}
}

func readWarehouse(file string,redisConn redis.Conn) {
	// OPEN THE CSV FILE
	DTCfile, err := os.Open(file)
	if err != nil {panic(err)}
	defer DTCfile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(DTCfile)
	reader.Comma = ','

	// get all skus we have in RT db
	skus,_ := Map(redisConn.Do("SMEMBERS","skus"))

	// ITERATE THROUGH THE FILE
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", err)
			return
		}
		for _,v := range skus {
			if v == record[2] {
				redis.Strings(redisConn.Do("HMSET","CPWM:sku:"+v,"Receipt Date",record[0],"Item Description",record[3],"Active Location",record[4],"Active Units",record[5],"Reserve Units",record[6],"Carton Units",record[7],"SDC ECom Units",record[8],"Receipt Date",record[9],"Active Lock Code",record[10]))
			}
		}
	}
}
