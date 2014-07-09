package main

import (
	"fmt"
	"database/sql"
		_ "github.com/go-sql-driver/mysql"
	"github.com/garyburd/redigo/redis"
	"os"
	"io"
	"io/ioutil"
	"encoding/csv"
)

func main () {
	// Connect to Redis
	redisConn, err := redis.Dial("tcp", ":6379")
	if err != nil {fmt.Println("ERROR: Cannot connect to Redis")}

	// Connect to mysql
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
			if k == "Carton Units" || k == "Active Units" || k == "SDC ECom Units" {
				redisConn.Do("HSET","sku:"+v,k,"0")
			} else {
				redisConn.Do("HSET","sku:"+v,k,"")
			}
			redisConn.Do("HSET","CPWM:sku:"+v,k,"")
		}
		redisConn.Do("HSET","sku:"+v,"Product Notes","")
		redisConn.Do("HSET","CPWM:sku:"+v,"Product Notes","")
	}

	// append DB values to sku HASH
	for _,sku := range skus {
		updateSkuHash(sku,redisConn)
	}

	fmt.Println("===============================================================")
	fmt.Println("LineList missing SKUs")
	fmt.Println("===============================================================")
	files, _ := ioutil.ReadDir("linelists/")
	for _, f := range files {
		if f.Name() != ".DS_Store" {
			processLinelist("linelists/"+f.Name(),redisConn)
		}
	}
	fmt.Println("===============================================================","\n")

	fileRecap,err := Map(redisConn.Do("HGETALL","LLRecap"))
	if err != nil {fmt.Println(err)}
	fmt.Println("===============================================================")
	fmt.Println("LineList record count recap")
	fmt.Println("===============================================================")
	for k,v := range fileRecap {
		fmt.Println(k+":",v,"records")
	}
	fmt.Println("===============================================================","\n")

	processDTC("DTC_20140709060001.csv",redisConn)
	processWarehouse("DCAvl_20140709053252.csv",redisConn)

	// compare hashes

	fmt.Println("===============================================================")
	fmt.Println("RealTIME DB vs CPWM data comparison")
	fmt.Println("===============================================================")
	compareHashes(redisConn)
	fmt.Println("===============================================================","\n")
}

func getProducts (dbConn *sql.DB,redisConn redis.Conn) {

	rows,err := dbConn.Query("SELECT id,upc,title,description,note FROM tt_products WHERE account_id = 4")
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
		redisConn.Do("HMSET","sku:"+result[1],"id",result[0],"sku",result[1],"Product Title",result[2],"Web Item Description",result[3],"Product Notes",result[4])
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
		redisConn.Do("HSET","metaData:"+result[0],result[1],result[2])
	}
}

func processDTC(file string,redisConn redis.Conn) {
	dataFile, err := os.Open(file)
	if err != nil {fmt.Println(err)}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:",file, err)
		}
		redisConn.Do("HMSET","CPWM:sku:"+record[0],"Web Item Description",record[1],"Country of Origin",record[3],"Selling Qty",record[4],"Ecom Style #",record[2],"Pad Icon",record[5],"Web Live Date",record[6],"Product Title",record[7])
	}
}

func processLinelist(file string,redisConn redis.Conn) {
	var count int
	dataFile, err := os.Open(file)
	if err != nil {panic(err)}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:",file, err)
			return
		}
		caralho,_ := redis.Int(redisConn.Do("SISMEMBER","skus",record[0]))
		if caralho == 0 && record[0] != "SKU"{
			fmt.Println(record[0],"DOES NOT EXIST IN RealTIME")
		} else {
			redisConn.Do("HMSET","CPWM:sku:"+record[0],"Category",record[2],"Sub Category",record[3],"Item Description",record[4],"Web Item Description",record[4],"Drop?",record[5],"Default Gateway",record[6],"Default Directory 1",record[7],"Default Directory 2",record[8],"Default Directory 3",record[9],"LY SKU",record[10],"Dropdown?",record[11],"SKUs In Dropdown",record[12],"Dropdown Variant",record[13],"Family?",record[14],"SKUs In Family",record[15],"Kit?",record[16],"SKUs In Kit",record[17],"Master SKU",record[18],"Photo Notes",record[19],"BackStory Request",record[20],"Web Live Date",record[21],"Product Notes",record[22],"Product Type",record[23])
		}
		count ++
	}
	redisConn.Do("HSET","LLRecap",file,count)
}

func processWarehouse(file string,redisConn redis.Conn) {
	dataFile, err := os.Open(file)
	if err != nil {panic(err)}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:",file, err)
		}
		redisConn.Do("HMSET","CPWM:sku:"+record[2],"Receipt Date",record[0],"Item Description",record[3],"Active Location",record[4],"Active Units",record[5],"Reserve Units",record[6],"Carton Units",record[7],"SDC ECom Units",record[8],"Receipt Date",record[9],"Active Lock Code",record[10])
	}
}

func compareHashes(redisConn redis.Conn) {

	// get all skus we have in RT db
	skus,_ := Map(redisConn.Do("SMEMBERS","skus"))
	metaFields,_ := Map(redisConn.Do("HGETALL","metaFields"))

	for _,sku := range skus {
		rtData,_ := Map(redisConn.Do("HGETALL","sku:"+sku))
		cpwmData,_ := Map(redisConn.Do("HGETALL","CPWM:sku:"+sku))
		fmt.Println(sku+":")
		for k,_ := range metaFields {
			if rtData[k] != cpwmData[k] {
				fmt.Println("- ["+k+"]",rtData[k],"<>",cpwmData[k])
			}
		}
		fmt.Println(" ")
	}
}

func readFile(file string) (map[string][]string) {
	m := make(map[string][]string)

	dataFile, err := os.Open(file)
	if err != nil {panic(err)}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:",file, err)
		}
		m[record[0]] = record
	}
	return m
}
