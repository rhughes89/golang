package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var ops uint64 = 0

func main() {

	//Authenticate AWS
	auth, err := aws.GetAuth("AKIAJIEYRNMCD32RKBRQ", "uRvtAs2VcBlC+PhIzyZMy2RKzvkEHmM8ZlW6+7AM")
	if err != nil {
		fmt.Println("Error Authentication AWS: ", err)
	}
	client := s3.New(auth, aws.USEast)

	lineListDir, dtcFile, warehouseFile := downloadFeeds(client)

	dbServer := flag.String("dbserver", "10.31.189.212", "Database Server")
	dbName := flag.String("dbname", "realtime_cpwm_prod", "Database name")
	flag.Parse()

	// Connect to Redis
	redisConn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println("ERROR: Cannot connect to Redis")
	}

	// Connect to mysql
	dbConn, err := sql.Open("mysql", "app_others:W8HJRy2V6WGbCv0@tcp("+*dbServer+":3306)/"+*dbName)
	if err != nil {
		fmt.Println("ERROR:", "Cannot connect to database")
	}
	defer dbConn.Close()

	// clear out the SKUs SET
	redisConn.Do("DEL", "skus")
	redisConn.Do("DEL", "linelists")
	redisConn.Do("DEL", "LLRecap")
	redisConn.Do("DEL", "metaFields")
	redisConn.Do("DEL", "metaData")

	// Get data from DB and add to Redis
	getProducts(dbConn, redisConn)
	getMetaFields(dbConn, redisConn)
	getMetaData(dbConn, redisConn)

	// add blank fields to sku HASH
	skus, _ := redis.Strings(redisConn.Do("SORT", "skus", "ALPHA"))
	metaFields, _ := Map(redisConn.Do("HGETALL", "metaFields"))
	for _, v := range skus {
		for k, _ := range metaFields {
			if k == "Carton Units" || k == "Active Units" || k == "SDC ECom Units" {
				redisConn.Do("HSET", "sku:"+v, k, "0")
			} else {
				redisConn.Do("HSET", "sku:"+v, k, "")
			}
			redisConn.Do("HSET", "CPWM:sku:"+v, k, "")
		}
		redisConn.Do("HSET", "sku:"+v, "Product Notes", "")
		redisConn.Do("HSET", "CPWM:sku:"+v, "Product Notes", "")
	}

	/*
		a := make([]string, 0)
		for k,_ := range metaFields {
			a[0] = k
			if k == "Carton Units" || k == "Active Units" || k == "SDC ECom Units" {
				a[0] = "0"
			}else {
				a[0] = ""
			}
		}
		for _,v := range skus {
			redisConn.Do("HMSET","sku:"+v,a...)
			redisConn.Do("HMSET","CPWM:sku:"+v,a...)
			redisConn.Do("HSET","sku:"+v,"Products Notes","")
			redisConn.Do("HSET","CPWM:sku:"+v,"Products Notes","")
		}
	*/

	// append DB values to sku HASH
	for _, sku := range skus {
		updateSkuHash(sku, redisConn)
	}

	fmt.Println("===============================================================")
	fmt.Println("LineList missing SKUs")
	fmt.Println("===============================================================")
	files, _ := ioutil.ReadDir(lineListDir)
	for _, f := range files {
		if f.Name() != ".DS_Store" {
			oof := strings.Split(f.Name(), "_")
			oof = strings.Split(oof[len(oof)-1], ".")
			redisConn.Do("HSET", "linelists", oof[0], f.Name())
		}
	}
	unsortedFiles, _ := Map(redisConn.Do("HGETALL", "linelists"))
	keys := make([]string, 0, len(unsortedFiles))
	for k := range unsortedFiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i := range keys {
		//fmt.Println(unsortedFiles[keys[i]])
		processLinelist(lineListDir+unsortedFiles[keys[i]], redisConn)
	}

	fileRecap, err := Map(redisConn.Do("HGETALL", "LLRecap"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("\n")
	fmt.Println("===============================================================")
	fmt.Println("LineList record count recap")
	fmt.Println("===============================================================")
	for k, v := range fileRecap {
		fmt.Println(k+":", v, "records")
	}
	fmt.Println("===============================================================", "\n")

	// Process DTC and WH
	processDTC(dtcFile, redisConn)
	processWarehouse(warehouseFile, redisConn)

	// compare hashes
	fmt.Println("===============================================================")
	fmt.Println("RealTIME DB vs CPWM data comparison")
	fmt.Println("===============================================================")
	compareHashes(redisConn)
	fmt.Println("===============================================================", "\n")

	purgeFeeds()
}

func getProducts(dbConn *sql.DB, redisConn redis.Conn) {

	rows, err := dbConn.Query("SELECT id,upc,title,description,note FROM tt_products WHERE account_id = 4")
	if err != nil {
		fmt.Println("ERROR:", "Cannot connect SELECT products;", err)
		os.Exit(1)
	}

	// Get the columns
	cols, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			fmt.Println("Failed to scan row", err)
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		// Create the redis SET with upc's and the sku HASH
		redisConn.Do("SADD", "skus", result[1])
		redisConn.Do("HMSET", "sku:"+result[1], "id", result[0], "sku", result[1], "Product Title", result[2], "Web Item Description", result[3], "Product Notes", result[4])
	}
}

func getMetaFields(dbConn *sql.DB, redisConn redis.Conn) {

	rows, err := dbConn.Query("SELECT id,field_name FROM tt_meta_fields")
	if err != nil {
		fmt.Println("ERROR:", "Cannot connect SELECT meta fields")
	}

	// Get the columns
	cols, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			fmt.Println("Fnailed to scan row", err)
		}
		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		redis.Strings(redisConn.Do("HSET", "metaFields", result[1], result[0]))
	}
}

func Map(do_result interface{}, err error) (map[string]string, error) {
	result := make(map[string]string, 0)
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

func updateSkuHash(sku string, redisConn redis.Conn) {
	metaData, _ := Map(redisConn.Do("HGETALL", "metaData:"+getProductID(sku, redisConn)))
	for k, v := range metaData {
		redis.Strings(redisConn.Do("HSET", "sku:"+sku, k, v))
	}
}

func getProductID(sku string, redisConn redis.Conn) string {
	productID, _ := Map(redisConn.Do("HGETALL", "sku:"+sku))
	return productID["id"]
}

func getMetaData(dbConn *sql.DB, redisConn redis.Conn) {

	caralho, _ := dbConn.Query("SELECT object_id,field_name,value FROM tt_meta_field_datas JOIN tt_meta_fields ON tt_meta_fields.id = tt_meta_field_datas.field_id")

	// Get the columns
	cols, err := caralho.Columns()
	if err != nil {
		fmt.Println(err)
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for caralho.Next() {
		err = caralho.Scan(dest...)
		if err != nil {
			fmt.Println("Failed to scan row", err)
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		redisConn.Do("HSET", "metaData:"+result[0], result[1], result[2])
	}
}

func processDTC(file string, redisConn redis.Conn) {
	dataFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err)
	}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", file, err)
		}
		redisConn.Do("HMSET", "CPWM:sku:"+record[0], "Web Item Description", record[1], "Country of Origin", record[3], "Selling Qty", record[4], "Ecom Style #", record[2], "Pad Icon", record[5], "Web Live Date", record[6], "Product Title", record[7])
	}
}

func processLinelist(file string, redisConn redis.Conn) {
	var count int
	dataFile, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", file, err)
			return
		}
		caralho, _ := redis.Int(redisConn.Do("SISMEMBER", "skus", record[0]))
		if caralho == 0 && record[0] != "SKU" {
			fmt.Println(record[0], "DOES NOT EXIST IN RealTIME")
		} else {
			redisConn.Do("HMSET", "CPWM:sku:"+record[0], "Category", record[2], "Sub Category", record[3], "Item Description", record[4], "Web Item Description", record[4], "Drop?", record[5], "Default Gateway", record[6], "Default Directory 1", record[7], "Default Directory 2", record[8], "Default Directory 3", record[9], "LY SKU", record[10], "Dropdown?", record[11], "SKUs In Dropdown", record[12], "Dropdown Variant", record[13], "Family?", record[14], "SKUs In Family", record[15], "Kit?", record[16], "SKUs In Kit", record[17], "Master SKU", record[18], "Photo Notes", record[19], "BackStory Request", record[20], "Web Live Date", record[21], "Product Notes", record[22], "Product Type", record[23])
		}
		count++
	}
	redisConn.Do("HSET", "LLRecap", file, count)
}

func processWarehouse(file string, redisConn redis.Conn) {
	dataFile, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", file, err)
		}
		redisConn.Do("HMSET", "CPWM:sku:"+record[2], "Receipt Date", record[0], "Item Description", record[3], "Active Location", record[4], "Active Units", record[5], "Reserve Units", record[6], "Carton Units", record[7], "SDC ECom Units", record[8], "Receipt Date", record[9], "Active Lock Code", record[10])
	}
}

func compareHashes(redisConn redis.Conn) {

	// get all skus we have in RT db
	skus, _ := Map(redisConn.Do("SMEMBERS", "skus"))
	metaFields, _ := Map(redisConn.Do("HGETALL", "metaFields"))

	for _, sku := range skus {
		rtData, _ := Map(redisConn.Do("HGETALL", "sku:"+sku))
		cpwmData, _ := Map(redisConn.Do("HGETALL", "CPWM:sku:"+sku))
		fmt.Println(sku + ":")
		for k, _ := range metaFields {
			if rtData[k] != cpwmData[k] {
				fmt.Println("- ["+k+"]", rtData[k], "<>", cpwmData[k])
			}
		}
		fmt.Println(" ")
	}
}

func readFile(file string) map[string][]string {
	m := make(map[string][]string)

	dataFile, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer dataFile.Close()

	// READ THE CSV FILE
	reader := csv.NewReader(dataFile)
	reader.Comma = ','

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("CSV ITERATE ERROR:", file, err)
		}
		m[record[0]] = record
	}
	return m
}

func purgeFeeds() {
	os.RemoveAll("feeds")
}

func downloadFeeds(client *s3.S3) (string, string, string) {

	// Checking if feed directories exits
	feedsFolder, err := os.Stat("feeds")
	if err != nil {
		fmt.Println("Creating Feed Directories")
		os.MkdirAll("feeds/linelists", 777)
		os.MkdirAll("feeds/dtc", 777)
		os.MkdirAll("feeds/warehouse", 777)
	} else {
		if !feedsFolder.IsDir() {
			panic("feeds is not a directory")
			os.Exit(1)
		}
	}

	bucket := client.Bucket("realtimeprocess-app")
	feeds, err := bucket.List("feeds/cpwm/", "", "", 10000)

	if err != nil {
		fmt.Println("Error getting objects from bucket", err)
	}

	llFeeds := make([]string, 0)  // Holds a list of all LineList Feeds
	dtcFeeds := make([]string, 0) // Holds a list of all DTC Feeds
	dcaFeeds := make([]string, 0) // Holds a list of all Warehouse Feeds

	for _, element := range feeds.Contents {
		if strings.Contains(element.Key, "feeds/cpwm/LL_") {
			llFeeds = append(llFeeds, element.Key)
		}
		if strings.Contains(element.Key, "feeds/cpwm/DTC_") {
			dtcFeeds = append(dtcFeeds, element.Key)
		}
		if strings.Contains(element.Key, "feeds/cpwm/DCAvl_") {
			dcaFeeds = append(dcaFeeds, element.Key)
		}
	}

	// Sort DTC and Warehouse feeds so that we can only download the latest.
	sort.Strings(dtcFeeds)
	sort.Strings(dcaFeeds)

	//Create a channel to send feed asynchronously
	send_chan := make(chan string, 0)

	//Create workers to handle processing channel
	for i := 0; i < 200; i++ {
		go work(send_chan, bucket)
	}

	//Get All Linelists
	for _, linelist := range llFeeds {
		send_chan <- linelist
	}

	// Busy wait for all feeds to download
	for ops < uint64(len(llFeeds)) {
		time.Sleep(time.Millisecond * 100)
	}

	//Get Latest DTC
	latestDTC := dtcFeeds[len(dtcFeeds)-1]
	dtcFileName := strings.Replace(latestDTC, "feeds/cpwm/", "feeds/dtc/", -1)
	data, err := bucket.Get(latestDTC)
	if err != nil {
		fmt.Println("Error getting file: ", latestDTC)
	} else {
		fmt.Println("Downloaded ", latestDTC)
		err := ioutil.WriteFile(dtcFileName, data, 0777)
		if err != nil {
			fmt.Println("Error saving file: ")
		}
	}

	//Get Latest Warehouse
	latestDCA := dcaFeeds[len(dcaFeeds)-1]
	dcaFileName := strings.Replace(latestDCA, "feeds/cpwm/", "feeds/warehouse/", -1)
	data, err = bucket.Get(latestDCA)
	if err != nil {
		fmt.Println("Error getting file: ", latestDCA)
	} else {
		fmt.Println("Downloaded ", latestDCA)
		err := ioutil.WriteFile(dcaFileName, data, 0777)
		if err != nil {
			fmt.Println("Error saving file: ")
		}
	}

	fmt.Println("===============================================================")
	fmt.Println("All Feeds Downloaded")
	fmt.Println("===============================================================")
	return "feeds/linelist/", dtcFileName, dcaFileName

}

func work(r_chan chan string, bucket *s3.Bucket) {
	for {
		linelist := <-r_chan // read from channel
		data, err := bucket.Get(linelist)
		if err != nil {
			fmt.Println("Error getting file: ", linelist)
			go func() {
				fmt.Println("RETRYING", linelist)
				r_chan <- linelist //Reput file into channel for reprocessing
			}()
		} else {
			fileName := strings.Replace(linelist, "feeds/cpwm/", "feeds/linelists/", -1)
			fmt.Println("Downloaded ", ops, linelist)
			err := ioutil.WriteFile(fileName, data, 0777)
			if err != nil {
				fmt.Println("Error saving file: ", linelist)
				fmt.Println("RETRYING", linelist)
				r_chan <- linelist //Reput file into channel for reprocessing
			} else {
				atomic.AddUint64(&ops, 1)
			}
		}
	}
}
