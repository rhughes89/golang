package main

import (
    "fmt"
    "io/ioutil"
    "encoding/json"
)


type JSONData struct {

    //This field will correspond to the specific named key in JSON,
    //as denoted by `json:[key name]`
    Key string `json:key`
    Greetings string `json:greetings`

    //Make sure your field name is uppercase if you want access to it,
    //Go considers fields that start a lowercase letter private
    //You can access the field however in struct methods however
}

func (q *JSONData) FromJSON(file string) error {

    //Reading JSON file
    J, err := ioutil.ReadFile(file)
    if err != nil { panic(err) }


    var data = &q
    //Umarshalling JSON into struct
    return json.Unmarshal(J, data)
}

func main() {

    //Setting up a struct where will place our data that we extract
    JSONStruct := &JSONData{}

    //Extracting the JSON data into a golang struct
    err := JSONStruct.FromJSON("config.json")

    //If there was an error extracting the JSON file,
    // (incorrect file type, file not found, etc), go cray!
    if err != nil { panic(err) }

    //Accsesing that field and printing it.
    fmt.Println(JSONStruct.Key)
    // >> value
    fmt.Println(JSONStruct.Greetings)
    // >> Hello World!
}
