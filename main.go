package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	baseAPIURL       = "https://api.finanku.com:3000/v1/api"
	token            = "ZwMdi7iMoMJimLoM6sFaR1Ed2mzHkkAR7c9dB1AINS8LGS35pySwKo1N2NQNbC0J"
	defaultUserAgent = ""
)

var defaultHeader = http.Header{
	"User-Agent":    {defaultUserAgent},
	"Accept":        {"application/json"},
	"Authorization": {token},
	"Connection":    {"keep-alive"},
}

var ctx = context.Background()

var doc interface{}

type Phones struct {
	UserID string `json:"user_id"`
}

func main() {
	start := time.Now()

	fmt.Println(start)

	// fmt.Printf("%s\n", resp)

	// insert()

	db, err := connectSql()
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		fmt.Println(err)
	}

	saveBorrower()
}

func connectMongo() (*mongo.Database, error) {
	dbName := "tutorial1"
	clientOptions := options.Client()
	clientOptions.ApplyURI("mongodb://localhost:27017")
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)

	if err != nil {
		return nil, err
	}

	return client.Database(dbName), nil
}

func connectSql() (*sql.DB, error) {
	db, err := sql.Open("mysql", "root:inipassword@tcp(127.0.0.1:3306)/finanku")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func saveBorrower() {
	// ambil data user_id dari sql
	// select user_id from user where is_saved = 'false'
	db, err := connectSql()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer db.Close()

	results, err := db.Query("select * from user")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for results.Next() {
		var phone Phones

		err = results.Scan(&phone.UserID)
		if err != nil {
			panic(err.Error())
		}

		resp, err := getBorrowerProfile(phone.UserID)
		if err != nil {
			log.Fatalln(err)
		}

		db, err := connectMongo()
		if err != nil {
			log.Fatal(err)
		}

		if err := json.Unmarshal(resp, &doc); err != nil {
			log.Fatal(err)
		}

		_, err = db.Collection("borrower").InsertOne(ctx, doc)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("success")
	}

	defer results.Close()
}

func getBorrowerProfile(id string) ([]byte, error) {
	// iterate api borrower profile sesuai dengan user_id get phones dan simpan data ke database
	url, err := url.Parse(baseAPIURL + "/profile/" + id + "/borrower-profile")

	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header = defaultHeader

	cl := &http.Client{}

	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}
