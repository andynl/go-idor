package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	baseAPIURL       = "https://api.finanku.com:3000/v1/api"
	token            = "ZwMdi7iMoMJimLoM6sFaR1Ed2mzHkkAR7c9dB1AINS8LGS35pySwKo1N2NQNbC0J"
	defaultUserAgent = ""
	dbMaxIdleConns   = 4
	dbMaxConns       = 100
	totalWorker      = 100
)

var defaultHeader = http.Header{
	"User-Agent":    {defaultUserAgent},
	"Accept":        {"application/json"},
	"Authorization": {token},
	"Connection":    {"keep-alive"},
}

var ctx = context.Background()

var doc interface{}

var dataHeaders = make([]string, 0)

type Phones struct {
	UserID string `json:"user_id"`
}

func main() {
	start := time.Now()

	fmt.Println(start)

	// fmt.Printf("%s\n", resp)

	// insert()

	db, err := openDbConnectionMysql()
	if err != nil {
		log.Fatal(err)
	}

	mongo, err := connectMongo()
	if err != nil {
		log.Fatal(err)
	}

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(mongo, jobs, wg)
	readDataPhonesFromMysql(db, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")

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

func openDbConnectionMysql() (*sql.DB, error) {
	log.Println("=> open db connection MySQL")
	db, err := sql.Open("mysql", "root:inipassword@tcp(127.0.0.1:3306)/finanku")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func readDataPhonesFromMysql(db *sql.DB, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		results, err := db.Query("select * from user")
		if err != nil {
			fmt.Println(err.Error())
			return
		}
  k 7
		for results.Next() {
			var phone Phones

			err = results.Scan(&phone.UserID)
			if err != nil {
				break
			}

			if len(dataHeaders) == 0 {
				dataHeaders = results
				continue
			}

			rowOrdered := make([]interface{}, 0)
			for _, each := range results {
				rowOrdered = append(rowOrdered, each)
			}

			wg.Add(1)
			jobs <- rowOrdered
		}
		defer results.Close()
		defer close(jobs)
	}
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

func dispatchWorkers(db *mongo.Database, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *mongo.Database, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func doTheJob(workerIndex, counter int, db *mongo.Database, values []interface{}) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			// resp, err := getBorrowerProfile(values)

			db, err := connectMongo()
			if err != nil {
				log.Fatal(err)
			}

			if err := json.Unmarshal(values, &doc); err != nil {
				log.Fatal(err)
			}

			_, err = db.Collection("borrower").InsertOne(ctx, doc)
			if err != nil {
				log.Fatal(err)
			}

		}(&outerError)
		if outerError == nil {
			break
		}
	}
	if counter%100 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
}
