package main

import (
	"context"
	"database/sql"
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
	token            = "kTsGkTyfmxP2B9GzL70WekNlumJPNXZfNMaV6CDEAgkpeX9SftxbnBbQXgnEnDH0"
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

type User struct {
	UserID string `json:"user_id"`
	Status string `json:"status"`
}

func main() {
	start := time.Now()

	fmt.Println(start)

	// fmt.Printf("%s\n", resp)

	// insert()
	ctx := context.Background()
	db := openDbConnection()
	// log.Println(getBorrowerProfile("5c219643-454e-469f-b06b-d68e24d9545e"))
	jobs := make(chan []interface{}, 1)
	wg := new(sync.WaitGroup)

	mongo, err := connectMongo()
	if err != nil {
		log.Fatal(err)
	}

	go dispatchWorkers(mongo, jobs, wg)
	readData(ctx, db, jobs, wg)

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

func openDbConnection() *sql.DB {
	log.Println("=> open db connection MySQL")
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3307)/finanku")
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db
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
				defer wg.Done()
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

			// if err := json.Unmarshal(values, &doc); err != nil {
			// 	log.Fatal(err)
			// }
			// log.Println(values)
			_, err := db.Collection("borrower").InsertOne(ctx, values)
			if err != nil {
				panic(err)
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

func readData(ctx context.Context, db *sql.DB, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	defer db.Close()

	query := "SELECT user_id, status FROM users LIMIT 10"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err = rows.Scan(&user.UserID, &user.Status)
		if err != nil {
			panic(err)
		}

		users = append(users, user)
	}

	rowOrdered := make([]interface{}, 0)
	for _, each := range users {
		rowOrdered = append(rowOrdered, each)
	}

	wg.Add(1)
	jobs <- rowOrdered

	close(jobs)
}
