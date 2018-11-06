package main

import (
	"github.com/aws/aws-lambda-go/events"
	"log"
	"encoding/json"
	"net/http"
	"fmt"
	"regexp"
	"strings"
	"html/template"
	"bytes"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"math/rand"
	"time"
	"sync/atomic"
	"golang.org/x/sync/syncmap"
	"errors"
	"strconv"
	"github.com/aws/aws-lambda-go/lambda"
)

var mainGen *generator

type response struct {
	statusCode int
	response string
	headerData string
	err error
}

func newResponse() response {
	res := response{
		http.StatusNoContent,
		"",
		"application.json",
		nil,
	}
	return res
}

func newDefaultResponse(stats int) response {
	res := response{
		stats,
		fmt.Sprintf("%d HTTP/1.1 1 1", stats),
		"text/html",
		nil,
	}
	return res
}

func newDetailedResponse(stats int, desc string) response {
	res := response {
		stats,
		fmt.Sprintf("%d HTTP/1.1 1 1 : %v", stats, desc),
		"text/html",
		nil,
	}
	return res
}

func init() {
	log.Println("!!!!!!___INIT___!!!!!!")
}
/**
* Request
* request.Path = Path ie) /words
* request.PathParameters = Path parameters ie) for link /words/{id} -> map[id:5} as map[string]string
* request.Body = text passed during a POST request
*
* RequestContext
* request.RequestContext.Stage = Prod vs Stage
* request.RequestContext.HTTPMethod = Http Method
* request.RequestContext.ResourcePath = Path ie) /words
*/

// Handler is executed by AWS Lambda in the main function. Once the request
// is processed, it returns an Amazon API Gateway response object to AWS Lambda
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Printf("Entry\n")

	log.Println(request.StageVariables)

	log.Printf("-->LOG for " + request.HTTPMethod + ":\n->Headers: %v\n->Body: %v\n->Path: %v, PathParams: %v\n", request.Headers, request.Body,  request.Path, request.PathParameters)

	resp := newResponse()

	if stat, err := regexp.MatchString("^/words(/?)*", request.Path); stat && err == nil {
		log.Println("~~~>Words")
		resp = routeWords(&request)

	} else if stat, err := regexp.MatchString("^/api(/?)*", request.Path); stat && err == nil {
		log.Println("~~~>Api")
		resp = routeAPI(&request)

	} else if stat, err := regexp.MatchString("^/gen(/?)*", request.Path); stat && err == nil {
		log.Println("~~~>Gen")
		resp = routeGenerator(&request)

	} else if stat, err := regexp.MatchString("^/charts(/?)*", request.Path); stat && err == nil {
		log.Println("~~~>Charts")
		resp = defaultRoute("public/charts.html")
	} else if stat, err := regexp.MatchString("^/tables(/?)*", request.Path); stat && err == nil {
		log.Println("~~~>Tables")
		resp = defaultRoute("public/tables.html")
	} else{
		log.Println("~~~>Default")
		resp = defaultRoute("public/index.gohtml")
	}

	return events.APIGatewayProxyResponse{
		StatusCode: resp.statusCode,
		Body:       resp.response,
		Headers: map[string]string{
			"Content-Type": resp.headerData,
		},
	}, resp.err
}

func defaultRoute(fileName string) response {
	tpl, err := template.ParseFiles(fileName)
	if err != nil {
		log.Println("->ERROR: " + err.Error())
		r := newDetailedResponse(http.StatusInternalServerError, "Default template error")
		r.err = err
		return r
	}

	var ind bytes.Buffer
	tpl.Execute(&ind, fileName)
	resp := response{http.StatusOK,ind.String(), "text/html", nil}
	return resp
}

func routeWords(request *events.APIGatewayProxyRequest) response {

	resp := newResponse()

	if stat, err := regexp.MatchString("^/words(/?)$",request.Path); stat && err == nil {

		if request.HTTPMethod == "GET" {
			log.Println("->Query All Words")
			js, _ := json.Marshal(queryAllFromDynamo())

			// TODO return all words json on webpage

			resp.response = string(js)
			resp.statusCode = http.StatusOK

		} else if request.HTTPMethod == "POST" {
			log.Println("->Post Word")
			body := strings.Split(request.Body, "=")

			// TODO return post status on webpage

			resp = newDefaultResponse(postToDynamo(body[len(body)-1],-1))

		} else {
			resp = newDefaultResponse(http.StatusMethodNotAllowed)
		}

	} else if stat, err := regexp.MatchString("^/words/[0-9]+(/?)$", request.Path); stat && err == nil {

		if request.HTTPMethod == "GET" {
			log.Println("->Query Word #")
			id := request.PathParameters["id"]

			if wrd, err := queryFromDynamo(id); err == nil {
				if xb, err := json.Marshal(map[string]string{id:wrd}); err == nil {

					// TODO return word json on webpage

					resp.response = string(xb)
					resp.statusCode = http.StatusOK

				} else {
					resp = newDetailedResponse(http.StatusInternalServerError, "Error Marshalling JSON")
				}
			} else {
				log.Println(err)
				resp = newDetailedResponse(http.StatusNotFound, "Error Querying Word")
			}
		} else {
			resp = newDefaultResponse(http.StatusMethodNotAllowed)
		}
	} else {
		resp = newDefaultResponse(http.StatusNotFound)
	}

	return resp
}

func routeGenerator(request *events.APIGatewayProxyRequest) response {

	resp := newResponse()

	if stat, err := regexp.MatchString("^/gen(/?)$", request.Path); stat && err == nil {
		if request.HTTPMethod != "GET" {
			return newDefaultResponse(http.StatusMethodNotAllowed)
		}

		if nil != mainGen && mainGen.running {
			log.Println("Tried to start generator that is ALREADY running!")
			return  newDetailedResponse(http.StatusBadRequest, "Tried to start generator that is ALREADY running!")
		}
		mainGen = newGenerator(2,"cat")
		mainGen.run()
		mainGen.running = false
		js, _ := json.Marshal(queryAllFromDynamo())

		resp = response{http.StatusOK,fmt.Sprintf("%v", string(js)),"application/json",nil}
	} else {
		return defaultRoute("public/index.gohtml")
	}

	return resp
}

func routeAPI(request *events.APIGatewayProxyRequest) response {

	resp := newResponse()

	if stat, err := regexp.MatchString("^/api(/?)$",request.Path); stat && err == nil {

		if request.HTTPMethod == "GET" {
			log.Println("->Query Api Help")

			tpl, err := template.ParseFiles("public/apihelp.gohtml")

			if err != nil {
				log.Println("->ERROR: " + err.Error())

				resp := newDefaultResponse(http.StatusInternalServerError)
				resp.err = err

				return resp
			}

			var ind bytes.Buffer
			tpl.Execute(&ind,"public/apihelp.gohtml")
			resp = response{http.StatusOK,ind.String(), "text/html", nil}

		} else {
			resp = newDefaultResponse(http.StatusMethodNotAllowed)
		}

	} else if stat, err := regexp.MatchString("^/api/words(/?)$", request.Path); stat && err == nil {

		if request.HTTPMethod == "GET" {
			log.Println("->API: Query All Words")
			js, _ := json.Marshal(queryAllFromDynamo())

			resp.response = string(js)
			resp.statusCode = http.StatusOK

		} else if request.HTTPMethod == "POST" {
			log.Println("->API: Post Word")
			body := strings.Split(request.Body, "=")

			resp = newDefaultResponse(postToDynamo(body[len(body)-1],-1))

		} else {
			resp = newDefaultResponse(http.StatusMethodNotAllowed)
		}
	} else if stat, err := regexp.MatchString("^/api/words/[0-9]+(/?)$", request.Path); stat && err == nil {

		if request.HTTPMethod == "GET" {
			id := request.PathParameters["id"]
			log.Println("-> API: Query Word " + id)

			if wrd, err := queryFromDynamo(id); err == nil {
				if xb, err := json.Marshal(map[string]string{id:wrd}); err == nil {
					resp.response = string(xb)
					resp.statusCode = http.StatusOK
				} else {
					resp = newDetailedResponse(http.StatusInternalServerError, "Error Marshalling JSON")
				}
			} else {
				log.Println(err)
				resp = newDetailedResponse(http.StatusNotFound, "Error Querying Word")
			}
		} else {
			resp = newDefaultResponse(http.StatusMethodNotAllowed)
		}
	} else {
		resp = newDefaultResponse(http.StatusNotFound)
	}

	return resp
}

func queryFromDynamo(id string) (string, error) {
	table := "bip-poc-stream-test"

	yamlFile, err := ioutil.ReadFile("const.yml")

	if err != nil {
		return "Error Reading Key to query", err
	}

	x := make(map[string]int,1)
	yaml.Unmarshal(yamlFile,&x)

	if i, _ := strconv.Atoi(id); i >= x["Key"] {
		return "", errors.New("invalid key")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		log.Println(err)
		return "", err
	}

	client := dynamodb.New(sess)

	result, err := client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				N: aws.String(id),
			},
		},
	})

	if err != nil {
		log.Println(err)
		return "", err
	}

	item := map[string]string{}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)

	if err != nil {
		log.Println(err)
		return "", err
	}

	return item["Value"], nil
}

func queryAllFromDynamo() map[string]string {
	table := "bip-poc-stream-test"

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		log.Println(err)
	}

	client := dynamodb.New(sess)

	out, err := client.Scan(&dynamodb.ScanInput{
		TableName:aws.String(table),
	})

	if err != nil {
		log.Println(err)
	}

	res := map[string]string{}

	for i := range out.Items {
		mp := out.Items[i]
		res[string(*mp["Key"].N)] = string(*mp["Value"].S)
	}

	return res
}

func postToDynamo(word string, fromGen int) int {
	table := "bip-poc-stream-test"

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		log.Println(err)
		return http.StatusNotFound
	}

	client := dynamodb.New(sess)

	var k int
	if fromGen == -1 {
		k = getNextKey()
	} else {
		k = fromGen
	}

	data := map[string]interface{}{"Key":k,"Value":word}

	item, err := dynamodbattribute.MarshalMap(data)

	if err != nil {
		log.Println(err)
		return http.StatusBadRequest
	}

	input := &dynamodb.PutItemInput{
		Item: item,
		TableName: aws.String(table),
	}

	_, err = client.PutItem(input)

	if err != nil {
		log.Println(err.Error())
		return http.StatusInternalServerError
	}

	return http.StatusOK
}


func getNextKey() int {

	yamlFile, err := ioutil.ReadFile("const.yml")

	if err != nil {
		panic(fmt.Sprintf("error reading key for post"))
	}

	x := make(map[string]int,1)
	err = yaml.Unmarshal(yamlFile,&x)

	key := x["Key"]

	data, _ := yaml.Marshal(map[string]int{"Key":key+1})
	ioutil.WriteFile("const.yml", data, 02)

	return key
}

func main() {
	lambda.Start(Handler)
	//mainGen = newGenerator(2,"cat")
	//mainGen.run()
	//mainGen.running = false
	//js, _ := json.Marshal(queryAllFromDynamo())
	//fmt.Println(string(js))
}

type generator struct {
	threads int
	keyWord string
	stringMap syncmap.Map
	running bool
	done chan bool
}

func newGenerator(threads int, word string) *generator {
	gen := generator{
		threads,
		word,
		syncmap.Map{},
		false,
		make(chan bool),
	}
	rand.Seed(time.Now().UTC().UnixNano())
	return &gen
}

func (gen *generator) generate(start int64, key *int64) <-chan string {

	out := make(chan string)

	go func(inKey *int64) {
		for *key < start+100 {

			letters := "abcdefghijklmnopqrstuvwxyz"

			length := rand.Intn(24) + 3

			wordBuilder := make([]byte, length)

			for i := 0; i < length; i++ {
				index := rand.Intn(len(letters))
				ranLetter := letters[index]
				wordBuilder[i] = ranLetter
				letters = strings.Replace(letters, string(ranLetter), "", 1)
			}

			word := string(wordBuilder)

			if gen.validWord(word) {
				out <- word
			}
		}
		close(out)
	}(key)
	return out
}

func (gen *generator) validWord(word string) bool {

	if strings.Contains(word, gen.keyWord) {
		if _, ok := gen.stringMap.Load(word); !ok {
			return true
		}
	}
	return false
}

func (gen *generator) run() {

	gen.running = true

	wordChan := make([]<-chan string, gen.threads)

	genKey := int64(getNextKey())
	log.Printf("NEXT GET KEY: %v\n",genKey)

	for i := 0; i < gen.threads; i++ {
		wordChan[i] = gen.generate(genKey, &genKey)
	}

	log.Println("GEN RUNNING")

	for i := 0;  i < gen.threads; i++ {
		c := wordChan[i]
		go func(in <-chan string) {
			for word := range in {
				k := atomic.LoadInt64(&genKey)
				gen.stringMap.Store(word, k)
				atomic.AddInt64(&genKey, 1)
				postToDynamo(word, int(k))
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
			gen.done <- true
		}(c)
	}
	for i := 0; i < gen.threads; i++ {
		<- gen.done
	}

	dt, _ := yaml.Marshal(map[string]int64{"Key":genKey})
	ioutil.WriteFile("const.yml", dt, 02)
	log.Printf("Generator finished with key: %v",genKey)
}

//Golang -> Java API
//func queryWord(id string) (string, error) {
//	log.Println("***Query for ID: " + id)
//	table := "bip-poc-stream-test"
//
//	maxCap, _ := strconv.Atoi(getNextKey())
//	i, _ := strconv.Atoi(id)
//
//	if i >= maxCap {
//		log.Println("Error Generating Key")
//		log.Printf("%d vs %d", i, maxCap)
//		return string(http.StatusNotFound), fmt.Errorf("word for key not possible")
//	}
//
//	client := &http.Client{}
//
//	var br io.Reader
//	req, err := http.NewRequest(http.MethodGet, "http://35.173.230.220/api/getData?Table%20name="+table+"&Primary%20Key=Key&Primary%20Key%20Value=" + id + "&Primary%20Key%20Type=int&Strongly%20Consistent=false", br)
//
//	if err != nil {
//		log.Println("Error Generating Request")
//		return fmt.Sprintf("Error generating request: %d",http.StatusInternalServerError), err
//	}
//
//	req.Header.Add("Accept","text/plain")
//	req.Header.Add("Content-type","application/json")
//	req.Header.Add("Authorization", "Basic azd4aFcwejQva1JSOGt5QTBZZEw0Z3daNFFBNm00bGc6ZytmSEcwdHBpWjRoNU9rVHF4dERCYk1QR3U1bkhHQi85aU9SOXI5eDF0ODRybmViQis0UGZLZlZ4TlNwanN0aA==")
//
//	log.Println("Checkpoint 1: Connecting....")
//
//	resp, err := client.Do(req)
//
//	log.Println("Checkpoint 2: Connected!!!")
//
//	if err != nil {
//		log.Println("Error Retrieving Response")
//		return fmt.Sprintf("Error retrieving response: %d",http.StatusInternalServerError), err
//	}
//	if length := resp.ContentLength; length == -1 {
//		log.Println("Error: Word for key not found")
//		return string(http.StatusNotFound), fmt.Errorf("word for key not found")
//	}
//	xb := make([]byte,resp.ContentLength)
//
//	resp.Body.Read(xb)
//	resp.Body.Close()
//
//	return string(xb), nil
//}

//func postWord(word string) string {
//	log.Println("***Post for Word: " + word)
//	table := "bip-poc-stream-test"
//
//	key := getNextKey()
//
//	client := &http.Client{}
//
//	post := "{\"tableName\": \""+table+"\",\"primaryKey\": {\"name\":\"Key\",\"type\": \"int\"},\"items\": [{\"primaryKeyVal\": \""+key+"\",\"columns\":[{\"name\": \"Value\",\"type\": \"string\",\"value\": \""+word+"\"}]}]}"
//
//	req, err := http.NewRequest(http.MethodPost, "http://35.173.230.220/api/insertData", bytes.NewBuffer([]byte(post)))
//
//	if err != nil {
//		log.Println("ERROR generating request")
//		return fmt.Sprintf("Error generating request: %d",http.StatusInternalServerError)
//	}
//
//	req.Header.Add("Accept","*/*")
//	req.Header.Add("Content-type","application/json")
//	req.Header.Add("Authorization", "Basic azd4aFcwejQva1JSOGt5QTBZZEw0Z3daNFFBNm00bGc6ZytmSEcwdHBpWjRoNU9rVHF4dERCYk1QR3U1bkhHQi85aU9SOXI5eDF0ODRybmViQis0UGZLZlZ4TlNwanN0aA==")
//
//	log.Println("Checkpoint 1: Connecting....")
//
//	resp, err := client.Do(req)
//
//	log.Println("Checkpoint 2: Connected!!!")
//
//	log.Println(resp)
//	resp.Body.Close()
//
//	if err != nil {
//		log.Println("ERROR retrieving response")
//		return fmt.Sprintf("Error retrieving response: %d",http.StatusInternalServerError)
//	}
//
//
//	k, _ := strconv.Atoi(key)
//
//	data, _ := yaml.Marshal(map[string]string{"Key":strconv.Itoa(k+1)})
//
//
//	ioutil.WriteFile("const.yml", data, 02)
//
//	log.Println("Returning Post Status")
//
//	return string(resp.StatusCode)
//}