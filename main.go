package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-lambda-go/events"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
)

type Item struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Author      string `xml:"author"`
	Discount    string `xml:"discount"`
	Publisher   string `xml:"publisher"`
	PubDate     string `xml:"pubdate"`
	ISBN        string `xml:"isbn"`
	Description string `xml:"description"`
	Image       string `xml:"image"`
}

type Book struct {
	Title           string
	PurchaseURL     string
	ImageURL        string
	Author          string
	Price           float64
	Publisher       string
	PubDate         string
	ISBN            string
	IndexContent    string
	Introduction    string
	PublisherReview string
	MiddleCategory  string
	DetailCategory  string
	Search          string
}

type Channel struct {
	Items []Item `xml:"item"`
}

type Response struct {
	Channel Channel `xml:"channel"`
}

func convertDateString(inputDate string) (string, error) {
	// Parse the input date string
	dateObj, err := time.Parse("20060102", inputDate)
	if err != nil {
		return "", err
	}

	// Format the date object to the desired layout
	formattedDate := dateObj.Format("2006-01-02")

	return formattedDate, nil
}

func connectElasticSearch(CLOUD_ID, API_KEY string) (*elasticsearch.Client, error) {

	config := elasticsearch.Config{
		CloudID: CLOUD_ID,
		APIKey:  API_KEY,
	}

	es, err := elasticsearch.NewClient(config)
	if err != nil {
		fmt.Print(err)
		return nil, err
	}

	fmt.Print("엘라스틱 클라이언트 : ", es)

	// Elasticsearch 서버에 핑을 보내 연결을 테스트합니다.
	res, err := es.Ping()
	if err != nil {
		fmt.Println("Elasticsearch와 연결 중 오류 발생:", err)
		return nil, err
	}
	defer res.Body.Close()

	fmt.Println("Elasticsearch 클라이언트가 성공적으로 연결되었습니다.")

	return es, nil

}

func searchIndex(es *elasticsearch.Client, indexName, fieldName, value string) ([]map[string]interface{}, error) {
	var allHits []map[string]interface{}

	// Define initial pagination parameters
	size := 1000 // Number of documents to retrieve per page
	from := 0    // Starting index of the page

	for {
		// 페이지 네이션이 포함된 검색 쿼리 작성
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					fieldName: value,
				},
			},
			"size": size,
			"from": from,
		}

		// 쿼리를 JSON으로 변환합니다.
		queryJSON, err := json.Marshal(query)
		if err != nil {
			return nil, err
		}

		// 검색 요청을 수행합니다.
		res, err := es.Search(
			es.Search.WithContext(context.Background()),
			es.Search.WithIndex(indexName),
			es.Search.WithBody(bytes.NewReader(queryJSON)),
		)
		if err != nil {
			return nil, err
		}

		// 검색 응답을 디코딩합니다.
		var searchResponse map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
			fmt.Println("검색 응답 디코딩 중 오류 발생:", err)
			return nil, err
		}

		// 히트를 추출하고 후 저장
		hits := searchResponse["hits"].(map[string]interface{})["hits"].([]interface{})
		for _, hit := range hits {
			allHits = append(allHits, hit.(map[string]interface{})["_source"].(map[string]interface{}))
		}

		// Check if there are more results
		totalHits := int(searchResponse["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
		if len(allHits) >= totalHits {
			break // Break the loop if all results have been retrieved
		}

		// Update pagination parameters for the next page
		from += size
	}

	return allHits, nil
}

func naverCrawling(API_URL, ISBN, CLIENT_ID, CLIENT_SECRET string) (*Book, error) {
	apiURL := API_URL

	client := &http.Client{}

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		fmt.Println("request 생성 실패 에러:", err)
		return nil, err
	}

	q := req.URL.Query()
	q.Add("d_isbn", ISBN)
	req.URL.RawQuery = q.Encode()

	req.Header.Add("X-Naver-Client-Id", CLIENT_ID)
	req.Header.Add("X-Naver-Client-Secret", CLIENT_SECRET)

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("request 보내기 실패 에러 :", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("response body 읽기 실패 에러:", err)
		return nil, err
	}

	var response Response
	err = xml.Unmarshal(body, &response)
	if err != nil {
		fmt.Println("XML 해석 에러:", err)
		return nil, err
	}

	for _, item := range response.Channel.Items {
		doc, err := goquery.NewDocument(item.Link)
		if err != nil {
			fmt.Println("HTML로딩 에러 :", err)
			continue
		}

		itemInfoList := doc.Find("div.infoItem_data_text__bUgVI")
		if item.Author == "" {
			item.Author = "없음"
		}

		if item.Image == "" {
			item.Image = "없음"
		}

		introduction := item.Description
		if item.Description == "" {
			introduction = "없음"
		}

		publisherReview := "없음"
		indexContent := "없음"

		if introduction == "없음" {
			if itemInfoList.Length() == 2 {
				publisherReview = itemInfoList.Eq(0).Text()
				indexContent = itemInfoList.Eq(1).Text()
			} else if itemInfoList.Length() == 1 {
				indexContent = itemInfoList.Eq(0).Text()
			}
		} else {
			if itemInfoList.Length() == 3 {
				publisherReview = itemInfoList.Eq(1).Text()
				indexContent = itemInfoList.Eq(2).Text()
			} else if itemInfoList.Length() == 2 {
				indexContent = itemInfoList.Eq(1).Text()
			}
		}

		category := doc.Find("a.bookCatalogTop_category__LIOY2")

		middleCategory := "없음"
		detailCategory := "없음"

		if category.Length() > 1 {
			middleCategory = category.Eq(1).Text()
		}

		if category.Length() > 2 {
			detailCategory = category.Eq(2).Text()
		}

		// 가격을 정수형으로 변환합니다.
		discount, err := strconv.ParseFloat(item.Discount, 64)
		if err != nil {
			fmt.Println("가격를 정수로 변환하는 동안 오류 발생:", err)
			return nil, err
		}

		return &Book{
			Title:           item.Title,
			PurchaseURL:     item.Link,
			ImageURL:        item.Image,
			Author:          item.Author,
			Price:           discount,
			Publisher:       item.Publisher,
			PubDate:         item.PubDate,
			ISBN:            item.ISBN,
			IndexContent:    indexContent,
			Introduction:    introduction,
			PublisherReview: publisherReview,
			MiddleCategory:  middleCategory,
			DetailCategory:  detailCategory,
			Search:          "없음",
		}, nil

	}

	// 아이템이 없는 경우를 처리합니다.
	return nil, errors.New("아이템을 찾을 수 없습니다")
}

func refineData(data *Book) *Book {
	// If 목차가 없음이면 data drop
	if data.IndexContent == "없음" {
		return nil
	}

	// string 값의 공백 문자 replace, "->\", '->\' 로 변경
	replacements := map[string]string{
		"'":  "^",
		"\"": "^",
		"\\": "           ",
	}

	title := data.Title
	// Perform replacements
	for old, new := range replacements {
		title = strings.ReplaceAll(title, old, new)
	}

	indexContent := data.IndexContent
	for old, new := range replacements {
		indexContent = strings.ReplaceAll(indexContent, old, new)
	}

	introduction := data.Introduction
	for old, new := range replacements {
		introduction = strings.ReplaceAll(introduction, old, new)
	}

	publisherReview := data.PublisherReview
	for old, new := range replacements {
		publisherReview = strings.ReplaceAll(publisherReview, old, new)
	}

	var search string

	if introduction == "없음" && publisherReview == "없음" {
		search = indexContent
	} else if introduction != "없음" && publisherReview == "없음" {
		search = indexContent + introduction
	} else if introduction == "없음" && publisherReview != "없음" {
		search = indexContent + publisherReview
	} else {
		search = indexContent + introduction
	}

	//출판일 yyyy-mm-dd 형식으로 변경
	formattedPubDate, err := convertDateString(data.PubDate)
	if err != nil {
		fmt.Println("데이트타입으로 바꾸는데 에러가 발생했습니다.", err)
	}

	docData := &Book{
		Title:           title,
		PurchaseURL:     strings.ReplaceAll(data.PurchaseURL, "\n", " "),
		ImageURL:        strings.ReplaceAll(data.ImageURL, "\n", " "),
		Author:          data.Author,
		Price:           data.Price,
		Publisher:       data.Publisher,
		PubDate:         formattedPubDate,
		ISBN:            data.ISBN,
		IndexContent:    indexContent,
		Introduction:    introduction,
		PublisherReview: publisherReview,
		MiddleCategory:  data.MiddleCategory,
		DetailCategory:  data.DetailCategory,
		Search:          search,
	}

	return docData
}

func handler(ctx context.Context, event events.DynamoDBEvent) error {
	//0. 환경변수
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	CLOUD_ID := os.Getenv("CLOUD_ID")
	API_KEY := os.Getenv("API_KEY")
	ISBN_INDEX_NAME := os.Getenv("ISBN_INDEX_NAME")
	BOOK_INDEX_NAME := os.Getenv("BOOK_INDEX_NAME")
	FIELD_NAME := os.Getenv("FIELD_NAME")
	CLIENT_ID := os.Getenv("CLIENT_ID")
	CLIENT_SECRET := os.Getenv("CLIENT_SECRET")
	API_URL := os.Getenv("API_URL")
	PIPE_LINE := os.Getenv("PIPE_LINE")

	for _, record := range event.Records {
		// 이벤트가 어떤 타입의 변화인지 검사
		if string(record.EventName) != string(events.DynamoDBOperationTypeInsert) {
			fmt.Printf("현재 이벤트의 타입 : %s\n", record.EventName)
		}

		//1. 들어오는 다이나모 DB 변화 이벤트로부터 크롤링 타임 추출
		newImage := record.Change.NewImage
		dateString := strings.Split(newImage["crawling_time"].String(), " ")[0]
		crawling_time_string := dateString[1:]
		crawling_time, err := convertDateString(crawling_time_string)
		if err != nil {
			fmt.Println("에러 parsing date:", err)
			fmt.Println("crawling_time을 go에서 시간 객체로 변환할 수 없습니다.")

		}

		//2. 엘라스틱 인덱스에서 1의 크롤링타임에 가져온 isbn 가져오기
		esClient, err := connectElasticSearch(CLOUD_ID, API_KEY)
		if err != nil {
			fmt.Println("Error connecting to Elasticsearch:", err)
			return nil
		}

		// 인덱스에서 값 검색합니다.
		res, err := searchIndex(esClient, ISBN_INDEX_NAME, FIELD_NAME, crawling_time)
		if err != nil {
			fmt.Println("인덱스 검색 중 오류 발생:", err)
			return nil
		}

		for _, hit := range res {
			isbnString := hit["isbn"].(string)

			fmt.Println("ISBN:", isbnString)

			//3. 각 isbn 값들 네이버 크롤링해오기
			data, err := naverCrawling(API_URL, isbnString, CLIENT_ID, CLIENT_SECRET)
			if err != nil {
				fmt.Println("네이버 크롤링 중 오류 발생:", err)
				continue
			}

			//4. 정제 및 벡터 필드 추가
			refinedData := refineData(data)
			if refinedData == nil {
				continue
			}

			//5. elastic cloud book index에 크롤링한 데이터 넣기. 이때 document id는 isbn이며 파이프라인 연결
			reqBody, err := json.Marshal(refinedData)
			if err != nil {
				log.Fatalf("Error marshaling document to JSON: %s", err)
			}

			req := map[string]interface{}{
				"index":    BOOK_INDEX_NAME,
				"id":       isbnString,
				"document": bytes.NewReader(reqBody),
				"pipeline": PIPE_LINE,
			}

			// Convert the request body to JSON bytes
			reqBytes, err := json.Marshal(req)
			if err != nil {
				// Handle error
			}

			// Perform the index request
			res, err := esClient.Index(
				BOOK_INDEX_NAME,                     // Index name
				strings.NewReader(string(reqBytes)), // Request body
			)

			fmt.Println(res)

		}

	}

	return nil

}

func main() {
	// //람다
	// lambda.Start(handler)

	//테스트~~~~~~~~~~~~~~
	// Read the test data file
	testData, err := os.Open("test-event.json")

	if err != nil {
		fmt.Printf("failed to read test data file: %v", err)
	}
	defer testData.Close()

	// Unmarshal the test data into the DynamoDBEvent type
	var testEvent events.DynamoDBEvent
	err = json.NewDecoder(testData).Decode(&testEvent)
	if err != nil {
		log.Fatalf("Error decoding test event JSON: %s", err)
	}

	// Create a mock context
	ctx := context.Background()

	// Call your Lambda function handler with the test event and context
	if err := handler(ctx, testEvent); err != nil {
		fmt.Printf("handler returned error: %v", err)
	}
}
