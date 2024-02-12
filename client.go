package main

import (
	"bytes"
	"encoding/base64"
	_ "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type QueueClient struct {
	ServerURL string
}

func NewQueueClient(serverURL string) *QueueClient {
	return &QueueClient{ServerURL: serverURL}
}

func (qc *QueueClient) Push(key, value string) string {
	url := qc.ServerURL + "/push"
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	data := map[string]string{"key": key, "value": encodedValue}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Sprintf("Failed to encode JSON: %v", err)
	}

	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Sprintf("HTTP request failed: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Failed to close response body: %v", err)
		}
	}(response.Body)

	if response.StatusCode == 200 {
		return "Message pushed successfully."
	} else {
		body, _ := ioutil.ReadAll(response.Body)
		return fmt.Sprintf("Failed to push message. Status: %d, Response: %s", response.StatusCode, string(body))
	}
}

func (qc *QueueClient) Pull() (string, string, error) {
	url := qc.ServerURL + "/pop"
	response, err := http.Post(url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return "", "", err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(response.Body)

	if response.StatusCode == 200 {
		var result map[string]string
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return "", "", err
		}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return "", "", err
		}
		return result["key"], result["value"], nil
	} else {
		return "", "", fmt.Errorf("failed to pop message")
	}
}

func (qc *QueueClient) pullPeriodically(f func(string, string)) {
	for {
		key, value, err := qc.Pull()
		if err == nil && key != "" && value != "" {
			f(key, value)
		}
		time.Sleep(1 * time.Second)
	}
}

func (qc *QueueClient) Subscribe(f func(string, string)) {
	go qc.pullPeriodically(f)
}

func main() {
	serverURL := "http://87.247.170.145:8000"
	client := NewQueueClient(serverURL)

	// Push a message
	fmt.Println(client.Push("queue", "message"))

	for {
		key, value, err := client.Pull()
		if key == "" {
			fmt.Println("queue is empty!")
		} else {
			fmt.Printf("Received message: key=%s, value=%s\n", key, value)
		}
		if err != nil {
			fmt.Printf("Failed to pull message: %v", err)
		}
		time.Sleep(1 * time.Second)

	}
}
