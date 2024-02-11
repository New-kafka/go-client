package main

import (
	"bytes"
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
	data := map[string]string{"queue_name": key, "value": value}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "Failed to encode JSON"
	}

	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "HTTP request failed"
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(response.Body)

	if response.StatusCode == 200 {
		return "Message pushed successfully."
	} else {
		return "Failed to push message."
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
	serverURL := "87.247.170.145:8000/"
	client := NewQueueClient(serverURL)
	subscriptionFunc := func(key, value string) {
		fmt.Printf("Received message: key=%s, value=%s\n", key, value)
	}
	client.Subscribe(subscriptionFunc)

	select {}
}
