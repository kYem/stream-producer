package main

import (
	"fmt"
	"gopkg.in/redis.v5"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

// Message gets exchanged between users through redis pub/sub messaging
// Users may have websocket connections to different nodes and stored in
// different instances of this application
type Message struct {
	DeliveryID string `json:"id"`
	Content    string `json:"content"`
}

const (
	liveMatchEndpoint      = "/live/stats"
	channelLiveMatchPrefix = "dota_live_match."
	publishRate            = 2
	channelRate            = 1
)

var apiHostname = "localhost:8008"

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	api := os.Getenv("DOTA_TV_API_HOSTNAME")
	if api != "" {
		apiHostname = api
	}

	fmt.Printf("Connecting to api %s\n", apiHostname)


	defer client.Close()
	var channels []string
	var err error

	ticker := time.NewTicker(channelRate * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				channels, err = client.PubSubChannels(channelLiveMatchPrefix + "*").Result()
				fmt.Println(channels)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	publishTicker := time.NewTicker(publishRate * time.Second)
	publicQuit := make(chan struct{})
	go func() {
		for {
			select {
			case <-publishTicker.C:
				for _, channelName := range channels {
					go publishMatchData(client, channelName)
				}
			case <-publicQuit:
				publishTicker.Stop()
				return
			}
		}
	}()

	select {}

}

func publishMatchData(client *redis.Client, channelName string) {
	s := strings.Split(channelName, ".")
	url := fmt.Sprintf("http://%s%s?server_steam_id=%s", apiHostname, liveMatchEndpoint, s[1])
	fmt.Printf("Getting Match data from %s", url)
	resp, err := http.Get(url)

	if err != nil {
		fmt.Println(err)
		return
	}

	if resp.Body == nil {
		fmt.Println("Please send a request body")
		return
	}

	defer resp.Body.Close()
	contents, bodyErr := ioutil.ReadAll(resp.Body)
	if bodyErr != nil {
		fmt.Println("Error on request body")
		return
	}

	cmd := client.Publish(channelName, string(contents))
	fmt.Printf("Channel %s, published to %d users", channelName, cmd.Val())
}
