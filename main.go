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
	apiHostname            = "api.dotatv.com"
	liveMatchEndpoint      = "/live/stats"
	channelLiveMatchPrefix = "dota_live_match."
	publishRate			   = 2
	channelRate			   = 1
)

var hostname = apiHostname

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
	})

	v := os.Getenv("DOTA_TV_API_HOSTNAME")
	if len(v) > 3 {
		hostname = v
	}

	defer client.Close()
	var channels []string
	var err error

	ticker := time.NewTicker(channelRate * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <- ticker.C:
				channels, err = client.PubSubChannels(channelLiveMatchPrefix + "*").Result()
				fmt.Println(channels)
			case <- quit:
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
			case <- publishTicker.C:
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
	url := fmt.Sprintf("http://%s%s?server_steam_id=%s", hostname, liveMatchEndpoint, s[1])
	fmt.Printf("Getting Match data from %s", url)
	resp, err := http.Get(url)

	if err != nil {
		panic(err)
	}

	if resp.Body == nil {
		fmt.Println("Please send a request body")
	}

	defer resp.Body.Close()
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error on request body")
	}

	cmd := client.Publish(channelName, string(contents))
	fmt.Printf("Channel %s, published to %d users", channelName, cmd.Val())
}

