package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
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
	apiHostname            = "api.dota.kyem.media"
	liveMatchEndpoint      = "/live/stats"
	channelLiveMatchPrefix = "dota_live_match."
	publishRate            = 2
	channelRate            = 1
)

var hostname = apiHostname
var ctx = context.Background()

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	v := os.Getenv("DOTA_TV_API_HOSTNAME")
	if len(v) > 3 {
		hostname = v
	}
	fmt.Printf("Connecting to %s\n", hostname)

	defer func(client *redis.Client) {
		err := client.Close()
		if err != nil {
			fmt.Printf("Error in closing redis client %s", err.Error())
		}
	}(client)
	var channels []string
	var err error

	ticker := time.NewTicker(channelRate * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				channels, err = client.PubSubChannels(ctx, channelLiveMatchPrefix+"*").Result()
				if err != nil {
					fmt.Printf("Error in receiving PubSubChannels: %s", err.Error())
				}
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
	url := fmt.Sprintf("http://%s%s?server_steam_id=%s", hostname, liveMatchEndpoint, s[1])
	resp, err := http.Get(url)

	if err != nil {
		fmt.Println(err)
		return
	}

	if resp.Body == nil {
		fmt.Println("Please send a request body")
		return
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Failed to close body", err.Error())
		}
	}(resp.Body)

	contents, bodyErr := ioutil.ReadAll(resp.Body)
	if bodyErr != nil {
		fmt.Println("Error on request body")
		return
	}

	cmd := client.Publish(ctx, channelName, string(contents))
	fmt.Printf("Channel %s, published to %d users \n", channelName, cmd.Val())
}
