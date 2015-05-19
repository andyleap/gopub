// gopub project gopub.go
package gopub

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type GoPub struct {
	subQueue    chan *subRequest
	DataStorage DataStorage
	subs        map[int64]*Subscription
	subSync     sync.Mutex
	Hub         *url.URL
	CurID       int64
}

func New(ds DataStorage) *GoPub {
	subs := ds.GetSubs()
	gp := &GoPub{
		subQueue:    make(chan *subRequest, 100),
		DataStorage: ds,
		subs:        subs,
	}
	for k := range subs {
		if k >= gp.CurID {
			gp.CurID = k + 1
		}
	}
	go func() {
		for {
			gp.processSubQueue()
		}
	}()
	return gp
}

type subRequest struct {
	mode     string
	topic    *url.URL
	callback *url.URL
}

type Subscription struct {
	ID          int64
	Topic       *url.URL
	Callback    *url.URL
	LeaseExpire time.Time
}

type DataStorage interface {
	AddSub(int64, *Subscription)
	RemoveSub(int64)
	GetSubs() map[int64]*Subscription
}

func (gp *GoPub) HubEndpoint(rw http.ResponseWriter, req *http.Request) {
	mode := req.FormValue("mode")
	topic := req.FormValue("topic")
	callback := req.FormValue("callback")
	if mode == "" {
		mode = req.FormValue("hub.mode")
	}
	if topic == "" {
		topic = req.FormValue("hub.topic")
	}
	if callback == "" {
		callback = req.FormValue("hub.callback")
	}

	topicURL, err := url.Parse(topic)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(fmt.Sprintf("Unable to parse topic %s: %s", topic, err)))
	}
	callbackURL, err := url.Parse(callback)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(fmt.Sprintf("Unable to parse callback %s: %s", callback, err)))
	}
	gp.subQueue <- &subRequest{
		mode,
		topicURL,
		callbackURL,
	}
}

func (gp *GoPub) Publish(topic *url.URL, content []byte) {
	gp.subSync.Lock()
	defer gp.subSync.Unlock()
	for _, sub := range gp.subs {
		if sub.Topic.String() == topic.String() {
			req, err := http.NewRequest("POST", sub.Callback.String(), bytes.NewReader(content))
			if err != nil {
				log.Printf("Error making request to: %s", sub.Callback)
			}
			req.Header.Add("Link", fmt.Sprintf("<%s>; rel=\"hub\"", gp.Hub.String()))
			req.Header.Add("Link", fmt.Sprintf("<%s>; rel=\"self\"", topic.String()))
			client := &http.Client{}
			resp, _ := client.Do(req)
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

func (gp *GoPub) processSubQueue() {
	req := <-gp.subQueue
	verifyURL, _ := url.Parse(req.callback.String())

	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	if err != nil {
		log.Printf("Failed to generate challenge: %s", err)
		return
	}
	challenge := hex.EncodeToString(buf)

	values := verifyURL.Query()
	values.Set("hub.mode", req.mode)
	values.Set("hub.topic", req.topic.String())
	values.Set("hub.challenge", challenge)
	if req.mode == "subscribe" {
		values.Set("hub.lease_seconds", strconv.Itoa(60*60*24*7))
	}
	verifyURL.RawQuery = values.Encode()

	resp, err := http.Get(verifyURL.String())
	if err != nil {
		log.Printf("Failed to verify PuSH sub: %s", verifyURL.String())
		return
	}

	buf, _ = ioutil.ReadAll(resp.Body)

	if string(buf) == challenge {
		gp.subSync.Lock()
		defer gp.subSync.Unlock()
		if req.mode == "subscribe" {
			subscription := &Subscription{
				ID:          atomic.AddInt64(&gp.CurID, 1),
				Topic:       req.topic,
				Callback:    req.callback,
				LeaseExpire: time.Now().Add(60 * 60 * 24 * 7 * time.Second),
			}

			gp.subs[subscription.ID] = subscription
			gp.DataStorage.AddSub(subscription.ID, subscription)
		} else {
			for _, sub := range gp.subs {
				if sub.Topic.String() == req.topic.String() && sub.Callback.String() == req.callback.String() {
					delete(gp.subs, sub.ID)
					gp.DataStorage.RemoveSub(sub.ID)
					return
				}
			}
		}
	}
}
