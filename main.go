package main

import (
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	g2g "github.com/peterbourgon/g2g"
	"github.com/uber-go/zap"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	CARBONSERVER  = 1
	GRAPHITEWEB09 = 2
)

var config = struct {
	CheckURL             string
	SendHost             string
	GraphiteHost         string
	MetricName           string
	Protocol             string
	CarbonType           string
	CheckTimeout         time.Duration
	SendTimeout          time.Duration
	RetryTime            time.Duration
	Interval             time.Duration
	GraphiteSendInterval time.Duration
}{
	CheckURL:             "http://localhost:8080/render",
	SendHost:             "localhost:2003",
	GraphiteHost:         "localhost:3002",
	MetricName:           "test.delay.1s",
	Protocol:             "tcp",
	CarbonType:           "carbonserver",
	CheckTimeout:         15 * time.Second,
	RetryTime:            100 * time.Millisecond,
	SendTimeout:          1 * time.Second,
	Interval:             1 * time.Second,
	GraphiteSendInterval: 60 * time.Second,
}

// Metrics is a structure that contains all internal application statistics
var Metrics = struct {
	CheckRequests *expvar.Int
	MetricDelayNS *expvar.Int
	SendTimeouts  *expvar.Int
	SendErrors    *expvar.Int
	CheckTimeouts *expvar.Int
}{
	CheckRequests: expvar.NewInt("check_requests"),
	MetricDelayNS: expvar.NewInt("metric_delay_ns"),
	SendTimeouts:  expvar.NewInt("send_timeouts"),
	SendErrors:    expvar.NewInt("send_errors"),
	CheckTimeouts: expvar.NewInt("check_timeouts"),
}

var logger zap.Logger
var readyToCheck = int32(0)

func sendMetric(ts int64) error {
	conn, err := net.DialTimeout(config.Protocol, config.SendHost, 150*time.Millisecond)
	if err != nil {
		Metrics.SendErrors.Add(1)
		return err
	}
	sendStr := config.MetricName + " " + strconv.FormatInt(ts, 10) + " " + strconv.FormatInt(ts, 10) + "\n"
	_, err = conn.Write([]byte(sendStr))
	if err != nil {
		Metrics.SendErrors.Add(1)
		return err
	}
	return nil
}

func generateIntervalMetrics(toCheck chan<- int64, exit <-chan struct{}) {
	tick := time.Tick(config.Interval)
	for {
		select {
		case <-exit:
			return
		case <-tick:
			ts := time.Now().Unix()
			err := sendMetric(ts)
			if err != nil {
				logger.Error("Can't send recurring metric", zap.Error(err))
			}
			if atomic.CompareAndSwapInt32(&readyToCheck, 1, 0) {
				Metrics.CheckRequests.Add(1)
				toCheck <- ts
			}
		}
	}
}

func sender(toCheck chan<- int64, checked <-chan struct{}, exit <-chan struct{}) {
	t := time.Now()
	Metrics.CheckRequests.Add(1)
	toCheck <- t.Unix()
	for {
		select {
		case <-exit:
			return
		case <-checked:
			time.Sleep(config.Interval - time.Since(t))
			t = time.Now()
			Metrics.CheckRequests.Add(1)
			toCheck <- t.Unix()
		}
	}
}

type carbonserverMetric struct {
	// We don't care about other fields in response for now
	/*
		name      string
		startTime uint32
		stopTime  uint32
		stepTime  uint32
		isAbsent  []bool
	*/
	Values []float64 `json:"values"`
}

type graphiteResponse struct {
	// carbonserver
	Metrics []carbonserverMetric `json:"metrics"`
	// graphite-web 0.9
	Datapoints [][]float64
}

func checker(senderChan <-chan int64, exit <-chan struct{}) {
	client := &http.Client{
		Timeout: config.CheckTimeout,
	}
	precision := 0.0001
	responder := CARBONSERVER
	if config.CarbonType == "graphite-web-0.9" {
		responder = GRAPHITEWEB09
	}
	baseURL := config.CheckURL + "/?format=json&target=" + config.MetricName + "&from="
	for {
		select {
		case <-exit:
			return
		case ts := <-senderChan:
			t0 := time.Now()
			interval := int64(config.Interval / time.Second)
			checkURL := baseURL + strconv.FormatInt(ts-4*interval, 10) + "&until=" + strconv.FormatInt(ts+4*interval, 10)
			tsF64 := float64(ts)

			timeout := time.After(config.CheckTimeout)
			tick := time.Tick(config.RetryTime)
			timeoutHappened := false
			for {
				resp, err := client.Get(checkURL)
				if err != nil {
					logger.Error("Error fetching data", zap.String("url", checkURL), zap.Error(err))
					continue
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					logger.Error("Got non 200 response")
					continue
				}
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					logger.Error("Can't read response body", zap.String("url", checkURL), zap.Error(err))
					continue
				}
				r := graphiteResponse{}
				err = json.Unmarshal(body, &r)
				if err != nil {
					logger.Error("Can't decode json", zap.String("url", checkURL), zap.Error(err))
					continue
				}

				found := false
				if responder == CARBONSERVER {
					if len(r.Metrics) != 0 {
						for _, v := range r.Metrics[0].Values {
							if math.Abs(v-tsF64) < precision {
								found = true
								break
							}
						}
					}
				} else {
					if len(r.Datapoints) != 0 {
						for _, t := range r.Datapoints {
							v := t[0]
							if math.Abs(v-tsF64) < precision {
								found = true
								break
							}
						}
					}
				}
				if found {
					Metrics.MetricDelayNS.Add(time.Since(t0).Nanoseconds())
					break
				}

				select {
				case <-timeout:
					logger.Info("Checker timeout", zap.Int64("timestamp", ts))
					Metrics.CheckTimeouts.Add(1)
					timeoutHappened = true
				case <-tick:
				}
				if timeoutHappened {
					break
				}
			}
			atomic.StoreInt32(&readyToCheck, 1)
		}

	}
}

func dumpStats(exit <-chan struct{}) {
	for {
		select {
		case <-exit:
			return
		case <-time.After(config.Interval * time.Second):
			logger.Info("Stats",
				zap.String("check_requests", Metrics.CheckRequests.String()),
				zap.String("check_timeouts", Metrics.CheckTimeouts.String()),
				zap.String("metric_delay_ns", Metrics.MetricDelayNS.String()),
				zap.String("send_timeouts", Metrics.SendTimeouts.String()),
				zap.String("send_errors", Metrics.SendErrors.String()),
			)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func main() {
	checkURL := flag.String("c", config.CheckURL, "URL (including http or https) where we'll perform our requests. Requests are done in graphite format and expects graphite-compatible json in the output")
	sendHost := flag.String("s", config.SendHost, "host:port where we'll send data (in graphite line format)")
	graphiteHost := flag.String("g", config.GraphiteHost, "host:port where we'll send our own stats (in graphite line format)")
	carbonType := flag.String("t", config.CarbonType, "Check URL Type. Available: carbonserver, graphite-web-0.9")
	interval := flag.Duration("i", config.Interval, "expected interval in seconds")

	logger = zap.New(zap.NewTextEncoder())

	logger.Info("Initializing...")

	flag.Parse()
	if *carbonType != "carbonserver" && *carbonType != "graphite-web-0.9" {
		logger.Fatal("Unknown Check URL Type", zap.String("got", *carbonType))
	}

	config.CheckURL = *checkURL
	config.SendHost = *sendHost
	config.Interval = *interval
	config.CarbonType = *carbonType

	senderChan := make(chan int64) // Send the value we want to check for.
	exit := make(chan struct{})

	if *graphiteHost != "" {
		hostname, err := os.Hostname()
		if err != nil {
			logger.Fatal("Can't get hostname", zap.Error(err))
		}
		hostname = strings.Replace(hostname, ".", "_", -1)

		graphite := g2g.NewGraphite(*graphiteHost, config.GraphiteSendInterval, 1*time.Second)
		graphite.Register(fmt.Sprintf("carbon.delay.%s.metric_delay_ns", hostname), Metrics.MetricDelayNS)
		graphite.Register(fmt.Sprintf("carbon.delay.%s.send_timeouts", hostname), Metrics.SendTimeouts)
		graphite.Register(fmt.Sprintf("carbon.delay.%s.send_errors", hostname), Metrics.SendErrors)
		graphite.Register(fmt.Sprintf("carbon.delay.%s.check_timeouts", hostname), Metrics.CheckTimeouts)
		graphite.Register(fmt.Sprintf("carbon.delay.%s.check_requests", hostname), Metrics.CheckRequests)
	} else {
		go dumpStats(exit)
	}

	logger.Info("Generating 5 events")
	go generateIntervalMetrics(senderChan, exit)
	time.Sleep(5 * config.Interval)
	logger.Info("Started")
	atomic.StoreInt32(&readyToCheck, 1)
	checker(senderChan, exit)
}
