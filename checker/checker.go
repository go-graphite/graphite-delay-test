package checker

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Civil/graphite-delay-test/config"
	g2g "github.com/peterbourgon/g2g"
	"github.com/uber-go/zap"
)

const (
	CARBONSERVER  = 1
	GRAPHITEWEB09 = 2
	GRAPHITEWEB10 = 3
)

type Metrics struct {
	MetricDelayNS *expvar.Int
	CheckRequests *expvar.Int
	CheckTimeouts *expvar.Int
	SendTimeouts  *expvar.Int
	SendErrors    *expvar.Int
}

type Checker struct {
	config.CheckerConfig
	logger zap.Logger

	readyToCheck int32

	metricsMap map[string]*expvar.Int
	metrics    Metrics
	exit       <-chan struct{}
}

func NewChecker(cfg config.CheckerConfig, name string, exit <-chan struct{}, logger zap.Logger) *Checker {
	metricsMap := map[string]*expvar.Int{
		"metric_delay_ns": expvar.NewInt(name + ".metric_delay_ns"),
		"check_requests":  expvar.NewInt(name + ".check_requests"),
		"check_timeouts":  expvar.NewInt(name + ".check_timeouts"),
		"send_timeouts":   expvar.NewInt(name + ".send_timeouts"),
		"send_errors":     expvar.NewInt(name + ".send_errors"),
	}

	c := Checker{
		CheckerConfig: cfg,
		logger:        logger.With(zap.String("checker_name", name)),
		readyToCheck:  1,
		metricsMap:    metricsMap,
		metrics: Metrics{
			MetricDelayNS: metricsMap["metric_delay_ns"],
			CheckRequests: metricsMap["check_requests"],
			CheckTimeouts: metricsMap["check_timeouts"],
			SendTimeouts:  metricsMap["send_timeouts"],
			SendErrors:    metricsMap["send_errors"],
		},
		exit: exit,
	}
	return &c
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
	metrics []carbonserverMetric `json:"metrics"`
	// graphite-web 0.9
	Datapoints [][]float64
}

// metrics is a structure that contains all internal application statistics

func (checker *Checker) sendMetric(ts int64) error {
	conn, err := net.DialTimeout(checker.Protocol, checker.SendHost, 150*time.Millisecond)
	if err != nil {
		return err
	}
	sendStr := checker.MetricName + " " + strconv.FormatInt(ts, 10) + " " + strconv.FormatInt(ts, 10) + "\n"
	_, err = conn.Write([]byte(sendStr))
	if err != nil {
		return err
	}
	return nil
}

func (checker *Checker) generateIntervalmetrics(toCheck chan<- int64, exit <-chan struct{}) {
	tick := time.Tick(checker.CheckInterval)
	for {
		select {
		case <-exit:
			return
		case <-tick:
			ts := time.Now().Unix()
			err := checker.sendMetric(ts)
			if err != nil {
				checker.logger.Error("Can't send recurring metric", zap.Error(err))
				checker.metrics.SendErrors.Add(1)
				// No need to even try to check for a data we haven't sent.
				continue
			}
			if atomic.CompareAndSwapInt32(&checker.readyToCheck, 1, 0) {
				checker.metrics.CheckRequests.Add(1)
				toCheck <- ts
			}
		}
	}
}

func (checker *Checker) sender(toCheck chan<- int64, checked <-chan struct{}, exit <-chan struct{}) {
	t := time.Now()
	checker.metrics.CheckRequests.Add(1)
	toCheck <- t.Unix()
	for {
		select {
		case <-exit:
			return
		case <-checked:
			time.Sleep(checker.CheckInterval - time.Since(t))
			t = time.Now()
			checker.metrics.CheckRequests.Add(1)
			toCheck <- t.Unix()
		}
	}
}

func (checker *Checker) dumpStats() {
	checker.logger.Info("Will dump stats to the console")
	tick := time.Tick(checker.InternalStatsDumpInterval)

	for {
		select {
		case <-checker.exit:
			return
		case <-tick:
			for name := range checker.metricsMap {
				checker.logger.Info(name, zap.String("value", checker.metricsMap[name].String()))
			}
		}
	}
}

func (checker *Checker) registerMetric(graphite *g2g.Graphite, pattern, name string) {
	graphite.Register(fmt.Sprintf(pattern + ".%s", name), checker.metricsMap[name])
}

func (checker *Checker) doChecks(senderChan <-chan int64) {
	if checker.GraphiteHost != "" {
		graphite := g2g.NewGraphite(checker.GraphiteHost, checker.InternalStatsDumpInterval, checker.CheckInterval)
		for name := range checker.metricsMap {
			checker.registerMetric(graphite, checker.ResultMetricPattern, name)
		}
	}
	if checker.LogToConsole {
		go checker.dumpStats()
	}

	client := &http.Client{
		Timeout: checker.CheckTimeout,
	}
	precision := 0.0001
	responder := CARBONSERVER
	if checker.CarbonType == "graphite-web-0.9" {
		responder = GRAPHITEWEB09
	}
	baseURL := checker.CheckURL + "/?format=json&noCache=1&target=" + checker.MetricName + "&from="
	var body []byte
	for {
		select {
		case <-checker.exit:
			return
		case ts := <-senderChan:
			t0 := time.Now()
			interval := int64(checker.CheckInterval / time.Second)
			checkURL := baseURL + strconv.FormatInt(ts-4*interval, 10) + "&until=" + strconv.FormatInt(ts+4*interval, 10)
			tsF64 := float64(ts)

			timeout := time.After(checker.CheckTimeout)
			tick := time.Tick(checker.RetryTime)
			timeoutHappened := false
			for {

				var graphitewebResponse []graphiteResponse
				carbonserverResponse := graphiteResponse{}
				resp, err := client.Get(checkURL)
				found := false
				if err != nil {
					checker.logger.Error("Error fetching data", zap.String("url", checkURL), zap.Error(err))
					continue
				}
				if resp.StatusCode != 200 {
					checker.logger.Error("Got non 200 response")
					goto END
				}
				body, err = ioutil.ReadAll(resp.Body)
				if err != nil {
					checker.logger.Error("Can't read response body", zap.String("url", checkURL), zap.Error(err))
					goto END
				}

				if responder == CARBONSERVER {
					err = json.Unmarshal(body, &carbonserverResponse)
				} else {
					err = json.Unmarshal(body, &graphitewebResponse)
				}
				if err != nil {
					checker.logger.Error("Can't decode json", zap.String("url", checkURL), zap.Error(err))
					goto END
				}
				if responder == CARBONSERVER {
					if len(carbonserverResponse.metrics) != 0 {
						for _, v := range carbonserverResponse.metrics[0].Values {
							if math.Abs(v-tsF64) < precision {
								found = true
								break
							}
						}
					}
				} else {
					checker.logger.Info("Got body", zap.String("body", string(body)))
					if len(graphitewebResponse[0].Datapoints) != 0 {
						for _, t := range graphitewebResponse[0].Datapoints {
							v := t[0]
							if math.Abs(v-tsF64) < precision {
								found = true
								break
							}
						}
					}
				}
				END:
				resp.Body.Close()
				if found {
					checker.metrics.MetricDelayNS.Add(time.Since(t0).Nanoseconds())
					break
				}

				select {
				case <-timeout:
					checker.logger.Info("Checker timeout", zap.Int64("timestamp", ts), zap.String("url", checkURL))
					checker.metrics.CheckTimeouts.Add(1)
					timeoutHappened = true
				case <-tick:
				}
				if timeoutHappened {
					break
				}
			}
			atomic.StoreInt32(&checker.readyToCheck, 1)
		}

	}
}

func (checker *Checker) Run() {
	senderChan := make(chan int64) // Send the value we want to check for.
	go checker.generateIntervalmetrics(senderChan, checker.exit)
	checker.logger.Info("Generating warmup events", zap.Int("events", checker.WarmupEvents))
	time.Sleep(time.Duration(checker.WarmupEvents) * checker.CheckInterval)
	checker.doChecks(senderChan)
}
