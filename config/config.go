package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type CheckerConfig struct {
	CheckURL             string `yaml:"check_url"`
	SendHost             string `yaml:"send_host"`
	GraphiteHost         string `yaml:"graphite_host"`
	ResultMetricPattern  string `yaml:"result_metric_pattern"`
	MetricName           string `yaml:"metric_name"`
	Protocol             string `yaml:"protocol"`
	CarbonType           string `yaml:"carbon_type"`
	CheckTimeout         time.Duration `yaml:"check_timeout"`
	SendTimeout          time.Duration `yaml:"send_timeout"`
	RetryTime            time.Duration `yaml:"retry_time"`
	StatsInterval        time.Duration `yaml:"stats_interval"`
	InternalStatsDumpInterval time.Duration `yaml:"internal_stats_dump_interval"`
	CheckInterval        time.Duration `yaml:"check_interval"`
	LogToConsole         bool `yaml:"log_to_console"`
}

var defaultCfg = CheckerConfig{
	CheckURL:             "http://localhost:8080/render",
	SendHost:             "localhost:2003",
	GraphiteHost:         "",
	MetricName:           "test.delay.1s",
	Protocol:             "tcp",
	CarbonType:           "graphite-web-0.9",
	ResultMetricPattern:  "carbon.delay.{host}",
	CheckInterval:        1 * time.Second,
	CheckTimeout:         15 * time.Second,
	RetryTime:            100 * time.Millisecond,
	SendTimeout:          1 * time.Second,
	StatsInterval:        1 * time.Second,
	InternalStatsDumpInterval: 1 * time.Second,
	LogToConsole:         true,
}

var hostname string

func getHostname() string {
	if hostname == "" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			hostname = "unknown"
			return "unknown"
		}
		hostname = strings.Replace(h, ".", "_", -1)
	}
	return hostname
}

func expandString(s string, cfg *CheckerConfig, name string) string {
	s = strings.Replace(s, "{host}", getHostname(), -1)
	s = strings.Replace(s, "{name}", name, -1)
	s = strings.Replace(s, "{check_interval}", cfg.CheckInterval.String(), -1)
	return s
}

func Validate(cfg *CheckerConfig, name string) error {
	if cfg.CarbonType == "" {
		cfg.CarbonType = defaultCfg.CarbonType
	}

	if cfg.CarbonType != "carbonserver" && cfg.CarbonType != "graphite-web-0.9" && cfg.CarbonType != "graphite-web-1.0" && cfg.CarbonType != "graphite-web-0.10" {
		return fmt.Errorf("Filed Protocol have invalid value: %v", cfg.CarbonType)
	}

	if cfg.CheckURL == "" {
		cfg.CheckURL = defaultCfg.CheckURL
	}
	if cfg.SendHost == "" {
		cfg.SendHost = defaultCfg.SendHost
	}
	if cfg.GraphiteHost == "" {
		cfg.GraphiteHost = defaultCfg.GraphiteHost
	}
	if cfg.MetricName == "" {
		cfg.MetricName = defaultCfg.MetricName
	}

	if cfg.Protocol == "" {
		cfg.Protocol = defaultCfg.Protocol
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = defaultCfg.CheckInterval
	}
	if cfg.CheckTimeout == 0 {
		cfg.CheckTimeout = defaultCfg.CheckTimeout
	}
	if cfg.RetryTime == 0 {
		cfg.RetryTime = defaultCfg.RetryTime
	}
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = defaultCfg.SendTimeout
	}
	if cfg.StatsInterval == 0 {
		cfg.StatsInterval = defaultCfg.StatsInterval
	}
	if cfg.InternalStatsDumpInterval == 0 {
		cfg.InternalStatsDumpInterval = defaultCfg.InternalStatsDumpInterval
	}
	if cfg.ResultMetricPattern == "" {
		cfg.ResultMetricPattern = defaultCfg.ResultMetricPattern
	}
	if !cfg.LogToConsole {
		cfg.LogToConsole = defaultCfg.LogToConsole
	}
	cfg.MetricName = expandString(cfg.MetricName, cfg, name)
	cfg.ResultMetricPattern = expandString(cfg.ResultMetricPattern, cfg, name)

	return nil
}
