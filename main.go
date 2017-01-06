package main

import (
	"flag"
	"fmt"

	"github.com/Civil/graphite-delay-test/checker"
	"github.com/Civil/graphite-delay-test/config"
	"github.com/uber-go/zap"

	"io/ioutil"
	"net/http"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

var logger zap.Logger

type configFileStruct map[string]config.CheckerConfig

func main() {
	configFile := flag.String("c", "delay.yaml", "Path to the config file")
	listenAddress := flag.String("l", ":8099", "ExpVars http listener")
	flag.Parse()
	logger = zap.New(zap.NewTextEncoder())

	logger.Info("Initializing...")
	filename, err := filepath.Abs(*configFile)
	if err != nil {
		logger.Fatal("Error getting file name", zap.Error(err))
	}
	logger.Info("Loading config", zap.String("name", *configFile))
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Fatal("Error reading config file", zap.Error(err))
	}

	cfg := make(configFileStruct)
	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		logger.Fatal("Error reading config file", zap.Error(err))
	}

	if len(cfg) == 0 {
		logger.Fatal("No checkers defined in the config file!")
	}

	exit := make(chan struct{})

	for name, c := range cfg {
		err = config.Validate(&c, name)
		fmt.Printf("%+v\n", c)
		if err != nil {
			logger.Error("Error validating config file", zap.Error(err))
			return
		}
		// TODO: Actually pass the config!
		chk := checker.NewChecker(c, name, exit, logger)
		go chk.Run()
	}
	logger.Info("Started")
	logger.Fatal("Failed to start HTTP server", zap.Error(http.ListenAndServe(*listenAddress, nil)))
}
