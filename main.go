package main

import (
	"flag"
	"os"

	redis "./redis"
	log "github.com/Sirupsen/logrus"
)

func main() {
	host, port := parseFlags()

	server, _ := redis.NewShadowRedisSlave(host, port)
	server.Start()

	os.Exit(0)
}

func parseFlags() (string, string) {
	host := flag.String("host", "127.0.0.1", "Redis Host")
	port := flag.String("port", "6379", "Redis port")
	loglevel := flag.String("loglevel", "debug", "Log level")

	flag.Parse()

	// Adjust log level
	l, err := log.ParseLevel(*loglevel)
	if err != nil {
		l = log.InfoLevel
		log.Warnf("Can not parse loglevel : %s", err.Error())
	}

	log.SetLevel(l)

	return *host, *port
}
