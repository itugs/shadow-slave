package redis

import (
	"bufio"
	log "github.com/Sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

type ShadowRedisSlave struct {
	host   string
	port   string
	conn   net.Conn
	reader *bufio.Reader
	// fetched offset. use this offset for sending ack to master time to time
	offset   int64
	runnable bool
}

func NewShadowRedisSlave(host string, port string) (*ShadowRedisSlave, error) {
	conn, err := net.Dial("tcp", host+":"+port)
	checkError(err)

	log.Infof("Connect to [%s:%s]", host, port)

	server := ShadowRedisSlave{host: host, port: port, conn: conn, reader: bufio.NewReader(conn)}
	server.offset = 0
	server.runnable = true

	return &server, nil
}

func (self ShadowRedisSlave) Start() {

	self.trySync()

	go self.startAckLoop()
	go self.startReadLoop()

	self.wait()
}

func (self ShadowRedisSlave) Close() {
	log.Info("Closing slave server")

	self.runnable = false
	self.conn.Close()

	log.Info("Closed.")
}

func (self ShadowRedisSlave) trySync() {
	masterId, masterOffset := self.fetchMasterIdAndReplOffset()

	if masterOffset > 0 {
		log.Infof("Master [%s] has Repl Offset: [%d]. Try partial sync.", masterId, masterOffset)
		_, err := self.conn.Write([]byte("psync " + masterId + " " + string(masterOffset) + "\r\n"))
		checkError(err)
	} else {
		log.Infof("No Master Repl Offset. Try full sync.")
		_, err := self.conn.Write([]byte("sync\r\n"))
		checkError(err)
	}
}

func (self ShadowRedisSlave) startReadLoop() {
	for self.runnable {
		time.Sleep(2 * time.Second)
	}
}

func (self ShadowRedisSlave) startAckLoop() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for self.runnable {
			select {
			case <-ticker.C:
				// do stuff
				log.Infof("ack with [%d]", self.offset)
			}
		}
	}()
}

func (self ShadowRedisSlave) wait() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	// Block until a signal is received.
	s := <-c
	log.Debugf("Got signal:%v", s)
	self.Close()
}

func (self ShadowRedisSlave) fetchMasterIdAndReplOffset() (string, int64) {
	_, err := self.conn.Write([]byte("INFO\r\n"))
	checkError(err)

	var masterId string = ""
	var offset int64 = 0

Loop:
	for {
		line, err := self.reader.ReadString('\n')
		checkError(err)

		line = strings.Trim(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "run_id:"):
			masterId = line[len("run_id:"):]
			log.Debugf("Master Id : %s", masterId)
			if offset > 0 {
				break Loop
			}
			break
		case strings.HasPrefix(line, "master_repl_offset:"):
			length := line[len("master_repl_offset:"):]
			offset, err = strconv.ParseInt(length, 10, 64)
			checkError(err)
			log.Debugf("Offset : %d", offset)

			if len(masterId) > 0 {
				break Loop
			}
			break
		}
	}

	return masterId, offset
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %v", err)
		os.Exit(1)
	}
}
