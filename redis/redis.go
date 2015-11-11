package redis

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
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

	// TCP options
	tcp, _ := conn.(*net.TCPConn)
	tcp.SetKeepAlive(true)
	tcp.SetReadBuffer(10 * 1024 * 1024)

	log.Infof("Connect to [%s:%s]", host, port)

	server := ShadowRedisSlave{host: host, port: port, conn: conn, reader: bufio.NewReader(conn)}
	server.runnable = true

	return &server, nil
}

func (self *ShadowRedisSlave) Start() {
	self.trySync()

	go self.startAckLoop()
	go self.startReadLoop()

	self.wait()
}

func (self *ShadowRedisSlave) Close() {
	log.Info("Closing slave server")

	self.runnable = false
	self.conn.Close()

	log.Info("Closed.")
}

var RESP_CONTINUE = []byte("+CONTINUE\r\n")

// Try sync to connected instance.
// Try `PSYNC` if back log is available or `SYNC`
func (self *ShadowRedisSlave) trySync() {
	masterId, masterOffset := self.fetchMasterIdAndReplOffset()

	if masterOffset > 0 {
		log.Infof("Master [%s] has Repl Offset: [%d]. Try partial sync.", masterId, masterOffset)
		cmd := "PSYNC " + masterId + " " + strconv.FormatInt(masterOffset, 10) + "\r\n"
		_, err := self.conn.Write([]byte(cmd))
		checkError(err)

		log.Debugf("Sent request : [%s]", cmd)

		expectedLengh := len(RESP_CONTINUE)
		buf := make([]byte, expectedLengh)
		n, err := self.conn.Read(buf)
		checkError(err)

		for i := 0; i < expectedLengh; i++ {
			if RESP_CONTINUE[i] != buf[i] {
				log.Fatalf("Invalid psync response: %s", string(buf[:n]))
				os.Exit(1)
			}
		}

		// set current offset
		self.offset = masterOffset - 1
		log.Infof("Start offset : %d", self.offset)

	} else {
		// log.Infof("No Master Repl Offset. Try full sync.")
		// _, err := self.conn.Write([]byte("SYNC\r\n"))
		// checkError(err)

		// Master will send `+FULLRESYNC {offset}\r\n` response first.
		// Parse it and keep offset to self.offset
		log.Fatal("Can not send PSYNC. Terminated.")
		os.Exit(1)
	}
}

var CMD_INFO = []byte("INFO\r\n")

// Fetch `run_id` and `master_repl_offset` from connected instance
func (self *ShadowRedisSlave) fetchMasterIdAndReplOffset() (string, int64) {
	_, err := self.conn.Write(CMD_INFO)
	checkError(err)

	var checkCounter int = 3
	var masterId string = ""
	var offset int64 = 0
	var firstOffset int64 = 0

Loop:
	for {
		line, err := self.reader.ReadString('\n')
		checkError(err)

		line = strings.Trim(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "run_id:"):
			masterId = line[len("run_id:"):]
			log.Debugf("Master Id : %s", masterId)

			checkCounter--
			if checkCounter == 0 {
				break Loop
			}
			break
		case strings.HasPrefix(line, "master_repl_offset:"):
			length := line[len("master_repl_offset:"):]
			offset, err = strconv.ParseInt(length, 10, 64)
			checkError(err)
			log.Debugf("Offset : %d", offset)

			checkCounter--
			if checkCounter == 0 {
				break Loop
			}
			break
		case strings.HasPrefix(line, "repl_backlog_first_byte_offset"):
			length := line[len("repl_backlog_first_byte_offset:"):]
			firstOffset, err = strconv.ParseInt(length, 10, 64)
			checkError(err)
			log.Debugf("firstOffset : %d", firstOffset)

			checkCounter--
			if checkCounter == 0 {
				break Loop
			}
			break
		}
	}

	return masterId, offset
}

// Read sync stream and just throw away
func (self *ShadowRedisSlave) startReadLoop() {
	buf := make([]byte, 1024*8)

	for self.runnable {
		time.Sleep(2 * time.Second)
		n, err := self.conn.Read(buf)
		if err != nil {
			log.Errorf("Fail read loop : %v", err)
			self.Close()
			return
		}

		log.Debugf("Got response : [%d] [%s]", n, string(buf[:n]))

		self.offset += int64(n)
	}
}

var CMD_REPLCONF = []byte("REPLCONF ACK ")
var CRLF = []byte("\r\n")

// Send REPLCONF ACK to master at every one second
func (self *ShadowRedisSlave) startAckLoop() {
	ticker := time.NewTicker(1 * time.Second)
	var prevOffset int64 = self.offset
	go func() {
		for self.runnable {
			select {
			case <-ticker.C:
				if prevOffset < self.offset {
					cmd := append(CMD_REPLCONF, strconv.FormatInt(self.offset, 10)...)
					cmd = append(cmd, CRLF...)
					_, err := self.conn.Write(cmd)
					checkError(err)

					n := bytes.Index(cmd, []byte{13})
					log.Debugf("Sent ack : [%s]", string(cmd[:n]))

					prevOffset = self.offset
				}
			}
		}
	}()
}

// Wait main process until receiving OS signal
func (self *ShadowRedisSlave) wait() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	// Block until a signal is received.
	s := <-sigChan
	log.Debugf("Got signal:%v", s)
	self.Close()
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %v", err)
		os.Exit(1)
	}
}
