package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	// connect to this socket
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	checkError(err)

	masterId, masterOffset := fetchMasterIdAndReplOffset(conn)

	if masterOffset > 0 {
		fmt.Fprintf(os.Stdout, "Master [%s] has Repl Offset: [%d]. Try partial sync.\n", masterId, masterOffset)
	} else {
		fmt.Fprintf(os.Stdout, "No Master Repl Offset. Try full sync.\n")
	}

	os.Exit(0)
}

func fetchMasterIdAndReplOffset(conn net.Conn) (string, int64) {
	_, err := conn.Write([]byte("INFO\r\n"))
	checkError(err)

	reader := bufio.NewReader(conn)

	var masterId string = ""
	var offset int64 = 0

	for {
		line, err := reader.ReadString('\n')
		checkError(err)

		line = strings.Trim(line, "\r\n")
		if strings.HasPrefix(line, "run_id:") {
			masterId = line[len("run_id:"):]
			fmt.Fprintf(os.Stdout, "Master Id : %s\n", masterId)
			if offset > 0 {
				break
			}
		}

		if strings.HasPrefix(line, "master_repl_offset:") {
			length := line[len("master_repl_offset:"):]
			offset, err = strconv.ParseInt(length, 10, 64)
			checkError(err)
			fmt.Fprintf(os.Stdout, "Offset : %d\n", offset)

			if len(masterId) > 0 {
				break
			}
		}
	}

	return masterId, offset
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
