package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// connect to this socket
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	checkError(err)

	fetchMasterReplOffset(conn)

	os.Exit(0)
}

func fetchMasterReplOffset(conn net.Conn) int64 {
	var offset int64

	_, err := conn.Write([]byte("INFO\r\n"))
	checkError(err)

	var infoLength int
	var bytes int

	for {
		var buf [512]byte
		count, err := conn.Read(buf[0:])
		bytes += count
		checkError(err)

		if count == 0 {
			return 0
		}

		// parse length
		if infoLength == 0 {
			if buf[0] != '$' {
				fmt.Fprintf(os.Stderr, "Invalid protocol. expected `$` but was `%c`", buf[0])
				os.Exit(1)
			}

			skip := 1
			for _, value := range buf[1:] {
				if value == '\r' {
					break
				}
				fmt.Fprintf(os.Stdout, "value : %d, string : %s, int : %d\n", value, string(value), (int)(value-'0'))

				infoLength = (infoLength * 10) + (int)(value-'0')
				skip++
			}

			bytes -= (skip + 4)
			fmt.Fprintf(os.Stdout, "Total info length : %d\n", infoLength)
			fmt.Fprintf(os.Stdout, "======================\n")
		}

		fmt.Fprintf(os.Stdout, "=== Read %d bytes, total %d bytes, gap : %d\n", count, bytes, (infoLength - bytes))
		fmt.Println(string(buf[:count]))
		fmt.Println("--------------------------")
		fmt.Println(buf)
		fmt.Println("--------------------------")

		if bytes == infoLength {
			fmt.Fprintf(os.Stdout, "Info print done\n")
			break
		}
	}

	return offset
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
