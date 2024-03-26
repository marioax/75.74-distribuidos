package common

import (
    "errors"
    "bytes"
    "fmt"
    "net"
    "os"
) 

var EOP byte = 0
var EOB byte = 1
var ACK string = "ACK"

func sendBet(conn net.Conn) (error) {
    // fields are newline separated
    payload := []byte(
        fmt.Sprintf(
            "%s\n%s\n%s\n%s\n%s\n%s",
            os.Getenv("CLI_ID"),
            os.Getenv("NAME"),
            os.Getenv("SURNAME"),
            os.Getenv("DNI"),
            os.Getenv("BIRTH"),
            os.Getenv("NUM"),
        ),
    )
    payload = append(payload, EOB)
    payload = append(payload, EOP)

    to_send, sent := len(payload), 0
    var err error = nil

    for sent < to_send {
        n := 0
        n, err = conn.Write(payload[sent:])
        
        if err != nil {
            break
        }
        sent += n
    }
    return err
}


func recvBetACK(conn net.Conn) (string, error) {
    buf := make([]byte, 1024) // should receive "ACK\0"
    read := 0

    for {
        n, err := conn.Read(buf)
        
        if err != nil { 
            return "", err 
        }
        read += n

        if bytes.HasSuffix(buf, []byte{EOP}) {
            break
        }
    }
    str_buf := string(buf[:read - 1])
    
    if str_buf != ACK {
        return "", errors.New("message received is not ACK")
    }
    return str_buf, nil
}
