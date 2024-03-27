package common

import (
    "encoding/csv"
    "errors"
    "bytes"
    "fmt"
    "net"
    "os"
) 

var EOP string = "\x00"
var EOB string = "\x01"
var ACK string = "ACK"

// asuming that:
// fullname <= 50 chars
// dni <= 8 chars
// date <= 10 chars
// bet number is int16 <= 5 chars
// worst case = 73B * 100 < 8kB 
var BATCH_SIZE = 100

var CLI_ID = os.Getenv("CLI_ID")

const (
    NAME uint = iota
    SURNAME
    DNI
    BIRTH
    NUM
)


func getNextBatch(bets *csv.Reader) (int, string) {
    batch := ""
    bet_number := 0

    for i := 0; i < BATCH_SIZE; i++ {
        bet_record, err := bets.Read()

        // includes EOF
        if err != nil {
            return 0, "" 
        }
        batch += fmt.Sprintf(
            "%s\n%s\n%s\n%s\n%s\n%s%s",
            CLI_ID,
            bet_record[NAME],
            bet_record[SURNAME],
            bet_record[DNI],
            bet_record[BIRTH],
            bet_record[NUM],
            EOB,
        )            
        bet_number += 1
    }
    return bet_number, batch + EOP
}


func sendBets(conn net.Conn, bets string) (error) {
    // fields are newline separated
    var err error = nil
    _bets := []byte(bets)
    //fmt.Printf("[BATCH %d] %s\n", i, bets)
    to_send, sent := len(_bets), 0
   
    for sent < to_send {
        n := 0
        n, err = conn.Write(_bets[sent:])
        
        if err != nil {
            break
        }
        sent += n
    }
    return err
}


func recvBetsACK(conn net.Conn, sig chan os.Signal) (string, error) {
    buf := make([]byte, 1024) // should receive "ACK\0"
    read := 0

    for {
        n, err := conn.Read(buf)
        
        if err != nil { 
            return "", err 
        }
        read += n

        if bytes.HasSuffix(buf, []byte(EOP)) {
            break
        }
    }
    str_buf := string(buf[:read - 1])
    
    if str_buf != ACK {
        return "", errors.New("message received is not ACK")
    }
    return str_buf, nil
}
