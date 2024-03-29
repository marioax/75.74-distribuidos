package common

import (
    "encoding/csv"
    "errors"
    "fmt"
    "net"
    "os"
) 

const (
    EOP  byte = 0x00  // end of payload
    EOB  byte = 0x01  // end of batch
    EOI  byte = 0x02  // end of id (cause is a string)
    EOT  byte = 0x03  // end of batch transmission
    BAT  byte = 0x04  // batch message 
    QWIN byte = 0x05  // winners query message
    RWIN byte = 0x06  // winners response message
    ACK  byte = 0xff  // acknowledge message
)

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


func getNextBatch(bets *csv.Reader) (uint, string) {
    batch := ""
    bet_number := uint(0)

    for i := 0; i < BATCH_SIZE; i++ {
        bet_record, err := bets.Read()

        // includes EOF
        if err != nil {
            return 0, "" 
        }
        // fields are newline separated
        batch += fmt.Sprintf(
            "%s\n%s\n%s\n%s\n%s%s",
            bet_record[NAME],
            bet_record[SURNAME],
            bet_record[DNI],
            bet_record[BIRTH],
            bet_record[NUM],
            []byte{EOB},
        )            
        bet_number += 1
    }
    return bet_number, batch + string([]byte{EOP})
}


func sendmsg(conn net.Conn, msg_type byte, buf []byte) (error) {
    var err error = nil

    payload := []byte(CLI_ID)
    payload = append(payload, EOI)
    payload = append(payload, msg_type)
    payload = append(payload, buf...)
    payload = append(payload, EOP)
    to_send, sent := len(payload), 0
   
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


func recvmsg(conn net.Conn) ([]byte, error) {
    buf := make([]byte, 1)
    msg := []byte{}

    for {
        _, err := conn.Read(buf)
        
        if err != nil { 
            return nil, err 
        }

        if buf[0] == EOP {
            break
        }
        msg = append(msg, buf[0])
    }
    return msg, nil
}


func sendNextBatch(conn net.Conn, reader *csv.Reader) (uint, error) {
    batch_size, batch := getNextBatch(reader)
    var err error = nil

    // send end of transmission if no more batches to sent
    if batch_size > 0 {
        err = sendmsg(conn, BAT, []byte(batch))
    } else {
        err = sendmsg(conn, EOT, []byte{})
    }
    
    if err != nil {
        return 0, err
    }
    if err = recvACK(conn); err != nil {
        return 0, err
    }
    
    return batch_size, nil
}


func recvACK(conn net.Conn) (error) {
    msg, err := recvmsg(conn)
    if (err == nil && (len(msg) == 0 || msg[0] != ACK)) {
        err = errors.New("ACK not received")
    }
    return err
}


func queryResult(conn net.Conn) (error) {
    // query the server
    err := sendmsg(conn, QWIN, []byte{})
    
    if err != nil {
        return err
    }
    return recvACK(conn)
}


func recvResult(conn net.Conn) (string, error) {
    msg, err := recvmsg(conn)
    
    if err != nil {
        return "", err
    } else if len(msg) == 0 || msg[0] != RWIN {
        return "", errors.New("winners not received")
    }
    // TODO: send ack after recv winners
    return string(msg[1:]), nil
}

