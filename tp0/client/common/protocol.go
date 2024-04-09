package common

import (
    "encoding/csv"
    "errors"
    "strings"
    "strconv"
    "encoding/binary"
    "net"
    "os"
) 


/*
    message structure:

          __ peer id (1 byte; up to 256 peers)
         /             ___ payload length (4 byte)
        /            /
    |  ID  |  T  |  L  |   P   |
               \            \____ payload (X byte)
                \__ message type (1 byte)

*/

var HSIZE = 6
var IDSIZE = 1
var TSIZE = 1
var LSIZE = 4

// message types
var ACK byte = 0x00
var BET byte = 0x01
var EOT byte = 0x02

// BET type 
var SEP string = ","
var EOB string = "\n"

var CLI_ID = os.Getenv("CLI_ID")

const (
    NAME uint = iota
    SURNAME
    DNI
    BIRTH
    NUM
)

// asuming that:
// fullname <= 50 chars
// dni <= 8 chars
// date <= 10 chars
// bet number is int16 <= 5 chars
// worst case = 73B * 100 < 8kB 
var BATCH_SIZE = 100



func sendAll(conn net.Conn, mtype byte, payload string) (error) {
    payload_size := make([]byte, 4)
    binary.BigEndian.PutUint32(payload_size, uint32(len(payload)))
    cli_id, _ := strconv.ParseUint(CLI_ID, 10, 8)

    msg := []byte{byte(cli_id), mtype}
    msg = append(msg, payload_size...)
    msg = append(msg, []byte(payload)...)

    to_send, sent := len(msg), 0
    var err error = nil
   
    for sent < to_send {
        n := 0
        n, err = conn.Write(msg[sent:])
        
        if err != nil {
            break
        }
        sent += n
    }
    return err
}


func recvAll(conn net.Conn) (byte, string, error) {
    header := make([]byte, HSIZE) 
    read := 0

    // read header
    for read < HSIZE {
        n, err := conn.Read(header[read:])

        if err != nil { 
            return 0xff, "", err 
        }
        read += n
    }
    // peerid := header[0] // change this if peer id size grows beyond 1 byte
    mtype := header[1] // change this if type size grows beyond 1 byte
    toread := int(binary.BigEndian.Uint32(header[IDSIZE + TSIZE:HSIZE]))
    payload := make([]byte, toread)
    read = 0

    // read payload
    for read < toread {
        n, err := conn.Read(payload[read:])
        
        if err != nil { 
            return mtype, "", err 
        }
        read += n
    }
    return mtype, string(payload), nil
}


func sendEOT(conn net.Conn) (error) {
    return sendAll(conn, EOT, "")
}


func sendBets(conn net.Conn, bets string) (error) {
    err := sendAll(conn, BET, bets)

    if err != nil {
        return err
    }
    mtype, _, err := recvAll(conn)
    
    // message received is not ACK
    if err == nil && mtype != ACK {
        return errors.New("ACK not received")
    }
    return err
}


func sendNextBatch(conn net.Conn, bets *csv.Reader) (int, error) {
    batch := ""
    bet_number := 0

    for i := 0; i < BATCH_SIZE; i++ {
        bet_record, err := bets.Read()

        // includes EOF
        if err != nil {
            return 0, err
        }
        fields := []string{
            bet_record[NAME],
            bet_record[SURNAME],
            bet_record[DNI],
            bet_record[BIRTH],
            bet_record[NUM],
        }
        bet := strings.Join(fields, SEP) + EOB
        batch += bet
        bet_number += 1
    }
    var err error = nil

    if bet_number > 0 {
        err = sendBets(conn, batch)
    } 
    return bet_number, err
}

