package common

import (
    "errors"
    "strings"
    "encoding/binary"
    "net"
    "os"
) 

var HSIZE = 5
var TSIZE = 1
var LSIZE = 4

// message types
var ACK byte = 0x00
var BET byte = 0x01

// BET type 
var SEP string = ","
var EOB string = "\n"


func sendAll(conn net.Conn, mtype byte, payload string) (error) {
    payload_size := make([]byte, 4)
    binary.BigEndian.PutUint32(payload_size, uint32(len(payload)))

    msg := []byte{mtype}
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
    mtype := header[0] // change this if type size grows beyond 1 byte
    toread := int(binary.BigEndian.Uint32(header[TSIZE:LSIZE + TSIZE]))
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


func sendBet(conn net.Conn) (error) {
    fields := []string{
        os.Getenv("CLI_ID"),
        os.Getenv("NAME"),
        os.Getenv("SURNAME"),
        os.Getenv("DNI"),
        os.Getenv("BIRTH"),
        os.Getenv("NUM"),
    }
    bet := strings.Join(fields, SEP) + EOB
    err := sendAll(conn, BET, bet)

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
