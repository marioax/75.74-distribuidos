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
    ======== general operations ========

    message SPEC:

          __ peer id (1 byte; up to 256 peers)
         /             ___ payload length (4 byte)
        /            /
    |  ID  |  T  |  L  |   P   |
               \            \____ payload (X byte)
                \__ message type (1 byte)

    / ---- HEADER ---- /

*/

var IDSIZE = 1
var TSIZE = 1
var LSIZE = 4
var HSIZE = IDSIZE + TSIZE + LSIZE

// message types
const (
    ACK  byte = 0x00
    BET  byte = 0x01
    EOT  byte = 0x02
    QWIN byte = 0x03
    RWIN byte = 0x04
    NIL  byte = 0xff
)


/*
    @brief parse and send a message
    beware of the protocol limitations (i.e. payload size, id size, etc)
    
    @param  conn    the active connection 
    @param mtype    the message type (see the message types above)
    @param payload  the message data

    @return 
        err     nil if no error has occured
        
*/
func sendmsg(conn net.Conn, mtype byte, payload string) (error) {
    // convert payload length from uint32 to big endian byte array
    payload_size := make([]byte, 4)
    binary.BigEndian.PutUint32(payload_size, uint32(len(payload)))

    cli_id, _ := strconv.ParseUint(CLI_ID, 10, 8)

    // HEADER = ID + T + L + P
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


/*
    @brief receive and parse a message according to the spec
    
    @param  conn    the active connection 
    
    @return 
        mtype   the message type (see the message types above)
        payload the message data 
        err     nil if no error has occured. if err != nil, then mtype = NIL, payload = ""
        
*/
func recvmsg(conn net.Conn) (byte, string, error) {
    header := make([]byte, HSIZE) 
    read := 0

    // read header
    for read < HSIZE {
        n, err := conn.Read(header[read:])

        if err != nil { 
            return NIL, "", err 
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
            return NIL, "", err 
        }
        read += n
    }
    return mtype, string(payload), nil
}



/*
    ======== send/recv wrappers ========
*/

/*
    @brief wrapper for sending an EOT message
    just a wrapper to avoid access to mtype enum

    @param  conn    the active connection 

    @return
        err     nil if no error has occured
*/
func sendEOT(conn net.Conn) (error) {
    err := sendmsg(conn, EOT, "")

    if err == nil {
        err = recvACK(conn)
    }
    return err
}


/*
    @brief wrapper for sending an BET message

    @param  conn    the active connection 
    @param  bets    the parsed bet(s)

    @return
        err     nil if no error has occured
*/
func sendBets(conn net.Conn, bets string) (error) {
    err := sendmsg(conn, BET, bets)

    if err != nil {
        return err
    }
    return recvACK(conn)
}


/*
    @brief wrapper for receiving an ACK message

    @param  conn    the active connection 

    @return 
        err     nil if the ack was received
*/
func recvACK(conn net.Conn) (error) {
    mtype, _, err := recvmsg(conn)
    
    // message received is not ACK
    if mtype != ACK {
        return errors.New("ACK not received")
    }
    return err
}


/*
    @brief sends a QWIN message and waits for a RWIN message 

    @param  conn        the active connection 

    @return 
        win     the number of winners of the current client/agency 
        err     nil if no error has occured
*/
func queryWinners(conn net.Conn) (string, error) {
    err := sendmsg(conn, QWIN, "")
    
    if err != nil {
        return "", err
    }
    mtype, win, err := recvmsg(conn)

    if mtype != RWIN {
        return "", errors.New("winners not received")
    }
    return win, err 
}



/* 
    ======== BET operations ========
    
    since many bets may be sent, each one must end with an
    EOB delimiter.
    each bet field is separated by SEP (i.e. name,lastname,dni,birth,num)
    
    BET payload structure:

      [ name | lastname | dni | birth | num ]
       \__         ________________________/
          \       /
          | bet_0 | EOB | ... | bet_n | EOB |

          / ----------- PAYLOAD ----------- /
*/

var SEP string = ","   // bet separator
var EOB string = "\n"  // end of bet

var CLI_ID = os.Getenv("CLI_ID")

const (
    NAME uint = iota
    LASTNAME 
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
var BATCH_SIZE = 1000


/*
    @brief parse and send a batch of bets 
    batch size is given by a constant

    @param  conn        the active connection 
    @param  bets_csv    

    @return 
        bet_count   the number of bets sent 
        err         nil if no error has occured
*/
func sendNextBatch(conn net.Conn, bets_csv *csv.Reader) (int, error) {
    batch := ""
    bet_count := 0

    for i := 0; i < BATCH_SIZE; i++ {
        bet_record, err := bets_csv.Read()

        // includes EOF
        if err != nil {
            break
        }
        fields := []string{
            bet_record[NAME],
            bet_record[LASTNAME],
            bet_record[DNI],
            bet_record[BIRTH],
            bet_record[NUM],
        }
        bet := strings.Join(fields, SEP) + EOB
        batch += bet
        bet_count += 1
    }
    var err error = nil

    if bet_count > 0 {
        err = sendBets(conn, batch)
    } 
    return bet_count, err
}

