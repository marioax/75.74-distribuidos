package common

import (
    "os"
    "net"
    "time"
    "encoding/csv"

    log "github.com/sirupsen/logrus"
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
    ID            string
    ServerAddress string
    LoopLapse     time.Duration
    LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
    config ClientConfig
    conn   net.Conn
    shutdown chan bool
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, shutdown chan bool) *Client {
    client := &Client{
        config: config,
        shutdown: shutdown,
    }
    return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
    conn, err := net.Dial("tcp", c.config.ServerAddress)
    if err != nil {
        log.Fatalf(
            "action: connect | result: fail | client_id: %v | error: %v",
            c.config.ID,
            err,
        )
    }
    c.conn = conn
    return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
    defer c.sutdown <- true
    bets_file, err := os.Open("bets.csv")

    if err != nil {
        log.Errorf("action: open_bets_file | result: fail | client_id: %v | error: %v",
            c.config.ID,
            err,
        )
        return
    }
    bets_reader := csv.NewReader(bets_file)
    c.createClientSocket()

    for {
        select {
        // signal handler
        case <- c.shutdown:
            log.Infof("received shutdown alert; closing connection | client_id: %v", c.config.ID)
            c.conn.Close()
            bets_file.Close()
            return 
        default:
        }

        bets_sent, err := sendNextBatch(c.conn, bets_reader)

        if err != nil {
            log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
                c.config.ID,
                err,
            )
            return 
        }
        //bets_sent = 0 // for demo send one batch

        // if there are not more batches to send
        if bets_sent <= 0 {
            log.Infof("action: all_bets_sent | result: success")
            break
        }
        log.Infof("action: bets_sent | result: success | number of bets sent: %d",
            bets_sent,
        )
        time.Sleep(c.config.LoopPeriod)
    }
    err = sendEOT(c.conn)
    c.conn.Close()
    bets_file.Close()
    
    if err != nil {
        log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
            c.config.ID,
            err,
        )
        return
    }
    c.createClientSocket()
    log.Infof("action: query_winners | result: in_progress | client_id: %v", c.config.ID)
    winners, err := queryWinners(c.conn)
    c.conn.Close()

    if err != nil {
        log.Errorf("action: recv_winners | result: fail | client_id: %v | error: %v",
            c.config.ID,
            err,
        )
    } else {
        log.Infof("action: query_winners | result: success | client_id: %v  | winners: %v", c.config.ID, winners)
    }
}
