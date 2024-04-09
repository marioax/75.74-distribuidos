package common

import (
    "os"
    "os/signal"
    "syscall"
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
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
    client := &Client{
        config: config,
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
    // capture signals
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM) 
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
        case <-sig:
            log.Infof("received shutdown alert; closing connection | client_id: %v", c.config.ID)
            break
        default:
        }

        bets_sent, err := sendNextBatch(c.conn, bets_reader)

        if err != nil {
            log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
                c.config.ID,
                err,
            )
            break   
        }


        // if there are not more batches to send
        if bets_sent <= 0 {
            break
        }
        log.Infof("action: bets_sent | result: success | number of bets sent: %d",
            bets_sent,
        )
        time.Sleep(c.config.LoopPeriod)
        //break // for demo send one batch
    }
    sendEOT(c.conn) // dont care if fails
    c.conn.Close()
    bets_file.Close()
    log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
