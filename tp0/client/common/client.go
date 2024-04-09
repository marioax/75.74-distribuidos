package common

import (
    "os"
    "os/signal"
    "syscall"
    "net"
    "time"

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


loop:
    // Send messages if the loopLapse threshold has not been surpassed
    for timeout := time.After(c.config.LoopLapse); ; {
        select {
        case <-timeout:
            log.Infof("action: timeout_detected | result: success | client_id: %v",
                c.config.ID,
            )
            break loop
        // signal handler
        case <-sig:
            c.conn.Close()
            log.Infof("received shutdown alert; closing connection | client_id: %v", c.config.ID)
            break loop
        default:
        }
        // Create the connection the server in every loop iteration. Send an
        c.createClientSocket()

        // send a bet
        err := sendBet(c.conn)
        c.conn.Close()

        if err != nil {
            log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
                c.config.ID,
                err,
            )
            return
        }
        log.Infof("action: bet_sent | result: success | dni: %s | number: %s",
            os.Getenv("DNI"),
            os.Getenv("NUM"),
        )
        break
        // Wait a time between sending one message and the next one
        //time.Sleep(c.config.LoopPeriod)
    }

    log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
