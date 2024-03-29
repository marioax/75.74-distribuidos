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
    shutdown chan bool
	conn   net.Conn
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
    client.conn.SetReadDeadline(time.Now().Add(time.Second)) 
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
    // capture signals
    bets_file, err := os.Open("bets.csv")

    if err != nil {
   	    log.Errorf("action: open_bets_file | result: fail | client_id: %v | error: %v",
            c.config.ID,
	        err,
	    )
	    return
    }
    bets_reader := csv.NewReader(bets_file)


    // send all batches of the csv bets file 
    for {
        select {
        /*
		case <-timeout:
	        log.Infof("action: timeout_detected | result: success | client_id: %v",
                c.config.ID,
            )
			break loop
        */        
        // signal handler
        case <-c.shutdown:
            log.Infof("action: shutdown_detected | client_id: %v", c.config.ID)
            bets_file.Close()
            c.conn.Close()
            return 
		default:
		}
        c.createClientSocket()
        bets_sent, err := sendNextBatch(c.conn, bets_reader) 
        c.conn.Close()

        if err != nil {
   			log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v",
                c.config.ID,
				err,
			)
            c.conn.Close()
            bets_file.Close()
		    return 
        }
        
        if bets_sent <= 0 {
            break
        }
        //if c.config.ID == "1" {
        //    bets_reader.ReadAll() // for demo 

        //}
	    log.Infof("action: send_batch | result: success | number of bets sent: %d",
            bets_sent,
        )
        // Wait a time between sending one message and the next one
		time.Sleep(c.config.LoopPeriod)
	}
    bets_file.Close()
    c.createClientSocket()
	log.Infof("action: query_winners | result: in_progress | client_id: %v", c.config.ID)
    // here the recv must block, since it will
    client.conn.SetReadDeadline(time.Time{}) 
    winners, err := queryWinners(c.conn)
    c.conn.Close()

    if err != nil {
        log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v",
            c.config.ID,
		    err,
		)
    }
	log.Infof("action: query_winners | result: success | client_id: %v  | winners: %v", c.config.ID, winners)
}
