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
        c.createClientSocket()

        select {
        case <-c.shutdown:
            log.Infof("action: shutdown_detected | client_id: %v", c.config.ID)
            bets_file.Close()
            c.conn.Close()
            return 
		default:
		}
        bets_sent, err := sendNextBatch(c.conn, bets_reader) 
        c.conn.Close()

        if err != nil {
   			log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v",
                c.config.ID,
				err,
			)
            bets_file.Close()
		    return 
        }
        
        if bets_sent <= 0 {
	        log.Infof("action: all_bets_sent | result: success")
            break
        }
        //if c.config.ID == "2" {
        //bets_reader.ReadAll() // for demo only send one batch
        //}
	    log.Infof("action: send_batch | result: success | number of bets sent: %d",
            bets_sent,
        )
        // Wait a time between sending one message and the next one
		time.Sleep(c.config.LoopPeriod)
	}
    bets_file.Close()

    // result ping loop
    for {
        c.createClientSocket(); 

	    select {
        case <-c.shutdown:
            log.Infof("action: shutdown_detected | client_id: %v", c.config.ID)
            c.conn.Close()
            return 
        default:
        }
	    log.Infof("action: ping_result | result: in_progress")
        err := queryResult(c.conn)

        // server did take the query 
        if err == nil {
            break 
        }
        c.conn.Close()
		time.Sleep(c.config.LoopPeriod)
    }
    winners, err := recvResult(c.conn)
    c.conn.Close()
        
    if err != nil {
        log.Errorf("action: recv_winners | result: fail | client_id: %v | error: %v",
            c.config.ID,
		    err,
		)
    } else {
	    log.Infof("action: query_winners | result: success | client_id: %v  | winners: %v", c.config.ID, winners)
    }
	log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
    c.shutdown <- true
}
