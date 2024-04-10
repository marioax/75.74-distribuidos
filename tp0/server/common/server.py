import os
import socket
import logging
import signal
import multiprocessing as mp

from . import protocol
from . import utils

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_id = int(os.getenv("SERVER_ID"))

        self._client_socket = None

        # for winner query logic
        self._client_num = int(os.getenv("CLI_NUM"))
        self._ended_client_ids = set() 
        self._clients_pending = {} # clients that are waiting response

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # setup signal handler
        def sig_handler(sig, frame):
            raise ShutdownInterrupt 

        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)
 
        manager = mp.Manager()

        # for winner query logic
        client_ids = manager.dict()
        ended_client_ids = manager.dict() 
        send_result_event = manager.Event()
        # ~

        bets_lock = manager.Lock() 
        
        client_procs = {}

        try:
            while True:
                # calculate results in main process to not take any more bets 
                client_sock = self.__accept_new_connection()
                p_args = (client_sock, client_ids, ended_client_ids, bets_lock, send_result_event) 
                p = mp.Process(target=self.__handle_client_connection, args=p_args)
                p.start()
                client_procs[p.pid] = p
                self.__join_procs(client_procs, force=False)
            

        except ShutdownInterrupt:
            logging.info("[MAIN] received shutdown alert; cleaning up...")
        except Exception as e:
            logging.error(f"error: {e}")  
        finally: 
            self._server_socket.close()

            # send SIGTERM to child processes
            self.__join_procs(client_procs, force=True)

            manager.shutdown()
            logging.debug("[MAIN] exiting...")  


    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('[MAIN] action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'[MAIN] action: accept_connections | result: success | ip: {addr[0]}')
        return c


    def __handle_client_connection(self, 
                                   client_sock,
                                   client_ids, 
                                   ended_client_ids, 
                                   bets_lock, 
                                   send_result_event):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            cli_id, mtype, payload = protocol.recv_msg(client_sock)

            if mtype == protocol.BET and payload:
                self._ended_client_ids.discard(cli_id) # not ended if sends bets
                
                # process bets until EOT is received
                while mtype != protocol.EOT:
                    bets = protocol.parse_bets(cli_id, payload)

                    if bets:
                        utils.store_bets(bets)
                        logging.info(f"action: bets_stored | result: success | client_id: {cli_id} | number of bets: {len(bets)}")
                        protocol.send_ack(client_sock, self._server_id)

                    cli_id, mtype, payload = protocol.recv_msg(client_sock)

                logging.info(f"action: eot_received | result: success | client_id: {cli_id}")
                self._ended_client_ids.add(cli_id)
                protocol.send_ack(client_sock, self._server_id)

            elif mtype == protocol.QWIN:
                logging.info(f"action: winners_query_received | result: success | client_id: {cli_id}")
                self._clients_pending[cli_id] = client_sock
                client_sock = None

                # perform lottery if all clients have ended and are waiting the results
                if len(self._ended_client_ids) == len(self._clients_pending) == self._client_num:
                    self.__announce_winners()                    
                                                        

        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        except protocol.ProtocolError as e:
            logging.error(f"action: receive_bets | result: fail | error: {e.message}")
        except ShutdownInterrupt:
            logging.debug("[CLIENT] received shutdown alert from parent; cleaning up")
        finally:
            if client_sock:
                client_sock.close()
 

    def __send_winners(self, cli_id, client_sock, bets_lock):
        winners = 0
        with bets_lock:
            for bet in utils.load_bets():
                if utils.has_won(bet) and str(bet.agency) == cli_id:
                    winners += 1

        protocol.send_winners(client_sock, str(winners))
        logging.info(f"[CLIENT] action: winners_sent | result: success | client_id: {cli_id}")
         

    def __join_procs(self, procs, force=False):
        joined = 0
        forced = 0

        for pid, p in list(procs.items()):
            if p.is_alive():
                if force:
                    p.terminate() # SIGTERM
                    forced += 1 
                else:
                    continue
            p.join()
            procs.pop(pid)
            joined += 1

        logging.debug(f"[MAIN] action: join_process | status: sucess | processes joined: {joined} | forced termination: {forced}")


    def __announce_winners(self):
        winners = {i : 0 for i in self._clients_pending}

        for bet in utils.load_bets():
            if bet.agency in winners and utils.has_won(bet):
                winners[bet.agency] += 1

        logging.info(f"action: lottery | result: success")
        self._ended_client_ids.clear()

        for cli_id, sock in self._clients_pending.items():
            protocol.send_winners(sock, self._server_id, winners[cli_id])
            sock.close()

        self._clients_pending.clear()

