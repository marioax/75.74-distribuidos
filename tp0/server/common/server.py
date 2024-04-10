import os
import socket
import signal
import logging 
import multiprocessing as mp

from . import protocol
from . import utils


class ShutdownInterrupt(Exception):
    def __init__(self, message=""):
        super().__init__(message)


class Server:

    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_id = int(os.getenv("SERVER_ID"))
        self._client_num = int(os.getenv("CLI_NUM"))

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
        ended_client_ids = manager.dict() 
        send_result_event = manager.Event()
        # ~
    
        bets_lock = manager.Lock() 

        cli_terminated_event = manager.Event()
        max_procs = self._client_num 
        client_procs = {} 

        try:
            while True:
                client_sock = self.__accept_new_connection()
                p_args = (client_sock, ended_client_ids, bets_lock, send_result_event, cli_terminated_event) 
                p = mp.Process(target=self._handle_client_connection, args=p_args)
                p.start()
                client_procs[p.pid] = p

                # if exceded max_procs blocks until a process terminates 
                if len(client_procs) >= max_procs: 
                    cli_terminated_event.wait()

                cli_terminated_event.clear()
                self.__join_procs(client_procs)

            

        except ShutdownInterrupt:
            logging.debug("[MAIN] received shutdown alert; cleaning up...")
        except Exception as e:
            logging.error(f"[MAIN] error: {e}")  
        finally: 
            self._server_socket.close()
            self.__join_procs(client_procs, terminate=True)
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


    def _handle_client_connection(self, 
                                  client_sock,
                                  ended_client_ids, 
                                  bets_lock, 
                                  send_result_event,
                                  termination_event):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            cli_id, mtype, payload = protocol.recv_msg(client_sock)

            if mtype == protocol.BET and payload:
                ended_client_ids.pop(cli_id, None) # starts sending again -> not ended
                
                # process bets until EOT is received
                while mtype != protocol.EOT:
                    bets = protocol.parse_bets(cli_id, payload)

                    if bets:
                        with bets_lock:
                            utils.store_bets(bets)
                        logging.info(f"[CLIENT] action: bets_stored | result: success | client_id: {cli_id} | number of bets: {len(bets)}")
                        protocol.send_ack(client_sock, self._server_id)

                    cli_id, mtype, payload = protocol.recv_msg(client_sock)

                logging.debug(f"[CLIENT] action: eot_received | result: success | client_id: {cli_id}")
                ended_client_ids[cli_id] = True 
                protocol.send_ack(client_sock, self._server_id)

                # `perform lottery`
                if len(ended_client_ids) == self._client_num:
                    logging.info(f"[CLIENT] action: lottery | result: success")
                    send_result_event.set()

            elif mtype == protocol.QWIN:
                logging.debug(f"[CLIENT] action: winners_query_received | result: success | client_id: {cli_id}")
                send_result_event.wait()
                self._send_winners(client_sock, cli_id, bets_lock)                    
                                                        
            termination_event.set()

        except OSError as e:
            logging.error(f"[CLIENT] action: receive_message | result: fail | error: {e}")
        except protocol.ProtocolError as e:
            logging.error(f"[CLIENT] action: receive_bets | result: fail | error: {e.message}")
        except ShutdownInterrupt:
            logging.debug("[CLIENT] received shutdown alert from parent; cleaning up")
        finally:
            client_sock.close()
 

    def _send_winners(self, client_sock, cli_id, bets_lock):
        winners = 0
        with bets_lock:
            for bet in utils.load_bets():
                if utils.has_won(bet) and bet.agency == cli_id:
                    winners += 1

        protocol.send_winners(client_sock, self._server_id, winners)
        logging.debug(f"[CLIENT] action: winners_sent | result: success | client_id: {cli_id}")
         

    def __join_procs(self, procs, terminate=False):
        joined = 0
        forced = 0

        for pid, p in list(procs.items()):
            if terminate and p.is_alive():
                p.terminate() # SIGTERM
                forced += 1 
            elif p.is_alive():
                    continue
            p.join()
            procs.pop(pid)
            joined += 1

        logging.debug(f"[MAIN] action: join_process | status: sucess | processes joined: {joined} | forced termination: {forced}")

