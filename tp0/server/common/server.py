import os
import socket
import logging
import signal
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
            cli_id, msg_type, payload = protocol.recv(client_sock)

            if msg_type == protocol.BAT:
                bets = protocol.parse_bets(cli_id, payload)
                with bets_lock:
                    utils.store_bets(bets)
                logging.info(f"[CLIENT] action: bets_stored | result: success | client_id: {cli_id} | number of bets: {len(bets)}")
                client_ids[cli_id] = None
                protocol.send_ack(client_sock)

            elif msg_type == protocol.EOT:
                logging.info(f"[CLIENT] action: eot_received | result: success | client_id: {cli_id}")

                if cli_id in client_ids:
                    ended_client_ids[cli_id] = None

                # All clients finished, send the results
                # side note: since are dict proxies, using == operator will not compare the dicts
                if dict(client_ids) == dict(ended_client_ids):
                    client_ids.clear()
                    ended_client_ids.clear()
                    send_result_event.set()
                    logging.info(f"[CLIENT] action: lottery | result: success")

                protocol.send_ack(client_sock)

            elif msg_type == protocol.QWIN:
                logging.info(f"[CLIENT] action: winners_query_received | result: success | client_id: {cli_id}")

                if send_result_event.is_set():
                    protocol.send_ack(client_sock)
                    self.__send_winners(cli_id, client_sock, bets_lock)
                else:
                    protocol.send_nack(client_sock)


        except OSError as e:
            logging.error(f"[CLIENT] action: receive_message | result: fail | error: {e}")
        except protocol.ProtocolError as e:
            logging.error(f"[CLIENT] action: receive_bets | result: fail | error: {e.message}")
        except ShutdownInterrupt:
            logging.debug("[CLIENT] received shutdown alert from parent; cleaning up")
        finally:
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

