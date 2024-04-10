import os
import socket
import signal
import logging
import multiprocessing as mp

from . import protocol
from . import utils

mp_logging = mp.log_to_stderr()    
mp_logging.setLevel(logging.INFO)

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
 
        mp.set_start_method('forkserver') # using the default (`fork`) pool.terminate() hangs
        manager = mp.Manager()

        # for winner query logic
        ended_client_ids = manager.dict() 
        send_result_event = manager.Event()
        # ~

        bets_lock = manager.Lock() 


        try:
            # context manager ensures of calling terminate and join in all workers
            with mp.Pool(processes=None) as pool:
                while True:
                    client_sock = self.__accept_new_connection()
                    p_args = (client_sock, ended_client_ids, bets_lock, send_result_event) 
                    pool.apply_async(self._handle_client_connection, args=p_args)
            

        except ShutdownInterrupt:
            mp_logging.debug("[MAIN] received shutdown alert; cleaning up...")
        except Exception as e:
            mp_logging.error(f"[MAIN] error: {e}")  
        finally: 
            self._server_socket.close()
            manager.shutdown()
            mp_logging.debug("[MAIN] exiting...")  


    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        mp_logging.info('[MAIN] action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        mp_logging.info(f'[MAIN] action: accept_connections | result: success | ip: {addr[0]}')
        return c


    def _handle_client_connection(self, 
                                  client_sock,
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
                ended_client_ids.pop(cli_id, None) # starts sending again -> not ended
                
                # process bets until EOT is received
                while mtype != protocol.EOT:
                    bets = protocol.parse_bets(cli_id, payload)

                    if bets:
                        with bets_lock:
                            utils.store_bets(bets)
                        mp_logging.info(f"[CLIENT] action: bets_stored | result: success | client_id: {cli_id} | number of bets: {len(bets)}")
                        protocol.send_ack(client_sock, self._server_id)

                    cli_id, mtype, payload = protocol.recv_msg(client_sock)

                mp_logging.debug(f"[CLIENT] action: eot_received | result: success | client_id: {cli_id}")
                ended_client_ids[cli_id] = True 
                protocol.send_ack(client_sock, self._server_id)

                # `perform lottery`
                if len(ended_client_ids) == self._client_num:
                    mp_logging.info(f"[CLIENT] action: lottery | result: success")
                    send_result_event.set()

            elif mtype == protocol.QWIN:
                mp_logging.debug(f"[CLIENT] action: winners_query_received | result: success | client_id: {cli_id}")
                send_result_event.wait()
                self.__send_winners(client_sock, cli_id, bets_lock)                    
                                                        

        except OSError as e:
            mp_logging.error(f"[CLIENT] action: receive_message | result: fail | error: {e}")
        except protocol.ProtocolError as e:
            mp_logging.error(f"[CLIENT] action: receive_bets | result: fail | error: {e.message}")
        except ShutdownInterrupt:
            mp_logging.debug("[CLIENT] received shutdown alert from parent; cleaning up")
        finally:
            client_sock.close()
 

    def _send_winners(self, client_sock, cli_id, bets_lock):
        winners = 0
        with bets_lock:
            for bet in utils.load_bets():
                if utils.has_won(bet) and str(bet.agency) == cli_id:
                    winners += 1

        protocol.send_winners(client_sock, self._server_id, winners)
        mp_logging.debug(f"[CLIENT] action: winners_sent | result: success | client_id: {cli_id}")
         

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

        mp_logging.debug(f"[MAIN] action: join_process | status: sucess | processes joined: {joined} | forced termination: {forced}")

