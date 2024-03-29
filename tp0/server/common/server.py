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
        clients_pending = manager.dict() # clients that are waiting response
        send_result_event = manager.Event()
        # ~

        bets_lock = manager.Lock() 
        
        ann_args = (clients_pending, bets_lock, send_result_event) 
        announcer = mp.Process(target=self.__announce_winners_handler, args=ann_args)

        client_procs = {}

        try:
            announcer.start()

            while True:
                # calculate results in main process to not take any more bets 
                client_sock = self.__accept_new_connection()
                p_args = (client_sock, client_ids, ended_client_ids, clients_pending, bets_lock, send_result_event) 
                p = mp.Process(target=self.__handle_client_connection, args=p_args)
                client_procs[p.pid] = p
                p.start()
                self.__join_finished(client_procs)
            

        except ShutdownInterrupt:
            logging.info("[MAIN] received shutdown alert; cleaning up...")
        except Exception as e:
            logging.error(f"error: {e}")  
        finally: 

            self._server_socket.close()

            for pid, proc in client_procs.items():
                if proc.is_alive():
                    os.kill(pid, signal.SIGTERM)
                proc.join()

            # send SIGTERM to child processes
            os.kill(announcer.pid, signal.SIGTERM)
            announcer.join()

            # close sockets that are waiting for server response
            for sock in clients_pending:
                sock.close()

            manager.shutdown()
            logging.debug("[MAIN] exiting...")  



    def __handle_client_connection(self, 
                                   client_sock,
                                   client_ids, 
                                   ended_client_ids, 
                                   clients_pending, 
                                   bets_lock, 
                                   send_result_event):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            cli_id, msg_type, payload = protocol.recv(client_sock)
            client_ids[cli_id] = None

            if msg_type == protocol.BAT:
                bets = protocol.parse_bets(cli_id, payload)
                with bets_lock:
                    utils.store_bets(bets)
                logging.info(f"[CLIENT] action: bets_stored | result: success | client_id: {cli_id} | number of bets: {len(bets)}")
                protocol.send_ack(client_sock)

            elif msg_type == protocol.EOT:
                logging.info(f"[CLIENT] action: eot_received | result: success | client_id: {cli_id}")
                ended_client_ids[cli_id] = None
                protocol.send_ack(client_sock)

            elif msg_type == protocol.QWIN:
                logging.info(f"[CLIENT] action: winners_query_received | result: success | client_id: {cli_id}")
                protocol.send_ack(client_sock)
                clients_pending[cli_id] = client_sock

                # No more bets will be received.
                # side note: since are dict proxies, using == operator will not compare the dicts
                if dict(client_ids) == dict(ended_client_ids):
                    client_ids.clear()
                    ended_client_ids.clear()
                    send_result_event.set()

                # dont close the sock; respond later
                return

            #msg = client_sock.recv(1024).rstrip().decode('utf-8')
            #addr = client_sock.getpeername()
            #logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            #client_sock.send("{}\n".format(msg).encode('utf-8'))
        except OSError as e:
            logging.error(f"[CLIENT] action: receive_message | result: fail | error: {e}")
        except protocol.ProtocolError as e:
            logging.error(f"[CLIENT] action: receive_bets | result: fail | error: {e.message}")
        except ShutdownInterrupt:
            logging.debug("[CLIENT] received shutdown alert from parent; cleaning up")
        finally:
            client_sock.close()
 

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


    def __announce_winners_handler(self, clients_pending, bets_lock, send_result_event):
        try:
            while True:
                send_result_event.wait()
                send_result_event.clear()
                logging.info(f"[ANNOUNCER] action: lottery | result: in_progress")

                winners = {i : 0 for i in clients_pending.keys()}
                
                with bets_lock:
                    for bet in utils.load_bets():
                        if utils.has_won(bet):
                            winners[str(bet.agency)] += 1

                logging.info(f"[ANNOUNCER] action: lottery | result: success")

                for cli_id, sock in list(clients_pending.items()):
                    protocol.send_winners(sock, str(winners[cli_id]))
                    sock.close()
                    clients_pending.pop(cli_id)


        except ShutdownInterrupt:
            logging.debug("[ANNOUNCER] received shutdown alert from parent; cleaning up")
        except Exception as e:
            logging.error(f"[ANNOUNCER] action: lottery | result: error | message: {e}")

        
    def __join_finished(self, processes):
        joined = 0

        for pid, p in list(processes.items()):
            if not p.is_alive():
                p.join()
                processes.pop(pid)
                joined += 1
            
        logging.debug(f"[MAIN] action: join_process | status: sucess | processes joined: {joined}")

