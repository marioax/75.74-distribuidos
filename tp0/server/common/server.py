import socket
import logging
import signal
import os

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
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        try:
            while True:
                self._client_socket = self.__accept_new_connection()
                self.__handle_client_connection(self._client_socket)
                self._client_socket = None

        except KeyboardInterrupt:
            logging.info("received shutdown alert; cleaning up")
            if self._client_socket:
                self._client_socket.close()
            self._server_socket.close()

    def __handle_client_connection(self, client_sock):
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
        finally:
            if client_sock:
                client_sock.close()


    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def __announce_winners(self):
        winners = {i : 0 for i in self._clients_pending}
        print(f"[CLIENT] {winners}")

        for bet in utils.load_bets():
            if bet.agency in winners and utils.has_won(bet):
                winners[bet.agency] += 1

        logging.info(f"action: lottery | result: success")
        self._ended_client_ids.clear()

        for cli_id, sock in self._clients_pending.items():
            protocol.send_winners(sock, self._server_id, winners[cli_id])
            sock.close()

        self._clients_pending.clear()

