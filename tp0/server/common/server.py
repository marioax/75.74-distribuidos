import socket
import logging
import signal

from . import protocol
from . import utils


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_socket = None

        # for winner query logic
        self._client_ids = set()
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

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
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
            cli_id, msg_type, payload = protocol.recv(client_sock)
            self._client_ids.add(cli_id)

            if msg_type == protocol.BAT:
                bets = protocol.parse_bets(cli_id, payload)
                utils.store_bets(bets)
                logging.info(f"action: bets_stored | result: success | client_id: {cli_id} | number of bets: {len(bets)}")
                protocol.send_ack(client_sock)

            elif msg_type == protocol.EOT:
                logging.info(f"action: eot_received | result: success | client_id: {cli_id}")
                self._ended_client_ids.add(cli_id)
                protocol.send_ack(client_sock)

            elif msg_type == protocol.QWIN:
                logging.info(f"action: winners_query_received | result: success | client_id: {cli_id}")
                self._clients_pending[cli_id] = client_sock
                protocol.send_ack(client_sock)

                if self._client_ids != self._ended_client_ids:
                    # dont close the sock; respond later
                    return
                self.__announce_winners()

            client_sock.close()
            #msg = client_sock.recv(1024).rstrip().decode('utf-8')
            #addr = client_sock.getpeername()
            #logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            #client_sock.send("{}\n".format(msg).encode('utf-8'))
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        except protocol.ProtocolError as e:
            logging.error(f"action: receive_bets | result: fail | error: {e.message}")

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

        for bet in utils.load_bets():
            if utils.has_won(bet):
                winners[str(bet.agency)] += 1

        logging.info(f"action: lottery | result: success")

        for cli_id, sock in self._clients_pending.items():
            protocol.send_winners(sock, str(winners[cli_id]))

