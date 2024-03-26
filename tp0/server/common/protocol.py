import socket
import logging

from . import utils

EOP = '\x00' # end of payload
EOB = '\x01' # enf of bet
SEP = '\n'   # field separator
ACK = "ACK"


class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def parse_bets(buf):
    buf = buf.rstrip(EOB)  # remove trailing EOB
    bets_raw = buf.split(EOB) 
    bets = []

    for bet in bets_raw:
        bet_fields = bet.split(SEP)

        if len(bet_fields) != 6:
            raise ProtocolError("wrong payload format")

        agency_id, name, lastname, dni, birth, number = bet_fields
        bets.append(
            utils.Bet(
                agency_id,
                name,
                lastname,
                dni,
                birth,
                number
            )
        )
    return bets


def recv_bets(client_sock) -> list[utils.Bet]:
    bets = []
    buf = "" 

    while not buf or buf[-1] != EOP:
       buf += client_sock.recv(1).decode('utf-8')
        
    return parse_bets(buf[:-1])


def send_bets_ack(client_sock):
    return client_sock.sendall((ACK + EOP).encode('utf-8'))
