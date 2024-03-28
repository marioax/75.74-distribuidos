import socket
import logging

from . import utils

EOP  = '\x00' # end of payload
EOB  = '\x01' # enf of bet
EOI  = '\x02' # enf of id
EOT  = '\x03' # enf of transmission 
BAT  = '\x04' # batch type message
QWIN = '\x05' # winners query message
RWIN = '\x06' # winners response message
ACK  = '\xff' # ack type message

SEP = '\n'   # field separator


class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def parse_bets(agency_id, buf) -> list[utils.Bet]:
    buf = buf.rstrip(EOB)  # remove trailing EOB
    bets_raw = buf.split(EOB) 
    bets = []

    for bet in bets_raw:
        bet_fields = bet.split(SEP)

        if len(bet_fields) != 5:
            raise ProtocolError("wrong payload format")

        name, lastname, dni, birth, number = bet_fields
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

def parse_msg(raw_msg) -> (str, str, str):
    cli_id, raw_msg = raw_msg.split(EOI)
    msg_type = raw_msg[0]
    payload = raw_msg[1:-1]
    return cli_id, msg_type, payload 


def recv(client_sock) -> (str, str, str):
    buf = b"" 

    while True:
        buf += client_sock.recv(1)
        if buf[-1] == ord(EOP):
            break
    return parse_msg(buf.decode('utf-8'))

def send_ack(client_sock):
    return client_sock.sendall((ACK + EOP).encode('latin-1'))


def send_winners(client_sock, winners: str):
    return client_sock.sendall((RWIN + winners + EOP).encode('latin-1')) 
