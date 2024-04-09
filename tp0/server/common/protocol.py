import socket
import logging

from . import utils

'''
    Message structure:
              ___ payload length (4 byte)
             /
    |  T  |  L  |   P   |
        \            \____ payload (X byte)
         \__ message type (1 byte)
'''

HSIZE = 5 # header size (T + L)
TSIZE = 1 # type size
LSIZE = 4 # length size

# Types
ACK = 0x00
BET = 0x01

# BET type
BET_FIELDS = 6
SEP = ','   # field separator
EOB = '\n'  # enf of bet


class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def recvmsg(client_sock) -> (int, str):
    bets = []
    header = b"" 
    payload = b""
    
    # read header
    while len(header) < HSIZE:
        header += client_sock.recv(HSIZE - len(header))

    mtype = header[0] # change if type size is < 1 byte
    toread = int.from_bytes(header[TSIZE:TSIZE + LSIZE], byteorder="big")
    
    # read payload 
    while len(payload) < toread:
       payload += client_sock.recv(toread - len(payload))
        
    return mtype, payload.decode('utf-8')


def sendmsg(client_sock, mtype: int, payload: str = ""):
    mtype = bytes([mtype])
    length = len(payload).to_bytes(byteorder="big", length=LSIZE)

    msg = mtype + length + payload.encode('utf-8')
    client_sock.sendall(msg)


def recv_bets(client_sock) -> list[utils.Bet]:
    mtype, payload = recvmsg(client_sock)
    if mtype == BET:
        return parse_bets(payload)
    return []


def send_ack(client_sock):
    sendmsg(client_sock, ACK)


def parse_bets(payload):
    payload = payload.rstrip(EOB)  # remove trailing EOB
    bets_raw = payload.split(EOB) 
    bets = []

    for bet in bets_raw:
        bet_fields = bet.split(SEP)

        if len(bet_fields) != BET_FIELDS:
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


