import socket
import logging

from . import utils


'''
    message structure:

          __ peer id (1 byte; up to 256 peers)
         /             ___ payload length (4 byte)
        /            /
    |  ID  |  T  |  L  |   P   |
               \            \____ payload (X byte)
                \__ message type (1 byte)
'''

HSIZE = 6  # header size (ID + T + L)
IDSIZE = 1 # peer id size
TSIZE = 1  # type size
LSIZE = 4  # length size

# Types
ACK = 0x00
BET = 0x01
EOT = 0x02 # end of transmision


# BET type
BET_FIELDS = 5
SEP = ','   # field separator
EOB = '\n'  # enf of bet


class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def recv_msg(client_sock) -> (int, int, str):
    bets = []
    header = b"" 
    payload = b""
    
    # read header
    while len(header) < HSIZE:
        header += client_sock.recv(HSIZE - len(header))

    peer_id = header[0] # change if client id size is > 1 byte
    mtype = header[1] # change if type size is > 1 byte
    toread = int.from_bytes(header[IDSIZE + TSIZE:HSIZE], byteorder="big")
    
    # read payload 
    while len(payload) < toread:
       payload += client_sock.recv(toread - len(payload))
        
    return peer_id, mtype, payload.decode('utf-8')


def send_msg(client_sock, peer_id: int, mtype: int, payload: str = ""):
    peer_id = bytes([peer_id])
    mtype = bytes([mtype])
    length = len(payload).to_bytes(byteorder="big", length=LSIZE)

    msg = peer_id + mtype + length + payload.encode('utf-8')
    client_sock.sendall(msg)


def send_ack(client_sock, peer_id):
    send_msg(client_sock, peer_id, ACK)


def parse_bets(agency_id, payload):
    payload = payload.rstrip(EOB)  # remove trailing EOB
    bets_raw = payload.split(EOB) 
    bets = []

    for bet in bets_raw:
        bet_fields = bet.split(SEP)

        if len(bet_fields) != BET_FIELDS:
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

