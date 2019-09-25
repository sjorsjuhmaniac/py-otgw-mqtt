from opentherm import OTGWClient, ConnectionException
import logging
import select
import socket

log = logging.getLogger(__name__)

class OTGWTcpClient(OTGWClient):
    r"""
    A skeleton for a TCP-client based
    """

    def __init__(self, listener, **kwargs):
        super(OTGWTcpClient, self).__init__(listener)
        self._host = kwargs['host']
        self._port = int(kwargs['port'])
        self._socket = None

    def open(self, connect_timeout=3):
        r"""
        Open the connection to the OTGW
        """
        try:
          log.info('Connecting to %s:%s', self._host, self._port)
          self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
          # https://docs.python.org/3/library/socket.html#notes-on-socket-timeouts
          self._socket.settimeout(connect_timeout) # Timeout for connect only
          self._socket.connect((self._host, self._port))
        except Exception as e:
            log.warning("Failed to open socket: %s", str(e))
            raise ConnectionException()
        log.info('Connected to %s:%s', self._host, self._port)
        self._socket.setblocking(False) # Non-blocking for all other socket operations

    def close(self):
        r"""
        Close the connection to the OTGW
        """
        try:
            log.debug('Closing socket connection')
            self._socket.close()
        except socket.error as e:
            log.warning("Failed to close socket: %s", str(e))

    def write(self, data):
        r"""
        Write data to the OTGW

        Packet inspection with wireshark of the original otmonitor learned
        that the command must only be terminated with a \r and not with \r\n
        """
        try:
            log.debug("Writing to socket: %s", data.encode('ascii', 'ignore'))
            self._socket.sendall(data.encode('ascii', 'ignore'))
        except socket.error as e:
            log.warning("Failed to write to socket: %s", str(e))
            raise ConnectionException()

    def read(self, timeout):
        r"""
        Read data from the OTGW
        """
        try:
            readable, writable, exceptional = select.select([self._socket], [], [self._socket], timeout)
            if readable:
                data = self._socket.recv(128).decode('ascii', 'ignore')
                if data:
                    return data
                else:
                    log.error('Client %s disconnected', self._socket.getpeername())
                    raise ConnectionException()
            if exceptional:
                log.error('Client %s encountered exceptional condition', self._socket.getpeername())
                raise ConnectionException()
        except socket.error as e:
            log.warn("Failed to read from socket: %s", str(e))
            raise ConnectionException()
