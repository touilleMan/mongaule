import os
import contextlib
from pymongo.pool import SocketInfo

from mongaule.db import MonGauleDB


db = MonGauleDB()


class FakeSocket:
    """
    Only implement methods used in pymongo, if you need more, tough shit...
    """

    def __init__(self, db):
        self._db = db
        self._received_msg = None

    def sendall(self, msg):
        assert self._received_msg is None, 'Answer already set, must call `recv`.'
        self._received_msg = self._db.execute(msg)

    def recv(self, bufsize):
        msg = self._received_msg
        assert msg is not None, 'No answer available, is `sendall` been called ?'
        self._received_msg = None
        return msg

    def close(self):
        print('Call close')
        pass

    def fileno(self):
        print('Call fileno')
        return

    def connect(host):
        print('Call connect')
        pass

    def setsockopt(*args, **kwargs):
        print('Call setsockopt')
        pass

    def settimeout(*args, **kwargs):
        print('Call settimeout')
        pass

    def getpeercert(self):
        print('Call getpeercert')
        pass


class FakeSocketPool:
    def __init__(self, address, options, handshake=True):
        """
        :Parameters:
          - `address`: a (hostname, port) tuple
          - `options`: a PoolOptions instance
          - `handshake`: whether to call ismaster for each new SocketInfo
        """
        # self._db = MonGauleDB()
        self._db = db
        self.active_sockets = 0

        # TODO: useful attributes ?
        self.pool_id = 0
        self.pid = os.getpid()
        self.address = address
        self.opts = options
        self.handshake = handshake

    def reset(self):
        self.active_sockets = 0

    def remove_stale_sockets(self):
        pass

    def connect(self):
        """Connect to Mongo and return a new SocketInfo.

        Can raise ConnectionFailure or CertificateError.

        Note that the pool does not keep a reference to the socket -- you
        must call return_socket() when you're done with it.
        """
        return SocketInfo(FakeSocket(self._db), self, None, self.address)

    @contextlib.contextmanager
    def get_socket(self, all_credentials, checkout=False):
        """Get a socket from the pool. Use with a "with" statement.

        Returns a :class:`SocketInfo` object wrapping a connected
        :class:`socket.socket`.

        This method should always be used in a with-statement::

            with pool.get_socket(credentials, checkout) as socket_info:
                socket_info.send_message(msg)
                data = socket_info.receive_message(op_code, request_id)

        The socket is logged in or out as needed to match ``all_credentials``
        using the correct authentication mechanism for the server's wire
        protocol version.

        Can raise ConnectionFailure or OperationFailure.

        :Parameters:
          - `all_credentials`: dict, maps auth source to MongoCredential.
          - `checkout` (optional): keep socket checked out.
        """
        self.active_sockets += 1
        yield SocketInfo(FakeSocket(self._db), self, None, self.address)

    def return_socket(self, sock_info):
        self.active_sockets -= 1
