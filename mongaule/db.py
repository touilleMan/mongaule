import threading
from mongomock import MongoClient

from mongaule.protocol import *


class MonGauleDB:
    def __init__(self):
        self._lock = threading.Lock()
        self._db = MongoClient()
        self._cursors = {}

    def execute(self, raw_req):
        try:
            self._lock.acquire()
            print('=====================>>>>>enter', threading.get_ident())
            return self._execute(raw_req)
        finally:
            self._lock.release()
            print('<<<<<<=================leave', threading.get_ident())

    def _execute(self, raw_req):
        header = parse_req_header(raw_req)
        opcode = header['opcode']
        print('Received %s' % header)
        if opcode == OP_REPLY:
            raise MongoWireProtocolError('Opcode OP_REPLY is not allowed for request.')
        elif opcode == OP_MSG:
            return generate_resp(REPLY_FLAG_QUERY_FAILURE, documents={'$err': 'Not supported.'})
        elif opcode == OP_UPDATE:
            return None
        elif opcode == OP_INSERT:
            return None
        elif opcode == RESERVED:
            raise MongoWireProtocolError('Dunno what to do with RESERVED opcode...')
        elif opcode == OP_QUERY:
            op_query = parse_req_op_query(raw_req)
            import pdb; pdb.set_trace()
            docs = []
            return generate_resp(documents=docs)
        elif opcode == OP_GET_MORE:
            docs = []
            return generate_resp(documents=docs)
        elif opcode == OP_DELETE:
            return None
        elif opcode == OP_KILL_CURSORS:
            return None
        elif opcode == OP_COMMAND:
            return generate_resp(REPLY_FLAG_QUERY_FAILURE, documents={'$err': 'Not supported.'})
        elif opcode == OP_COMMANDREPLY:
            return generate_resp(REPLY_FLAG_QUERY_FAILURE, documents={'$err': 'Not supported.'})
        else:
            raise MongoWireProtocolError('Unknown request opcode `%s`.' % opcode)
