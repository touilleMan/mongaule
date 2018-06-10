import random
import struct
import bson


OP_REPLY = 1  # Reply to a client request. responseTo is set.
OP_MSG = 1000  # Generic msg command followed by a string.
OP_UPDATE = 2001  # Update document.
OP_INSERT = 2002  # Insert new document.
RESERVED = 2003  # Formerly used for OP_GET_BY_OID.
OP_QUERY = 2004  # Query a collection.
OP_GET_MORE = 2005  # Get more data from a query. See Cursors.
OP_DELETE = 2006  # Delete documents.
OP_KILL_CURSORS = 2007  # Notify database that the client has finished with the cursor.
OP_COMMAND = 2010  # Cluster internal protocol representing a command request.
OP_COMMANDREPLY = 2011  # Cluster internal protocol representing a reply to an OP_COMMAND.


class MongoWireProtocolError(Exception):
    pass


def parse_req_header(raw_data):
    # Header format:
    # int32   messageLength; // total message size, including this
    # int32   requestID;     // identifier for this message
    # int32   responseTo;    // requestID from the original request
    #                        //   (used in responses from db)
    # int32   opCode;        // request type - see table below
    message_length, request_id, response_to, opcode = struct.unpack_from('<iiii', raw_data)
    return {
        'message_length': message_length,
        'request_id': request_id,
        'response_to': response_to,
        'opcode': opcode
    }


def parse_req_op_query(raw_data):
    # OP_QUERY format:
    #   MsgHeader header;                 // standard message header
    #   int32     flags;                  // bit vector of query options.  See below for details.
    #   cstring   fullCollectionName ;    // "dbname.collectionname"
    #   int32     numberToSkip;           // number of documents to skip
    #   int32     numberToReturn;         // number of documents to return
    #                                     //  in the first OP_REPLY batch
    #   document  query;                  // query object.  See below for details.
    # [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields
    #                                     //  to return.  See below for details.
    flags, = struct.unpack_from('<i', raw_data, offset=16)
    offset = 20
    cstring = b''
    while raw_data[offset] != 0:
        cstring += raw_data[offset].to_bytes(1, 'little')
        offset += 1
    offset += 1
    number_to_skip, number_to_return = struct.unpack_from('<ii', raw_data, offset=offset)
    offset += 8
    doc_size, = struct.unpack_from('<i', raw_data, offset=offset)
    raw_query = raw_data[offset:offset + doc_size]
    offset += doc_size
    query = bson.BSON.decode(raw_query)
    raw_return_field_selector = raw_data[offset:]
    if raw_return_field_selector:
        return_field_selector = bson.BSON.decode(raw_return_field_selector)
    else:
        return_field_selector = {}
    return {
        'flags': flags,
        'cstring': cstring,
        'number_to_skip': number_to_skip,
        'number_to_return': number_to_return,
        'query': query,
        'return_field_selector': return_field_selector
    }


def wrap_resp_with_header(request, raw_body):
    request_id = random.randint(2**31 + 1, 2**31)
    return struct.pack('<iiii', request_id, request['request_id'], OP_REPLY)


REPLY_FLAG_CURSOR_NOT_FOUND = 1
REPLY_FLAG_QUERY_FAILURE = 2
REPLY_FLAG_SHARD_CONFIG_STALE = 4
REPLY_FLAG_AWAIT_CAPABLE = 8


def generate_resp(flags=0, cursor_id=0, starting_from=0, documents=()):
    # OP_REPLY format:
    # MsgHeader header;         // standard message header
    # int32     responseFlags;  // bit vector - see details below
    # int64     cursorID;       // cursor id if client needs to do get more's
    # int32     startingFrom;   // where in the cursor this reply is starting
    # int32     numberReturned; // number of documents in the reply
    # document* documents;      // documents
    flags |= REPLY_FLAG_AWAIT_CAPABLE  # Always supported since mongodb 1.6
    number_returned = len(documents)
    raw_body = struct.pack('<iIii', flags, cursor_id, starting_from, number_returned)
    raw_body += b''.join([bson.BSON.encode(doc) for doc in documents])
    return wrap_resp_with_header(raw_body)


def handle_request(raw_req):
    header = parse_req_header(raw_req)
    opcode = header['opcode']
    if opcode == OP_REPLY:
        raise MongoWireProtocolError('Opcode OP_REPLY is not allowed for request.')
    elif opcode == OP_MSG:
        return generate_resp(REPLY_FLAG_QUERY_FAILURE, documents={'$err': 'Not supported.'})
    elif opcode == OP_UPDATE:
        pass
    elif opcode == OP_INSERT:
        pass
    elif opcode == RESERVED:
        pass
    elif opcode == OP_QUERY:
        pass
    elif opcode == OP_GET_MORE:
        pass
    elif opcode == OP_DELETE:
        pass
    elif opcode == OP_KILL_CURSORS:
        pass
    elif opcode == OP_COMMAND:
        return generate_resp(REPLY_FLAG_QUERY_FAILURE, documents={'$err': 'Not supported.'})
    elif opcode == OP_COMMANDREPLY:
        return generate_resp(REPLY_FLAG_QUERY_FAILURE, documents={'$err': 'Not supported.'})
    else:
        raise MongoWireProtocolError('Unknown request opcode `%s`.' % opcode)
