import asyncio
import struct

import canal_protocol as pb


class CanalConnector(object):
    __writer: asyncio.StreamWriter
    __reader: asyncio.StreamReader

    def __init__(self, address=('localhost', 11111), destination='example'):
        self.address = address
        self.client_identity = pb.ClientIdentity(
            destination,
            client_id='1001'  # magic
        )
        self.filter = ''    # 过滤需要监听的表名

        self.idle_timeout = 60 * 60 * 1000  # ???

        self.__reader = None
        self.__writer = None

    @asyncio.coroutine
    def connect(self):
        if self.__writer:
            return
        # open connection
        self.__reader, self.__writer = yield from asyncio.open_connection(*self.address)

        # <- handshake
        pack = yield from read_next_packet(self.__reader)
        if not pack.type == pb.PacketType.HANDSHAKE:
            raise pb.CanalException

        # -> auth
        ca = pb.ClientAuth()
        ca.destination = self.client_identity.destination
        ca.client_id = self.client_identity.client_id
        ca.net_read_timeout = self.idle_timeout
        ca.net_write_timeout = self.idle_timeout
        # ca.filter = self.filter

        # ca.username = ''
        # ca.password = ''
        # ca.net_read_timeout = self.idle_timeout
        # ca.net_write_timeout = self.idle_timeout
        pack = pb.Packet()
        pack.type = pb.PacketType.CLIENTAUTHENTICATION
        pack.body = ca.encode_to_bytes()
        yield from write_with_header(self.__writer, pack)

        # <- ACK
        yield from read_next_packet(self.__reader, ensure_ack=True)

    @asyncio.coroutine
    def disconnect(self):
        if not self.__writer:
            return
        yield from self.rollback(0)
        self.__writer.close()
        # yield from self.__writer.wait_closed()
        self.__writer = None
        self.__reader = None

    @asyncio.coroutine
    def subscribe(self, f):
        ca = pb.Sub()
        ca.destination = self.client_identity.destination
        ca.client_id = self.client_identity.client_id
        ca.filter = f

        pack = pb.Packet()
        pack.type = pb.PacketType.SUBSCRIPTION
        pack.body = ca.encode_to_bytes()

        yield from write_with_header(self.__writer, pack)
        yield from read_next_packet(self.__reader, ensure_ack=True)

    @asyncio.coroutine
    def unsubscribe(self):
        ca = pb.Unsub()
        ca.destination = self.client_identity.destination
        ca.client_id = self.client_identity.client_id
        ca.filter = self.filter

        pack = pb.Packet()
        pack.type = pb.PacketType.UNSUBSCRIPTION
        pack.body = ca.encode_to_bytes()

        yield from write_with_header(self.__writer, pack)
        yield from read_next_packet(self.__reader, ensure_ack=True)

    @asyncio.coroutine
    def get(self, fetch_size=1000) -> pb.Messages:
        msg = yield from self.get_without_ack(fetch_size)
        yield from self.ack(batch_id=msg.batch_id)

    @asyncio.coroutine
    def get_without_ack(self, fetch_size=100) -> pb.Messages:
        ca = pb.Get()
        ca.destination = self.client_identity.destination
        ca.client_id = self.client_identity.client_id
        ca.fetch_size = fetch_size
        ca.timeout = -1
        ca.unit = -1
        ca.auto_ack = False

        pack = pb.Packet()
        pack.type = pb.PacketType.GET
        pack.body = ca.encode_to_bytes()

        yield from write_with_header(self.__writer, pack)
        msg = yield from receive_messages(self.__reader)
        return msg

    @asyncio.coroutine
    def ack(self, batch_id):
        ca = pb.ClientAck()
        ca.destination = self.client_identity.destination
        ca.client_id = self.client_identity.client_id
        ca.batch_id = batch_id

        pack = pb.Packet()
        pack.type = pb.PacketType.CLIENTACK
        pack.body = ca.encode_to_bytes()

        yield from write_with_header(self.__writer, pack)

    @asyncio.coroutine
    def rollback(self, batch_id):
        ca = pb.ClientRollback()
        ca.destination = self.client_identity.destination
        ca.client_id = self.client_identity.client_id
        ca.batch_id = batch_id

        pack = pb.Packet()
        pack.type = pb.PacketType.CLIENTROLLBACK
        pack.body = ca.encode_to_bytes()

        yield from write_with_header(self.__writer, pack)


@asyncio.coroutine
def write_with_header(writer: asyncio.StreamWriter, packet: pb.Packet):
    tpl = struct.Struct('>I')
    data = packet.encode_to_bytes()
    writer.write(tpl.pack(len(data)))
    writer.write(data)
    yield from writer.drain()


@asyncio.coroutine
def read_next_packet(reader: asyncio.StreamReader, ensure_ack=False) -> pb.Packet:
    tpl = struct.Struct('>I')
    data = yield from reader.read(tpl.size)
    data_len = tpl.unpack(data)[0]
    data = yield from reader.read(data_len)
    pack = pb.Packet.create_from_bytes(data)

    if ensure_ack:
        if pack.type != pb.PacketType.ACK:
            raise pb.CanalException('unexpected packet type when ack is expected')

        ack = pb.Ack.create_from_bytes(pack.body)
        if ack.error_code > 0:
            raise pb.CanalException(ack.error_message)

    return pack


@asyncio.coroutine
def receive_messages(reader: asyncio.StreamReader) -> pb.Messages:
    """
    接收 canal-server 返回的消息体
    :param reader:
    :return:
    """
    pack = yield from read_next_packet(reader)
    if pack.type == pb.PacketType.MESSAGES:
        messages = pb.Messages.create_from_bytes(pack.body)
        return messages
    elif pack.type == pb.PacketType.ACK:
        ack = pb.Ack.create_from_bytes(pack.body)
        raise pb.CanalException('something goes wrong with reason: {msg}'.format(msg=ack.error_message))
    else:
        raise pb.CanalException('unexpected packet type: {type}'.format(type=pack.type))


def print_messages(messages: pb.Messages):
    """
    业务接口
    :param messages:
    :return:
    """
    for entry in messages.messages:
        entry = pb.Entry.create_from_bytes(entry)
        header: pb.Header = entry.header
        print(f'=== binlog[{header.logfileName}:{header.logfileOffset}], '
              f'table[{header.schemaName}.{header.tableName}] '
              f'executeTime={header.executeTime} gtid({header.gtid}) ===')

        # 事务
        if entry.entryType == pb.EntryType.TRANSACTIONBEGIN:
            tx = pb.TransactionBegin.create_from_bytes(entry.storeValue)
            print(f'TRANSACTIONBEGIN {tx.transactionId}')
        elif entry.entryType == pb.EntryType.TRANSACTIONEND:
            tx = pb.TransactionEnd.create_from_bytes(entry.storeValue)
            print(f'TRANSACTIONEND {tx.transactionId}')
        # UPDATE, INSERT, DELETE...
        elif entry.entryType == pb.EntryType.ROWDATA:
            tx = pb.RowChange.create_from_bytes(entry.storeValue)
            print(
                f'{tx.eventType} table {tx.tableId}, isDdl={tx.isDdl}, sql={tx.sql}, ddlSchemaName={tx.ddlSchemaName}')
            for row in tx.rowDatas:
                print('Before Datas')
                beforeColumns = row.beforeColumns
                for col in beforeColumns:
                    print(col.to_json())

                print('After Datas')
                afterColumns = row.afterColumns
                for col in afterColumns:
                    print(col.to_json())

        print('================================')


if __name__ == '__main__':
    @asyncio.coroutine
    def start():
        conn = CanalConnector(address=('dev.site', 11111))
        try:
            yield from conn.connect()
            yield from conn.rollback(0)
            # yield from conn.subscribe('.*\\\\..*')
            while True:
                msg = yield from conn.get_without_ack()
                msg: pb.Messages
                if msg.batch_id == -1 or not msg.messages:
                    print('no more data, have a relaxation')
                    yield from asyncio.sleep(3)
                    continue
                else:
                    print_messages(msg)
                yield from conn.ack(msg.batch_id)
        finally:
            yield from conn.disconnect()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(start())
    loop.close()
