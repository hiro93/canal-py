from collections import namedtuple
from enum import Enum

from protobuf3.fields import (
    BytesField,
    StringField,
    Int32Field,
    MessageField,
    EnumField,
    Int64Field,
    BoolField
)
from protobuf3.message import Message


# com.alibaba.otter.canal.protocol
class Compression(Enum):
    COMPRESSIONCOMPATIBLEPROTO2 = 0
    NONE = 1
    ZLIB = 2
    GZIP = 3
    LZF = 4


class PacketType(Enum):
    # compatible
    PACKAGETYPECOMPATIBLEPROTO2 = 0
    HANDSHAKE = 1
    CLIENTAUTHENTICATION = 2
    ACK = 3
    SUBSCRIPTION = 4
    UNSUBSCRIPTION = 5
    GET = 6
    MESSAGES = 7
    CLIENTACK = 8
    # management part
    SHUTDOWN = 9
    # integration
    DUMP = 10
    HEARTBEAT = 11
    CLIENTROLLBACK = 12


class Packet(Message):
    magic_number = Int32Field(field_number=1, default=17,
                              oneof_id='magic_number_present')
    version = Int32Field(field_number=2, default=1,
                         oneof_id='version_present')
    type = EnumField(field_number=3, enum_cls=PacketType)
    compression = EnumField(field_number=4, enum_cls=Compression,
                            default=Compression.NONE,
                            oneof_id='compression_present')
    body = BytesField(field_number=5)


class HeartBeat(Message):
    send_timestamp = Int64Field(field_number=1)
    start_timestamp = Int64Field(field_number=2)


class Handshake(Message):
    communication_encoding = StringField(
        field_number=1, default='utf8',
        oneof_id='communication_encoding_present')
    seeds = BytesField(field_number=2)
    supported_compressions = EnumField(field_number=3, enum_cls=Compression)


class ClientAuth(Message):
    username = StringField(field_number=1)
    password = BytesField(field_number=2)
    # in seconds
    net_read_timeout = Int32Field(field_number=3, default=0,
                                  oneof_id='net_read_timeout_present')
    # in seconds
    net_write_timeout = Int32Field(field_number=4, default=0,
                                   oneof_id='net_write_timeout_present')
    destination = StringField(field_number=5)
    client_id = StringField(field_number=6)
    filter = StringField(field_number=7)
    start_timestamp = Int64Field(field_number=8)


class Ack(Message):
    error_code = Int32Field(field_number=1, default=0,
                            oneof_id='error_code_present')
    # if something like compression is not supported, erorr_message will tell about it.
    error_message = StringField(field_number=2)


class ClientAck(Message):
    destination = StringField(field_number=1)
    client_id = StringField(field_number=2)
    batch_id = Int64Field(field_number=3)


class Sub(Message):
    """subscription"""
    destination = StringField(field_number=1)
    client_id = StringField(field_number=2)
    filter = StringField(field_number=7)


class Unsub(Message):
    """Unsubscription"""
    destination = StringField(field_number=1)
    client_id = StringField(field_number=2)
    filter = StringField(field_number=7)


class Get(Message):
    """PullRequest"""
    destination = StringField(field_number=1)
    client_id = StringField(field_number=2)
    fetch_size = Int32Field(field_number=3)
    # 默认-1时代表不控制
    timeout = Int64Field(field_number=4, default=-1, oneof_id='timeout_present')
    # 数字类型，0:纳秒,1:毫秒,2:微秒,3:秒,4:分钟,5:小时,6:天
    unit = Int32Field(field_number=5, default=2, oneof_id='unit_present')
    # 是否自动ack
    auto_ack = BoolField(field_number=6, default=False, oneof_id='auto_ack_present')


class Messages(Message):
    batch_id = Int64Field(field_number=1)
    messages = BytesField(field_number=2, repeated=True)


class Dump(Message):
    """TBD when new packets are required"""
    journal = StringField(field_number=1)
    position = Int64Field(field_number=2)
    timestamp = Int64Field(field_number=3, default=0, oneof_id='timestamp_present')


class ClientRollback(Message):
    destination = StringField(field_number=1)
    client_id = StringField(field_number=2)
    batch_id = Int64Field(field_number=3)


# com.alibaba.otter.canal.protocol
class Type(Enum):
    """数据库类型"""
    TYPECOMPATIBLEPROTO2 = 0
    ORACLE = 1
    MYSQL = 2
    PGSQL = 3


class EntryType(Enum):
    ENTRYTYPECOMPATIBLEPROTO2 = 0
    TRANSACTIONBEGIN = 1
    ROWDATA = 2
    TRANSACTIONEND = 3
    # 心跳类型，内部使用，外部暂不可见，可忽略
    HEARTBEAT = 4
    GTIDLOG = 5


class EventType(Enum):
    EVENTTYPECOMPATIBLEPROTO2 = 0
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    CREATE = 4
    ALTER = 5
    ERASE = 6
    QUERY = 7
    TRUNCATE = 8
    RENAME = 9
    # CREATE INDEX
    CINDEX = 10
    DINDEX = 11
    GTID = 12
    # XA
    XACOMMIT = 13
    XAROLLBACK = 14
    # MASTER HEARTBEAT
    MHEARTBEAT = 15


class Pair(Message):
    """预留扩展"""
    key = StringField(field_number=1)
    value = StringField(field_number=2)

    def to_json(self): return {'key': self.key, 'value': self.value}


class Header(Message):
    # 协议的版本号
    version = Int32Field(field_number=1, default=1, oneof_id='version_present')
    # binlog/redolog 文件名
    logfileName = StringField(field_number=2)
    # binlog/redolog 文件的偏移位置
    logfileOffset = Int64Field(field_number=3)
    # 服务端serverId
    serverId = Int64Field(field_number=4)
    # 变更数据的编码
    serverenCode = StringField(field_number=5)
    # 变更数据的执行时间
    executeTime = Int64Field(field_number=6)
    # 变更数据的来源
    sourceType = EnumField(field_number=7, default=Type.MYSQL, enum_cls=Type, oneof_id='sourceType_present')
    # 变更数据的schemaname
    schemaName = StringField(field_number=8)
    # 变更数据的tablename
    tableName = StringField(field_number=9)
    # 每个event的长度
    eventLength = Int64Field(field_number=10)
    # 数据变更类型
    eventType = EnumField(field_number=11, default=EventType.UPDATE, enum_cls=EventType, oneof_id='eventType_present')
    # 预留扩展
    props = MessageField(field_number=12, repeated=True, message_cls=Pair)
    # 当前事务的gitd
    gtid = StringField(field_number=13)


class Entry(Message):
    # 协议头部信息
    header = MessageField(field_number=1, message_cls=Header)
    entryType = EnumField(field_number=2, default=EntryType.ROWDATA, enum_cls=EntryType, oneof_id='entryType_present')
    storeValue = BytesField(field_number=3)


class Column(Message):
    """每个字段的数据结构"""
    # 字段下标
    index = Int32Field(field_number=1)
    # 字段java中类型
    sqlType = Int32Field(field_number=2)
    # 字段名称(忽略大小写)，在mysql中是没有的
    name = StringField(field_number=3)
    # 是否是主键
    isKey = BoolField(field_number=4)
    # 如果EventType=UPDATE,用于标识这个字段值是否有修改
    updated = BoolField(field_number=5)
    # 标识是否为空
    isNull = BoolField(field_number=6, default=False)
    # 预留扩展
    props = MessageField(field_number=7, repeated=True, message_cls=Pair)
    # 字段值,timestamp,Datetime是一个时间格式的文本
    value = StringField(field_number=8)
    # 对应数据对象原始长度
    length = Int32Field(field_number=9)
    # 字段mysql类型
    mysqlType = StringField(field_number=10)

    def to_json(self):
        return {
            'index': self.index,
            'sqlType': self.sqlType,
            'name': self.name,
            'isKey': self.isKey,
            'updated': self.updated,
            'isNull': self.isNull,
            'props': [p.to_json() for p in self.props],
            'value': self.value,
            'length': self.length,
            'mysqlType': self.mysqlType
        }


class RowData(Message):
    # 字段信息，增量数据(修改前,删除前)
    beforeColumns = MessageField(field_number=1, repeated=True, message_cls=Column)
    # 字段信息，增量数据(修改后,新增后)
    afterColumns = MessageField(field_number=2, repeated=True, message_cls=Column)
    # 预留扩展
    props = MessageField(field_number=3, repeated=True, message_cls=Pair)


class RowChange(Message):
    """message row 每行变更数据的数据结构"""
    # tableId,由数据库产生
    tableId = Int64Field(field_number=1)
    # 数据变更类型
    eventType = EnumField(field_number=2, default=EventType.UPDATE, enum_cls=EventType, oneof_id='eventType_present')
    # 标识是否是ddl语句
    isDdl = BoolField(field_number=10, default=False, oneof_id='isDdl_present')
    # ddl/query的sql语句
    sql = StringField(field_number=11)
    # 一次数据库变更可能存在多行
    rowDatas = MessageField(field_number=12, repeated=True, message_cls=RowData)
    # 预留扩展
    props = MessageField(field_number=13, repeated=True, message_cls=Pair)
    # ddl/query的schemaName，会存在跨库ddl，需要保留执行ddl的当前schemaName
    ddlSchemaName = StringField(field_number=14, optional=True)


class TransactionBegin(Message):
    """开始事务的一些信息"""
    # 已废弃，请使用header里的executeTime
    executeTime = Int64Field(field_number=1)
    # 已废弃，Begin里不提供事务id
    transactionId = StringField(field_number=2)
    # 预留扩展
    props = MessageField(field_number=3, repeated=True, message_cls=Pair)
    # 执行的thread Id
    threadId = Int64Field(field_number=4)


class TransactionEnd(Message):
    """结束事务的一些信息"""
    # 已废弃，请使用header里的executeTime
    executeTime = Int64Field(field_number=1)
    # 事务号
    transactionId = StringField(field_number=2)
    # 预留扩展
    props = MessageField(field_number=3, repeated=True, message_cls=Pair)


ClientIdentity = namedtuple('ClientIdentity', 'destination, client_id')


class CanalException(Exception):
    pass
