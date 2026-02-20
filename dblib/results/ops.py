from dblib import result_pb2 as rslt
from util.sql_parse import get_sql_operation_keyword

_KEYWORD_TO_OP_TYPE = {
    "SELECT": rslt.OpType.READ,
    "INSERT": rslt.OpType.INSERT,
    "UPDATE": rslt.OpType.UPDATE,
    "DELETE": rslt.OpType.UPDATE,
    "WITH": rslt.OpType.READ,
}


def get_op_type_from_sql(sql: str) -> rslt.OpType:
    """Determine operation type from a SQL statement."""
    keyword = get_sql_operation_keyword(sql)
    if not keyword:
        return rslt.OpType.UNSPECIFIED
    return _KEYWORD_TO_OP_TYPE.get(keyword, rslt.OpType.UNSPECIFIED)


def str_to_op_type(op_str: str) -> rslt.OpType:
    """Convert operation type enum name to value."""
    try:
        return rslt.OpType[op_str.upper().strip()]
    except KeyError:
        return rslt.OpType.UNSPECIFIED
