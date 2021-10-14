#include "include.h"

const char *PQftypeMy(Oid oid) {
    switch (oid) {
        case BOOLOID: return "bool";
        case BYTEAOID: return "bytea";
        case CHAROID: return "char";
        case NAMEOID: return "name";
        case INT8OID: return "int8";
        case INT2OID: return "int2";
        case INT2VECTOROID: return "int2vector";
        case INT4OID: return "int4";
        case REGPROCOID: return "regproc";
        case TEXTOID: return "text";
        case OIDOID: return "oid";
        case TIDOID: return "tid";
        case XIDOID: return "xid";
        case CIDOID: return "cid";
        case OIDVECTOROID: return "oidvector";
        case JSONOID: return "json";
        case XMLOID: return "xml";
#if (PG_VERSION_NUM >= 140000)
        case PG_NODE_TREEOID: return "pg_node_tree";
        case PG_NDISTINCTOID: return "pg_ndistinct";
        case PG_DEPENDENCIESOID: return "pg_dependencies";
        case PG_MCV_LISTOID: return "pg_mcv_list";
        case PG_DDL_COMMANDOID: return "pg_ddl_command";
#else
        case PGNODETREEOID: return "pgnodetree";
        case PGNDISTINCTOID: return "pgndistinct";
        case PGDEPENDENCIESOID: return "pgdependencies";
        case PGMCVLISTOID: return "pgmcvlist";
        case PGDDLCOMMANDOID: return "pgddlcommand";
#endif
#if (PG_VERSION_NUM >= 130000)
        case XID8OID: return "xid8";
#endif
        case POINTOID: return "point";
        case LSEGOID: return "lseg";
        case PATHOID: return "path";
        case BOXOID: return "box";
        case POLYGONOID: return "polygon";
        case LINEOID: return "line";
        case FLOAT4OID: return "float4";
        case FLOAT8OID: return "float8";
        case UNKNOWNOID: return "unknown";
        case CIRCLEOID: return "circle";
#if (PG_VERSION_NUM >= 140000)
        case MONEYOID: return "money";
#endif
        case CASHOID: return "cash";
        case MACADDROID: return "macaddr";
        case INETOID: return "inet";
        case CIDROID: return "cidr";
        case MACADDR8OID: return "macaddr8";
        case ACLITEMOID: return "aclitem";
        case BPCHAROID: return "bpchar";
        case VARCHAROID: return "varchar";
        case DATEOID: return "date";
        case TIMEOID: return "time";
        case TIMESTAMPOID: return "timestamp";
        case TIMESTAMPTZOID: return "timestamptz";
        case INTERVALOID: return "interval";
        case TIMETZOID: return "timetz";
        case BITOID: return "bit";
        case VARBITOID: return "varbit";
        case NUMERICOID: return "numeric";
        case REFCURSOROID: return "refcursor";
        case REGPROCEDUREOID: return "regprocedure";
        case REGOPEROID: return "regoper";
        case REGOPERATOROID: return "regoperator";
        case REGCLASSOID: return "regclass";
#if (PG_VERSION_NUM >= 130000)
        case REGCOLLATIONOID: return "regcollation";
#endif
        case REGTYPEOID: return "regtype";
        case REGROLEOID: return "regrole";
        case REGNAMESPACEOID: return "regnamespace";
        case UUIDOID: return "uuid";
#if (PG_VERSION_NUM >= 140000)
        case PG_LSNOID: return "pg_lsn";
#endif
        case LSNOID: return "lsn";
        case TSVECTOROID: return "tsvector";
        case GTSVECTOROID: return "gtsvector";
        case TSQUERYOID: return "tsquery";
        case REGCONFIGOID: return "regconfig";
        case REGDICTIONARYOID: return "regdictionary";
        case JSONBOID: return "jsonb";
        case JSONPATHOID: return "jsonpath";
        case TXID_SNAPSHOTOID: return "txid_snapshot";
#if (PG_VERSION_NUM >= 130000)
        case PG_SNAPSHOTOID: return "pg_snapshot";
#endif
        case INT4RANGEOID: return "int4range";
        case NUMRANGEOID: return "numrange";
        case TSRANGEOID: return "tsrange";
        case TSTZRANGEOID: return "tstzrange";
        case DATERANGEOID: return "daterange";
        case INT8RANGEOID: return "int8range";
        case RECORDOID: return "record";
        case RECORDARRAYOID: return "recordarray";
        case CSTRINGOID: return "cstring";
        case ANYOID: return "any";
        case ANYARRAYOID: return "anyarray";
        case VOIDOID: return "void";
        case TRIGGEROID: return "trigger";
#if (PG_VERSION_NUM >= 140000)
        case EVENT_TRIGGEROID: return "event_trigger";
#else
        case EVTTRIGGEROID: return "evttrigger";
#endif
        case LANGUAGE_HANDLEROID: return "language_handler";
        case INTERNALOID: return "internal";
#if (PG_VERSION_NUM >= 130000)
#else
        case OPAQUEOID: return "opaque";
#endif
        case ANYELEMENTOID: return "anyelement";
        case ANYNONARRAYOID: return "anynonarray";
        case ANYENUMOID: return "anyenum";
        case FDW_HANDLEROID: return "fdw_handler";
        case INDEX_AM_HANDLEROID: return "index_am_handler";
        case TSM_HANDLEROID: return "tsm_handler";
        case TABLE_AM_HANDLEROID: return "table_am_handler";
        case ANYRANGEOID: return "anyrange";
#if (PG_VERSION_NUM >= 130000)
        case ANYCOMPATIBLEOID: return "anycompatible";
        case ANYCOMPATIBLEARRAYOID: return "anycompatiblearray";
        case ANYCOMPATIBLENONARRAYOID: return "anycompatiblenonarray";
        case ANYCOMPATIBLERANGEOID: return "anycompatiblerange";
#endif
        case BOOLARRAYOID: return "boolarray";
        case BYTEAARRAYOID: return "byteaarray";
        case CHARARRAYOID: return "chararray";
        case NAMEARRAYOID: return "namearray";
        case INT8ARRAYOID: return "int8array";
        case INT2ARRAYOID: return "int2array";
        case INT2VECTORARRAYOID: return "int2vectorarray";
        case INT4ARRAYOID: return "int4array";
        case REGPROCARRAYOID: return "regprocarray";
        case TEXTARRAYOID: return "textarray";
        case OIDARRAYOID: return "oidarray";
        case TIDARRAYOID: return "tidarray";
        case XIDARRAYOID: return "xidarray";
        case CIDARRAYOID: return "cidarray";
        case OIDVECTORARRAYOID: return "oidvectorarray";
        case JSONARRAYOID: return "jsonarray";
        case XMLARRAYOID: return "xmlarray";
#if (PG_VERSION_NUM >= 130000)
        case XID8ARRAYOID: return "xid8array";
#endif
        case POINTARRAYOID: return "pointarray";
        case LSEGARRAYOID: return "lsegarray";
        case PATHARRAYOID: return "patharray";
        case BOXARRAYOID: return "boxarray";
        case POLYGONARRAYOID: return "polygonarray";
        case LINEARRAYOID: return "linearray";
        case FLOAT4ARRAYOID: return "float4array";
        case FLOAT8ARRAYOID: return "float8array";
        case CIRCLEARRAYOID: return "circlearray";
        case MONEYARRAYOID: return "moneyarray";
        case MACADDRARRAYOID: return "macaddrarray";
        case INETARRAYOID: return "inetarray";
        case CIDRARRAYOID: return "cidrarray";
        case MACADDR8ARRAYOID: return "macaddr8array";
        case ACLITEMARRAYOID: return "aclitemarray";
        case BPCHARARRAYOID: return "bpchararray";
        case VARCHARARRAYOID: return "varchararray";
        case DATEARRAYOID: return "datearray";
        case TIMEARRAYOID: return "timearray";
        case TIMESTAMPARRAYOID: return "timestamparray";
        case TIMESTAMPTZARRAYOID: return "timestamptzarray";
        case INTERVALARRAYOID: return "intervalarray";
        case TIMETZARRAYOID: return "timetzarray";
        case BITARRAYOID: return "bitarray";
        case VARBITARRAYOID: return "varbitarray";
        case NUMERICARRAYOID: return "numericarray";
        case REFCURSORARRAYOID: return "refcursorarray";
        case REGPROCEDUREARRAYOID: return "regprocedurearray";
        case REGOPERARRAYOID: return "regoperarray";
        case REGOPERATORARRAYOID: return "regoperatorarray";
        case REGCLASSARRAYOID: return "regclassarray";
#if (PG_VERSION_NUM >= 130000)
        case REGCOLLATIONARRAYOID: return "regcollationarray";
#endif
        case REGTYPEARRAYOID: return "regtypearray";
        case REGROLEARRAYOID: return "regrolearray";
        case REGNAMESPACEARRAYOID: return "regnamespacearray";
        case UUIDARRAYOID: return "uuidarray";
        case PG_LSNARRAYOID: return "pg_lsnarray";
        case TSVECTORARRAYOID: return "tsvectorarray";
        case GTSVECTORARRAYOID: return "gtsvectorarray";
        case TSQUERYARRAYOID: return "tsqueryarray";
        case REGCONFIGARRAYOID: return "regconfigarray";
        case REGDICTIONARYARRAYOID: return "regdictionaryarray";
        case JSONBARRAYOID: return "jsonbarray";
        case JSONPATHARRAYOID: return "jsonpatharray";
        case TXID_SNAPSHOTARRAYOID: return "txid_snapshotarray";
#if (PG_VERSION_NUM >= 130000)
        case PG_SNAPSHOTARRAYOID: return "pg_snapshotarray";
#endif
        case INT4RANGEARRAYOID: return "int4rangearray";
        case NUMRANGEARRAYOID: return "numrangearray";
        case TSRANGEARRAYOID: return "tsrangearray";
        case TSTZRANGEARRAYOID: return "tstzrangearray";
        case DATERANGEARRAYOID: return "daterangearray";
        case INT8RANGEARRAYOID: return "int8rangearray";
        case CSTRINGARRAYOID: return "cstringarray";
        default: return NULL;
    }
}
