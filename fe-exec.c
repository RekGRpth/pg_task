#include "include.h"

static const char *SPI_gettype_my(Oid oid) {
    const char *result;
    HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oid));
    if (!HeapTupleIsValid(typeTuple)) E("SPI_ERROR_TYPUNKNOWN");
    result = NameStr(((Form_pg_type)GETSTRUCT(typeTuple))->typname);
    ReleaseSysCache(typeTuple);
    return result;
}

const char *PQftypeMy(const PGresult *res, int column_number) {
    return SPI_gettype_my(ObjectIdGetDatum(PQftype(res, column_number)));
}
