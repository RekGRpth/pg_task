#include "include.h"

const char *PQftypeMy(const PGresult *res, int column_number) {
    HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(PQftype(res, column_number)));
    const char *result;
    if (!HeapTupleIsValid(typeTuple)) E("SPI_ERROR_TYPUNKNOWN");
    result = NameStr(((Form_pg_type)GETSTRUCT(typeTuple))->typname);
    ReleaseSysCache(typeTuple);
    return result;
}
