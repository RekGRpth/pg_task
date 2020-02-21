#include "include.h"

const char *PQftypeMy(const PGresult *res, int column_number) {
    return SPI_gettype_my(ObjectIdGetDatum(PQftype(res, column_number)));
}
