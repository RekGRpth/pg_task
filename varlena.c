#include "include.h"

static char *text_to_cstring_my(MemoryContext memoryContext, const text *t) {
    text *tunpacked = pg_detoast_datum_packed(unconstify(text *, t));
    int len = VARSIZE_ANY_EXHDR(tunpacked);
    char *result = (char *)MemoryContextAlloc(memoryContext, len + 1);
    memcpy(result, VARDATA_ANY(tunpacked), len);
    result[len] = '\0';
    if (tunpacked != t) pfree(tunpacked);
    return result;
}

char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum) {
    return datum ? text_to_cstring_my(memoryContext, (text *)DatumGetPointer(datum)) : NULL;
}
