#include "include.h"

static char *text_to_cstring_my(MemoryContext memoryContext, const text *t) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(memoryContext);
    char *result = text_to_cstring(t);
    MemoryContextSwitchTo(oldMemoryContext);
    return result;
}

char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum) {
    return datum ? text_to_cstring_my(memoryContext, (text *)DatumGetPointer(datum)) : NULL;
}
