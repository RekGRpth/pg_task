#include "include.h"

static char *text_to_cstring_my(MemoryContextData *memoryContext, const text *t) {
    MemoryContextData *oldMemoryContext = MemoryContextSwitchTo(memoryContext);
    char *result = text_to_cstring(t);
    MemoryContextSwitchTo(oldMemoryContext);
    return result;
}

char *TextDatumGetCStringMy(MemoryContextData *memoryContext, Datum datum) {
    return datum ? text_to_cstring_my(memoryContext, (text *)DatumGetPointer(datum)) : NULL;
}
