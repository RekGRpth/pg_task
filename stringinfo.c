#include "include.h"

void initStringInfoMy(MemoryContext memoryContext, StringInfoData *buf) {
    buf->maxlen = 1024;
    buf->data = MemoryContextAlloc(memoryContext, buf->maxlen);
    resetStringInfo(buf);
}
