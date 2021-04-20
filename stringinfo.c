#include "include.h"

void initStringInfoMy(MemoryContextData *memoryContext, StringInfoData *buf) {
    MemoryContextData *oldMemoryContext = MemoryContextSwitchTo(memoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}
