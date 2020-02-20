#include "include.h"

int WaitLatchOrSocketMy(Latch *latch, WaitEvent *event, int wakeEvents, List **list, long timeout, uint32 wait_event_info) {
    int ret = 0;
    WaitEventSet *set = CreateWaitEventSet(CurrentMemoryContext, 2 + list_length(*list));
    if (list_length(*list) > 0) {
        L("CurrentMemoryContext == TopMemoryContext = %s", CurrentMemoryContext == TopMemoryContext ? "true" : "false");
        wakeEvents |= WL_SOCKET_MASK;
//        wait_event_info = PG_WAIT_CLIENT;
    }
    if (wakeEvents & WL_TIMEOUT) Assert(timeout >= 0); else timeout = -1;
    if (wakeEvents & WL_LATCH_SET) AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, NULL);
    Assert(!IsUnderPostmaster || (wakeEvents & WL_EXIT_ON_PM_DEATH) || (wakeEvents & WL_POSTMASTER_DEATH)); /* Postmaster-managed callers must handle postmaster death somehow. */
    if ((wakeEvents & WL_POSTMASTER_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if ((wakeEvents & WL_EXIT_ON_PM_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if (wakeEvents & WL_SOCKET_MASK) {
        for (ListCell *cell = list_head(*list); cell; cell = lnext(cell)) {
            Context *context = lfirst(cell);
            L("context = %p", context);
            AddWaitEventToSet(set, wakeEvents & WL_SOCKET_MASK, context->fd, NULL, context);
        }
        list_free(*list);
        *list = NIL;
    }
    if (!WaitEventSetWait(set, timeout, event, 1, wait_event_info)) ret |= WL_TIMEOUT; else ret |= event->events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    FreeWaitEventSet(set);
    return ret;
}
