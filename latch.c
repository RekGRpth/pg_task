#include "include.h"

int WaitLatchOrSocketMy(Latch *latch, WaitEvent *event, int wakeEvents, List **socket_data, long timeout, uint32 wait_event_info) {
    int ret = 0;
    WaitEventSet *set = CreateWaitEventSet(CurrentMemoryContext, 2 + list_length(*socket_data));
    if (list_length(*socket_data) > 0) {
        wakeEvents |= WL_SOCKET_MASK;
        wait_event_info = PG_WAIT_CLIENT;
    }
    if (wakeEvents & WL_TIMEOUT) Assert(timeout >= 0); else timeout = -1;
    if (wakeEvents & WL_LATCH_SET) AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, NULL);
    Assert(!IsUnderPostmaster || (wakeEvents & WL_EXIT_ON_PM_DEATH) || (wakeEvents & WL_POSTMASTER_DEATH)); /* Postmaster-managed callers must handle postmaster death somehow. */
    if ((wakeEvents & WL_POSTMASTER_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if ((wakeEvents & WL_EXIT_ON_PM_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if (wakeEvents & WL_SOCKET_MASK) for (ListCell *cell; *socket_data && (cell = list_head(*socket_data)); *socket_data = list_delete_first(*socket_data)) {
        SocketData *sd = lfirst(cell);
        AddWaitEventToSet(set, wakeEvents & WL_SOCKET_MASK, sd->fd, NULL, sd->user_data);
    }
    if (!WaitEventSetWait(set, timeout, event, 1, wait_event_info)) ret |= WL_TIMEOUT; else ret |= event->events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    FreeWaitEventSet(set);
    return ret;
}
