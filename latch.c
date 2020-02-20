#include "include.h"

int WaitLatchOrSocketMy(Latch *latch, void **data, int wakeEvents, queue_t *fd_queue, long timeout, uint32 wait_event_info) {
    int ret = 0, count = queue_count(fd_queue);
    WaitEvent event;
    WaitEventSet *set = CreateWaitEventSet(CurrentMemoryContext, 2 + count);
    if (count > 0) {
        wakeEvents |= WL_SOCKET_MASK;
//        wait_event_info = PG_WAIT_CLIENT;
    }
    if (wakeEvents & WL_TIMEOUT) Assert(timeout >= 0); else timeout = -1;
    if (wakeEvents & WL_LATCH_SET) AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, NULL);
    Assert(!IsUnderPostmaster || (wakeEvents & WL_EXIT_ON_PM_DEATH) || (wakeEvents & WL_POSTMASTER_DEATH));
    if ((wakeEvents & WL_POSTMASTER_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if ((wakeEvents & WL_EXIT_ON_PM_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if (wakeEvents & WL_SOCKET_MASK) {
        queue_each(fd_queue, queue) {
            context_t *context = pointer_data(queue, context_t, pointer);
            AddWaitEventToSet(set, wakeEvents & context->wakeEvents, context->fd, NULL, context);
        }
    }
    if (!WaitEventSetWait(set, timeout, &event, 1, wait_event_info)) ret |= WL_TIMEOUT; else {
        ret |= event.events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
        if (data) *data = event.user_data;
        if (data && *data) {
            context_t *context = *data;
            pointer_remove(&context->pointer);
        }
        if (ret & WL_LATCH_SET) L("WL_LATCH_SET");
        if (ret & WL_SOCKET_READABLE) L("WL_SOCKET_READABLE");
        if (ret & WL_SOCKET_WRITEABLE) L("WL_SOCKET_WRITEABLE");
        if (ret & WL_TIMEOUT) L("WL_TIMEOUT");
        if (ret & WL_POSTMASTER_DEATH) L("WL_POSTMASTER_DEATH");
        if (ret & WL_EXIT_ON_PM_DEATH) L("WL_EXIT_ON_PM_DEATH");
        if (ret & WL_SOCKET_CONNECTED) L("WL_SOCKET_CONNECTED");
    }
    FreeWaitEventSet(set);
    return ret;
}
