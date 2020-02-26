#include "include.h"

int WaitLatchOrSocketMy(Latch *latch, WaitEvent *event, queue_t *work_queue, long timeout, uint32 wait_event_info) {
    int ret = 0;
    WaitEventSet *set = CreateWaitEventSet(CurrentMemoryContext, 2 + queue_count(work_queue));
    if (event->events & WL_TIMEOUT) Assert(timeout >= 0); else timeout = -1;
    if (event->events & WL_LATCH_SET) AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, NULL);
    Assert(!IsUnderPostmaster || (event->events & WL_EXIT_ON_PM_DEATH) || (event->events & WL_POSTMASTER_DEATH));
    if ((event->events & WL_POSTMASTER_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if ((event->events & WL_EXIT_ON_PM_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
    queue_each(work_queue, queue) {
        Task *task = queue_data(queue, Task, queue);
        AddWaitEventToSet(set, task->events & WL_SOCKET_MASK, task->fd, NULL, task);
    }
    if (!WaitEventSetWait(set, timeout, event, 1, wait_event_info)) ret |= WL_TIMEOUT; else ret |= event->events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    FreeWaitEventSet(set);
    return ret;
}
