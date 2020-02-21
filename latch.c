#include "include.h"

int WaitLatchOrSocketMy(Latch *latch, WaitEvent *event, int wakeEvents, queue_t *task_queue, long timeout, uint32 wait_event_info) {
    int ret = 0;
    WaitEventSet *set = CreateWaitEventSet(CurrentMemoryContext, 2 + queue_count(task_queue));
    if (wakeEvents & WL_TIMEOUT) Assert(timeout >= 0); else timeout = -1;
    if (wakeEvents & WL_LATCH_SET) AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, NULL);
    Assert(!IsUnderPostmaster || (wakeEvents & WL_EXIT_ON_PM_DEATH) || (wakeEvents & WL_POSTMASTER_DEATH));
    if ((wakeEvents & WL_POSTMASTER_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    if ((wakeEvents & WL_EXIT_ON_PM_DEATH) && IsUnderPostmaster) AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
    queue_each(task_queue, queue) {
        Task *task = pointer_data(queue, Task, pointer);
        AddWaitEventToSet(set, task->event.events, task->event.fd, NULL, task);
    }
    if (!WaitEventSetWait(set, timeout, event, 1, wait_event_info)) ret |= WL_TIMEOUT; else ret |= event->events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    FreeWaitEventSet(set);
    return ret;
}
