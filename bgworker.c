#include "include.h"

void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker) {
    BackgroundWorkerHandle *handle;
    if (!RegisterDynamicBackgroundWorker(worker, &handle)) E("!RegisterDynamicBackgroundWorker"); else {
        pid_t pid;
        switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
            case BGWH_NOT_YET_STARTED: E("WaitForBackgroundWorkerStartup == BGWH_NOT_YET_STARTED"); break;
            case BGWH_POSTMASTER_DIED: E("WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED"); break;
            case BGWH_STARTED: break;
            case BGWH_STOPPED: E("WaitForBackgroundWorkerStartup == BGWH_STOPPED");
        }
    }
    pfree(handle);
}
