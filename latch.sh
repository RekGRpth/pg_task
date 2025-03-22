#!/bin/sh -ex

cat latch.h | pcregrep -M \
-e '(?s)^typedef struct WaitEvent.*?} WaitEvent;' \
-e '(?s)^typedef struct WaitEventSet.*?WaitEventSet;' \
-e '(?s)^extern int\N+AddWaitEventToSet\(.*?\);' \
-e '(?s)^extern int\N+WaitEventSetWait\(.*?\);' \
-e '(?s)^extern void\N+FreeWaitEventSet\(.*?\);' \
-e '(?s)^extern void\N+InitializeLatchSupportMy\(.*?\);' \
-e '(?s)^extern WaitEventSet\N+\*CreateWaitEventSet\(.*?\);'
