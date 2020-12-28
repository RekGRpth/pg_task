#include "include.h"

/*
 * Functions for manipulating advisory locks
 *
 * We make use of the locktag fields as follows:
 *
 *	field1: MyDatabaseId ... ensures locks are local to each database
 *	field2: first of 2 int4 keys, or high-order half of an int8 key
 *	field3: second of 2 int4 keys, or low-order half of an int8 key
 *	field4: 1 if using an int8 key, 2 if using 2 int4 keys
 */
#define SET_LOCKTAG_INT64(tag, key64) \
	SET_LOCKTAG_ADVISORY(tag, \
						 MyDatabaseId, \
						 (uint32) ((key64) >> 32), \
						 (uint32) (key64), \
						 1)
#define SET_LOCKTAG_INT32(tag, key1, key2) \
	SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, key1, key2, 2)

#if (PG_VERSION_NUM < 130000)
static void
PreventAdvisoryLocksInParallelMode(void)
{
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot use advisory locks during a parallel operation")));
}
#endif

bool pg_try_advisory_lock_int8_my(int64 key) {
    LOCKTAG tag;
#if (PG_VERSION_NUM < 130000)
    PreventAdvisoryLocksInParallelMode();
#endif
    SET_LOCKTAG_INT64(tag, key);
    return LockAcquire(&tag, ExclusiveLock, true, true) != LOCKACQUIRE_NOT_AVAIL;
}

bool pg_advisory_unlock_int8_my(int64 key) {
    LOCKTAG tag;
#if (PG_VERSION_NUM < 130000)
    PreventAdvisoryLocksInParallelMode();
#endif
    SET_LOCKTAG_INT64(tag, key);
    return LockHeldByMe(&tag, ExclusiveLock) && LockRelease(&tag, ExclusiveLock, true);
}

bool pg_try_advisory_lock_int4_my(int32 key1, int32 key2) {
    LOCKTAG tag;
#if (PG_VERSION_NUM < 130000)
    PreventAdvisoryLocksInParallelMode();
#endif
    SET_LOCKTAG_INT32(tag, key1, key2);
    return LockAcquire(&tag, ExclusiveLock, true, true) != LOCKACQUIRE_NOT_AVAIL;
}

bool pg_advisory_unlock_int4_my(int32 key1, int32 key2) {
    LOCKTAG tag;
#if (PG_VERSION_NUM < 130000)
    PreventAdvisoryLocksInParallelMode();
#endif
    SET_LOCKTAG_INT32(tag, key1, key2);
    return LockHeldByMe(&tag, ExclusiveLock) && LockRelease(&tag, ExclusiveLock, true);
}
