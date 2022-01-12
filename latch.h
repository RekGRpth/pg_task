#ifndef _LATCH_H_
#define _LATCH_H_

#include <signal.h>
#include <storage/latch.h>
#include <storage/proc.h>

/*
 * Bitmasks for events that may wake-up WaitLatch(), WaitLatchOrSocket(), or
 * WaitEventSetWait().
 */
#define WL_LATCH_SET		 (1 << 0)
#define WL_SOCKET_READABLE	 (1 << 1)
#define WL_SOCKET_WRITEABLE  (1 << 2)
#define WL_TIMEOUT			 (1 << 3)	/* not for WaitEventSetWait() */
#define WL_POSTMASTER_DEATH  (1 << 4)

typedef struct WaitEvent
{
	int			pos;			/* position in the event data structure */
	uint32		events;			/* triggered events */
	pgsocket	fd;				/* socket fd associated with event */
	void	   *user_data;		/* pointer provided in AddWaitEventToSet */
#ifdef WIN32
	bool		reset;			/* Is reset of the event required? */
#endif
} WaitEvent;

/* forward declaration to avoid exposing latch.c implementation details */
typedef struct WaitEventSet WaitEventSet;

/*
 * prototypes for functions in latch.c
 */
extern WaitEventSet *CreateWaitEventSet(MemoryContext context, int nevents);
extern void FreeWaitEventSet(WaitEventSet *set);
extern int AddWaitEventToSet(WaitEventSet *set, uint32 events, pgsocket fd,
				  Latch *latch, void *user_data);
extern void ModifyWaitEvent(WaitEventSet *set, int pos, uint32 events, Latch *latch);

extern int	WaitEventSetWait(WaitEventSet *set, long timeout, WaitEvent *occurred_events, int nevents);

#endif // _LATCH_H_
