#ifndef _QUEUE_H_
#define _QUEUE_H_

typedef struct queue_t {
    struct queue_t *prev;
    struct queue_t *next;
} queue_t;

int queue_count(queue_t *queue);

#define queue_init(q) (q)->prev = (q)->next = q

#define queue_empty(h) (h == (h)->prev)

#define queue_insert_head(h, x) \
    do { \
        (x)->next = (h)->next; \
        (x)->next->prev = x; \
        (x)->prev = h; \
        (h)->next = x; \
    } while (0)

#define queue_insert_after queue_insert_head

#define queue_insert_tail(h, x) \
    do { \
        (x)->prev = (h)->prev; \
        (x)->prev->next = x; \
        (x)->next = h; \
        (h)->prev = x; \
    } while (0)

#define queue_head(h) (h)->next

#define queue_last(h) (h)->prev

#define queue_sentinel(h) (h)

#define queue_next(q) (q)->next

#define queue_prev(q) (q)->prev

#define queue_remove(x) \
    do { \
        (x)->next->prev = (x)->prev; \
        (x)->prev->next = (x)->next; \
        (x)->prev = NULL; \
        (x)->next = NULL; \
    } while (0)

#define queue_data(q, t, o) (t *)((char *)q - offsetof(t, o))

#define queue_each(h, q) for (queue_t *(q) = queue_head(h); (q) != queue_sentinel(h); (q) = queue_next(q))

#endif // _QUEUE_H_
