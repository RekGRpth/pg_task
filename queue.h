#ifndef _QUEUE_H_
#define _QUEUE_H_

typedef struct queue_s queue_t;

struct queue_s {
    queue_t *prev;
    queue_t *next;
};

int queue_count(queue_t *queue);

#define queue_init(q) ({ (q)->prev = (q); (q)->next = (q); })
#define queue_head(q) ((q)->next)
#define queue_remove(q) ({ (q)->next->prev = (q)->prev; (q)->prev->next = (q)->next; queue_init(q); })
#define queue_insert_tail(h, q) ({ (q)->prev = (h)->prev; (q)->prev->next = (q); (q)->next = (h); (h)->prev = (q); })
#define queue_data(q, t, f) ((t *)((char *)(q) - offsetof(t, f)))
#define queue_each(h, q) for (queue_t *(q) = (h)->next; (q) != (h); (q) = (q)->next)
#define queue_empty(q) ((q) == (q)->next)

#define pointer_t queue_t
#define pointer_init queue_init
#define queue_get_pointer queue_head
#define pointer_remove queue_remove
#define queue_put_pointer queue_insert_tail
#define pointer_data queue_data

#endif // _QUEUE_H_
