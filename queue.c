#include "queue.h"

int queue_count(queue_t *queue) {
    int count = 0;
    if (queue_empty(queue)) return count;
    queue_each(queue, _) count++;
    return count;
}
