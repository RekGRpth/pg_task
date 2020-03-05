#include "queue.h"

int queue_count(queue_t *h) {
    int count = 0;
    queue_each(h, q) count++;
    return count;
}
