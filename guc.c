#include "include.h"

void SetConfigOptionMy(const char *name, const char *value) {
    set_config_option(name, value, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
}
