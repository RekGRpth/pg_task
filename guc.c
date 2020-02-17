#include "include.h"

int set_config_option_my(const char *name, const char *value) {
    return set_config_option(name, value, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
}
