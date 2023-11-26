#include "include.h"

extern Task task;
static emit_log_hook_type emit_log_hook_prev = NULL;

void task_execute(void) {
    bool count = false;
    bool insert = false;
    char completionTag[COMPLETION_TAG_BUFSIZE];
    int rc = SPI_execute(task.input, false, 0);
    const char *tagname = SPI_result_code_string(rc) + sizeof("SPI_OK_") - 1;
    switch (rc) {
        case SPI_ERROR_ARGUMENT: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_ARGUMENT"))); break;
        case SPI_ERROR_COPY: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_COPY"))); break;
        case SPI_ERROR_OPUNKNOWN: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_OPUNKNOWN"))); break;
        case SPI_ERROR_TRANSACTION: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_TRANSACTION"))); break;
        case SPI_OK_DELETE: count = true; break;
        case SPI_OK_DELETE_RETURNING: count = true; break;
        case SPI_OK_INSERT: count = true; insert = true; break;
        case SPI_OK_INSERT_RETURNING: count = true; insert = true; break;
        case SPI_OK_SELECT: count = true; task.skip = 1; break;
        case SPI_OK_UPDATE: count = true; break;
        case SPI_OK_UPDATE_RETURNING: count = true; break;
    }
    elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, tagname);
    if (SPI_tuptable) for (uint64 row = 0; row < SPI_processed; row++) {
        task.skip = 1;
        if (!task.output.data) initStringInfoMy(&task.output);
        if (task.header && !row && SPI_tuptable->tupdesc->natts > 1) {
            if (task.output.len) appendStringInfoString(&task.output, "\n");
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                if (col > 1) appendStringInfoChar(&task.output, task.delimiter);
                appendBinaryStringInfoEscapeQuote(&task.output, SPI_fname(SPI_tuptable->tupdesc, col), strlen(SPI_fname(SPI_tuptable->tupdesc, col)), false, task.escape, task.quote);
            }
        }
        if (task.output.len) appendStringInfoString(&task.output, "\n");
        for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
            char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
            if (col > 1) appendStringInfoChar(&task.output, task.delimiter);
            if (!value) appendStringInfoString(&task.output, task.null); else {
                appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !init_oid_is_string(SPI_gettypeid(SPI_tuptable->tupdesc, col)) && task.string, task.escape, task.quote);
                pfree(value);
            }
        }
    }
    if (count) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, insert ? "%s 0 " UINT64_FORMAT : "%s " UINT64_FORMAT, tagname, SPI_processed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    elog(DEBUG1, "id = %li, completionTag = %s", task.shared->id, completionTag);
    if (task.skip) task.skip = 0; else {
        if (!task.output.data) initStringInfoMy(&task.output);
        if (task.output.len) appendStringInfoString(&task.output, "\n");
        appendStringInfoString(&task.output, completionTag);
    }
}

void task_catch(void) {
    emit_log_hook_prev = emit_log_hook;
    emit_log_hook = task_error;
    EmitErrorReport();
    FlushErrorState();
}
