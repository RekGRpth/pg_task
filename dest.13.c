void BeginCommandMy(CommandTag commandTag, CommandDest dest) {
    D1("%li: %s", task->id, GetCommandTagName(commandTag));
}

void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output) {
    char completionTag[COMPLETION_TAG_BUFSIZE];
    CommandTag tag = qc->commandTag;
    const char *tagname = GetCommandTagName(tag);
    if (command_tag_display_rowcount(tag) && !force_undecorated_output) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, tag == CMDTAG_INSERT ? "%s 0 " UINT64_FORMAT : "%s " UINT64_FORMAT, tagname, qc->nprocessed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    D1("%li: %s", task->id, completionTag);
    if (task->skip) task->skip = 0; else {
        if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
        if (task->output.len) appendStringInfoString(&task->output, "\n");
        appendStringInfoString(&task->output, completionTag);
    }
}
