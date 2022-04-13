#ifndef _DEST_H
#define _DEST_H

#include <postgres.h>

#include <tcop/dest.h>

DestReceiver *CreateDestReceiverMy(CommandDest dest);
#if PG_VERSION_NUM >= 130000
void BeginCommandMy(CommandTag commandTag, CommandDest dest);
#else
void BeginCommandMy(const char *commandTag, CommandDest dest);
#endif
#if PG_VERSION_NUM >= 130000
void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output);
#else
void EndCommandMy(const char *commandTag, CommandDest dest);
#endif
void exec_simple_query(const char *query_string);
void NullCommandMy(CommandDest dest);
void ReadyForQueryMy(CommandDest dest);

#endif // _DEST_H
