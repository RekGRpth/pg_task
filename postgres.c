#include "dest.h"

#if PG_VERSION_NUM >= 150000
#include <postgres.15.c>
#elif PG_VERSION_NUM >= 140000
#include <postgres.14.c>
#elif PG_VERSION_NUM >= 130000
#include <postgres.13.c>
#elif PG_VERSION_NUM >= 120000
#include <postgres.12.c>
#elif PG_VERSION_NUM >= 110000
#include <postgres.11.c>
#elif PG_VERSION_NUM >= 100000
#include <postgres.10.c>
#elif PG_VERSION_NUM >= 90600
#include <postgres.9.6.c>
#elif PG_VERSION_NUM >= 90500
#include <postgres.9.5.c>
#elif PG_VERSION_NUM >= 90400
#include <postgres.9.4.c>
#endif
