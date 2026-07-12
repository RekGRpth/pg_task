#!/bin/sh -ex

if [ -z $PG_BUILD_FROM_SOURCE ]; then
	GREEN="$(postgres --version | grep -Ei "Green(plum|gage)" >/dev/null && echo yes || echo no)"
	PG_MAJOR="$(pg_config --version | cut -f 2 -d ' ' | grep -E -o "[[:digit:]]+" | head -1)"
	if [ "$GREEN" = "yes" ]; then
		ARENADATA="$(pg_config --gp_version | grep -i arenadata >/dev/null && echo yes || echo no)"
		GREENGAGE="$(pg_config --gp_version | grep -i greengage >/dev/null && echo yes || echo no)"
		MAIN=main
		if [ "$ARENADATA" = "yes" ]; then
			REPO=arenadata/gpdb
			MAIN=adb-7.2.0
		elif [ "$GREENGAGE" = "yes" ]; then
			REPO=GreengageDB/greengage
			MAIN=7.x
		else
			REPO=greenplum-db/gpdb-archive
		fi
		REL="$(test "$PG_MAJOR" -lt 10 && pg_config --gp_version | cut -f 2 -d ' ' | cut -f 1 -d '+' || echo "main")"
	else
		MAIN=master
		REPO=postgres/postgres
		PG_VERSION="$(pg_config --version | cut -f 2 -d ' ' | tr '.' '_' | sed 's/rc/_RC/' | sed 's/beta/_BETA/')"
		REL="$(test "$PG_MAJOR" -lt 10 && echo "REL$PG_VERSION" || echo "REL_$PG_VERSION")"
		STABLE="$(test "$PG_MAJOR" -lt 10 && echo "REL9_${PG_MAJOR}_STABLE" || echo "REL_${PG_MAJOR}_STABLE")"
	fi
else
	MAIN=master
	REL="$(test "$PG_MAJOR" -lt 10 && echo "REL9_${PG_MAJOR}_STABLE" || echo "REL_${PG_MAJOR}_STABLE")"
	REPO=postgres/postgres
fi
(curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$REL/src/backend/tcop/postgres.c" || \
curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$STABLE/src/backend/tcop/postgres.c" || \
curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$MAIN/src/backend/tcop/postgres.c")
