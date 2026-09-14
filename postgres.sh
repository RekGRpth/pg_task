#!/bin/sh -ex

if [ -z $PG_BUILD_FROM_SOURCE ]; then
	GREEN="$(postgres --version | grep -Ei "Green(plum|gage)" >/dev/null && echo yes || echo no)"
	PG_MAJOR="$(pg_config --version | cut -f 2 -d ' ' | grep -E -o "[[:digit:]]+" | head -1)"
	if [ "$GREEN" = "yes" ]; then
		REPO=GreengageDB/greengage
		MAIN=7.x
		REL="$(pg_config --gp_version | cut -f 2 -d ' ' | cut -f 1 -d '+')"
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
for TAG in "$REL" "$STABLE" "$MAIN"; do
	[ -n "$TAG" ] || continue
	curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$TAG/src/backend/tcop/postgres.c" && break
done
