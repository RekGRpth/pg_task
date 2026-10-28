#!/bin/sh -ex

# the server that the Makefile builds for, by its PG_CONFIG, rather than whichever pg_config and postgres come first in PATH, of another version maybe
PG_CONFIG="${PG_CONFIG:-pg_config}"
GREEN="$("$PG_CONFIG" --gp_version >/dev/null 2>&1 && echo yes || echo no)"
PG_MAJOR="$("$PG_CONFIG" --version | cut -f 2 -d ' ' | grep -E -o "[[:digit:]]+" | head -1)"
PG_MINOR="$("$PG_CONFIG" --version | cut -f 2 -d ' ' | grep -E -o "[[:digit:]]+" | sed -n 2p)"
if [ "$GREEN" = "yes" ]; then
	REPO=GreengageDB/greengage
	MAIN=7.x
	REL="$("$PG_CONFIG" --gp_version | cut -f 2 -d ' ' | cut -f 1 -d '+')"
else
	MAIN=master
	REPO=postgres/postgres
	PG_VERSION="$("$PG_CONFIG" --version | cut -f 2 -d ' ' | tr '.' '_' | sed 's/rc/_RC/' | sed 's/beta/_BETA/')"
	REL="$(test "$PG_MAJOR" -lt 10 && echo "REL$PG_VERSION" || echo "REL_$PG_VERSION")"
	STABLE="$(test "$PG_MAJOR" -lt 10 && echo "REL${PG_MAJOR}_${PG_MINOR}_STABLE" || echo "REL_${PG_MAJOR}_STABLE")"
fi
# each try to a file of its own, a download cut short on the way not to have the next one appended to the part it got, extracted twice then, see exec.sh
for TAG in "$REL" "$STABLE" "$MAIN"; do
	[ -n "$TAG" ] || continue
	if curl --no-progress-meter -fL -o postgres.c.tmp "https://raw.githubusercontent.com/$REPO/$TAG/src/backend/tcop/postgres.c"; then
		cat postgres.c.tmp
		rm -f postgres.c.tmp
		exit 0
	fi
	rm -f postgres.c.tmp
done
exit 1
