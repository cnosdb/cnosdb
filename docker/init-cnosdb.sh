#!/bin/bash
set -e

AUTH_ENABLED="$CNOSDB_HTTP_AUTH_ENABLED"

if [ -z "$AUTH_ENABLED" ]; then
	AUTH_ENABLED="$(grep -iE '^\s*auth-enabled\s*=\s*true' /etc/cnosdb/cnosdb.conf | grep -io 'true' | cat)"
else
	AUTH_ENABLED="$(echo ""$CNOSDB_HTTP_AUTH_ENABLED"" | grep -io 'true' | cat)"
fi

INIT_USERS=$([ ! -z "$AUTH_ENABLED" ] && [ ! -z "$CNOSDB_ADMIN_USER" ] && echo 1 || echo)

if ( [ ! -z "$INIT_USERS" ] || [ ! -z "$CNOSDB_DB" ] || [ "$(ls -A /docker-entrypoint-initdb.d 2> /dev/null)" ] ) && [ ! "$(ls -A /var/lib/cnosdb)" ]; then

	INIT_QUERY=""
	CREATE_DB_QUERY="CREATE DATABASE $CNOSDB_DB"

	if [ ! -z "$INIT_USERS" ]; then

		if [ -z "$CNOSDB_ADMIN_PASSWORD" ]; then
			CNOSDB_ADMIN_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "CNOSDB_ADMIN_PASSWORD:$CNOSDB_ADMIN_PASSWORD"
		fi

		INIT_QUERY="CREATE USER $CNOSDB_ADMIN_USER WITH PASSWORD '$CNOSDB_ADMIN_PASSWORD' WITH ALL PRIVILEGES"
	elif [ ! -z "$CNOSDB_DB" ]; then
		INIT_QUERY="$CREATE_DB_QUERY"
	else
		INIT_QUERY="SHOW DATABASES"
	fi

	CNOSDB_INIT_PORT="8086"

	CNOSDB_HTTP_BIND_ADDRESS=127.0.0.1:$CNOSDB_INIT_PORT CNOSDB_HTTP_HTTPS_ENABLED=false cnosdb "$@" &
	pid="$!"

	$CNOSDB_CLI_CMD="cnosdb-cli -host 127.0.0.1 -port $CNOSDB_INIT_PORT -execute "

	for i in {30..0}; do
		if $CNOSDB_CLI_CMD "$INIT_QUERY" &> /dev/null; then
			break
		fi
		echo 'cnosdb init process in progress...'
		sleep 1
	done

	if [ "$i" = 0 ]; then
		echo >&2 'cnosdb init process failed.'
		exit 1
	fi

	if [ ! -z "$INIT_USERS" ]; then

		$CNOSDB_CLI_CMD="cnosdb-cli -host 127.0.0.1 -port $CNOSDB_INIT_PORT -username ${CNOSDB_ADMIN_USER} -password ${CNOSDB_ADMIN_PASSWORD} -execute "

		if [ ! -z "$CNOSDB_DB" ]; then
			$CNOSDB_CLI_CMD "$CREATE_DB_QUERY"
		fi

		if [ ! -z "$CNOSDB_USER" ] && [ -z "$CNOSDB_USER_PASSWORD" ]; then
			CNOSDB_USER_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "CNOSDB_USER_PASSWORD:$CNOSDB_USER_PASSWORD"
		fi

		if [ ! -z "$CNOSDB_USER" ]; then
			$CNOSDB_CLI_CMD "CREATE USER $CNOSDB_USER WITH PASSWORD '$CNOSDB_USER_PASSWORD'"

			$CNOSDB_CLI_CMD "REVOKE ALL PRIVILEGES FROM ""$CNOSDB_USER"""

			if [ ! -z "$CNOSDB_DB" ]; then
				$CNOSDB_CLI_CMD "GRANT ALL ON ""$CNOSDB_DB"" TO ""$CNOSDB_USER"""
			fi
		fi

		if [ ! -z "$CNOSDB_WRITE_USER" ] && [ -z "$CNOSDB_WRITE_USER_PASSWORD" ]; then
			CNOSDB_WRITE_USER_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "CNOSDB_WRITE_USER_PASSWORD:$CNOSDB_WRITE_USER_PASSWORD"
		fi

		if [ ! -z "$CNOSDB_WRITE_USER" ]; then
			$CNOSDB_CLI_CMD "CREATE USER $CNOSDB_WRITE_USER WITH PASSWORD '$CNOSDB_WRITE_USER_PASSWORD'"
			$CNOSDB_CLI_CMD "REVOKE ALL PRIVILEGES FROM ""$CNOSDB_WRITE_USER"""

			if [ ! -z "$CNOSDB_DB" ]; then
				$CNOSDB_CLI_CMD "GRANT WRITE ON ""$CNOSDB_DB"" TO ""$CNOSDB_WRITE_USER"""
			fi
		fi

		if [ ! -z "$CNOSDB_READ_USER" ] && [ -z "$CNOSDB_READ_USER_PASSWORD" ]; then
			CNOSDB_READ_USER_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "CNOSDB_READ_USER_PASSWORD:$CNOSDB_READ_USER_PASSWORD"
		fi

		if [ ! -z "$CNOSDB_READ_USER" ]; then
			$CNOSDB_CLI_CMD "CREATE USER $CNOSDB_READ_USER WITH PASSWORD '$CNOSDB_READ_USER_PASSWORD'"
			$CNOSDB_CLI_CMD "REVOKE ALL PRIVILEGES FROM ""$CNOSDB_READ_USER"""

			if [ ! -z "$CNOSDB_DB" ]; then
				$CNOSDB_CLI_CMD "GRANT READ ON ""$CNOSDB_DB"" TO ""$CNOSDB_READ_USER"""
			fi
		fi

	fi

	for f in /docker-entrypoint-initdb.d/*; do
		case "$f" in
			*.sh)     echo "$0: running $f"; . "$f" ;;
			*.iql)    echo "$0: running $f"; $CNOSDB_CLI_CMD "$(cat ""$f"")"; echo ;;
			*)        echo "$0: ignoring $f" ;;
		esac
		echo
	done

	if ! kill -s TERM "$pid" || ! wait "$pid"; then
		echo >&2 'cnosdb init process failed. (Could not stop cnosdb)'
		exit 1
	fi

fi
