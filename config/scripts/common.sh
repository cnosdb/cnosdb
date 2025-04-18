# Common Functions

## CLI Helper Functions.

### Check if $2 >= $3.
### Usage: To Check if there are at least 2 arguments:
### >> check_arg_num $1 $# 2
function check_arg_num {
  local opt_key=$1
  local arg_num=$2
  local arg_num_exp=$3

  if [[ $arg_num -lt $arg_num_exp ]]; then
    echo "Option ${opt_key} need at least ${arg_num_exp} arguments, but got ${arg_num}"
    exit 1
  fi
}

### Check if $2 is a valid integer between 1 and 10.
function check_arg_is_int_and_between_0_10 {
  local opt_key=$1
  local opt_val=$2

  if [[ $opt_val =~ ^[0-9]+$ ]]; then
    if [ $opt_val -lt 1 ]; then
      echo "Option ${opt_key} need value >= 1, but got ${opt_val}"
      exit 1
    fi
    if [ $opt_val -ge 10 ]; then
      echo "Option ${opt_key} need value <= 10, but got ${opt_val}"
      exit 1
    fi
  else
    echo "Option ${opt_key} need integer value [1, 10), but got ${opt_val}"
    exit 1
  fi
}

## Process Helper Functions

function kill_cnosdb_meta_processes {
  kill_process cnosdb-meta
}

function kill_cnosdb_processes {
  kill_process cnosdb
}

function kill_process {
  local process_name=$1

  echo -n "Killing process by name: ${process_name} ..."
  if [ "$(uname)" = "Darwin" ]; then
    # macOS
    if pgrep -xq -- "${process_name}"; then
      pkill -f "${process_name}"
      echo " success"
    fi
  else
    # Linux
    ## Command 'killall' will fail if there is no process to kill, so we use
    ## set +e to ignore it, and restore the error handling after the command.
    local old_options=$(set +o)
    set +e
    if killall $process_name 2>/dev/null; then
      echo " success"
    else
      echo " failed"
    fi
    eval "$old_options"
  fi
}

## HTTP Helper Functions

function http_get {
  local port=$1
  local uri=$2
  local data=$3

  local url="127.0.0.1:${port}${uri}"
  http_request GET "$url" "" "$data"
}

### Send HTTP Post Request With Authorization
### Usage: call 'http://127.0.0.1:8901/init' with data '{}'
### >> http_post 8901 /init '{}'
function http_post {
  local port=$1
  local uri=$2
  local data=$3

  local url="127.0.0.1:${port}${uri}"
  http_request POST "$url" "" "$data"
}

### Send HTTP Post Request With Authorization
### Usage: call 'http://127.0.0.1:8902/api/v1/sql?db=public' with data 'SHOW DATABASES'
### >> http_post_with_auth 8902 '/api/v1/sql?db=public' 'root:root' 'SHOW DATABASES'
function http_post_with_auth {
  local port=$1
  local uri=$2
  local auth=$3
  local data=$4

  local url="127.0.0.1:${port}${uri}"
  http_request POST "$url" "$auth" "$data"
}

### Send HTTP Request.
### If environment variable 'VERBOSE' != '', print curl command and the response.
###
### Usage: call 'http://127.0.0.1:8902/api/v1/sql?db=public' with data 'SHOW DATABASES'
### >> http_request POST 'http://127.0.0.1:8902/api/v1/sql?db=public' 'root:root' 'SHOW DATABASES'
function http_request {
  local method=$1
  local url=$2
  local auth=$3
  local data=$4

  echo '------------ HTTP ------------'
  local is_json=$(expr "$data" : '^[{\[].*$')
  echo "--- POST('${url}', '${data}')"

  local curl_cmd="curl -si -X ${method} ${url}"
  if [[ -n "$auth" ]]; then
    curl_cmd+=" -u ${auth}"
  fi
  if [[ $is_json -gt 0 ]]; then
    curl_cmd+=" -H 'Content-Type: application/json'"
  fi
  if [ -n "$data" ]; then
    curl_cmd+=" -d '${data}'"
  fi
  if [[ -n "$VERBOSE" ]]; then
    echo "CURL: ${curl_cmd}"
  fi

  local response=$(eval $curl_cmd)
  if [[ -n "$VERBOSE" ]]; then
    echo "RESP: ${response}"
  fi
  local resp_header=$(echo "$response" | awk 'NR==1, /^\r?$/{print}')
  local resp_body=$(echo "$response" | awk '/^\r?$/{y=1;next}y')
  local resp_content_type=$(echo "$resp_header" | grep -i "Content-Type:" | awk -F': ' '{print $2}' | tr -d '\r')
  echo -n '--- RESPONSE: '
  case "$resp_content_type" in
  *json*)
    echo "json"
    echo "$resp_body" | jq . 2>/dev/null || echo "$resp_body"
    ;;
  *csv*)
    echo "csv"
    echo "$resp_body" | echo "$resp_body"
    ;;
  *text*)
    echo "text"
    echo "$resp_body" | echo "$resp_body"
    ;;
  *html*)
    echo "html"
    echo "$resp_body"
    ;;
  *)
    echo "unknown (${resp_content_type})"
    echo "$resp_body"
    ;;
  esac
  echo '------------------------------'
}

## CnosDB Meta Server Helpers

### Extract memberships from meta server api: 127.0.0.1:8901/metrics
function extract_memberships {
  local metrics_response=$1

  tmp=${metrics_response#*\"membership\":{\"configs\":[[}
  echo $tmp
  extracted="${tmp%]]*}"
  echo $extracted
}