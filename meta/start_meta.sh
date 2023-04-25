#!/bin/sh
set -o errexit


PROJ_DIR=$(cd `dirname $0`;cd ..;pwd)
echo "PROJ_DIR: ${PROJ_DIR}"
cp ${PROJ_DIR}/meta/config/config_8921.toml ${PROJ_DIR}/meta/config/config_8931.toml

if [ "$(uname)" == "Darwin" ]; then
    sed -i '' -e 's/8921/8931/g' ${PROJ_DIR}/meta/config/config_8931.toml
    sed -i '' -e 's/id = 3/id = 4/g' ${PROJ_DIR}/meta/config/config_8931.toml
else
    sed -i 's/8921/8931/g' ${PROJ_DIR}/meta/config/config_8931.toml
    sed -i 's/id = 3/id = 4/g' ${PROJ_DIR}/meta/config/config_8931.toml
fi
# sed -i '' -e 's/8921/8931/g' ${PROJ_DIR}/meta/config/config_8931.toml
# sed -i '' -e 's/id = 3/id = 4/g' ${PROJ_DIR}/meta/config/config_8931.toml
# sed -i 's/8921/8931/g' ${PROJ_DIR}/meta/config/config_8931.toml
# sed -i 's/id = 3/id = 4/g' ${PROJ_DIR}/meta/config/config_8931.toml

rpc() {
    local uri=$1
    local body="$2"

    echo '---'" rpc(:$uri, $body)"
    {
        if [ ".$body" = "." ]; then
            curl --silent "127.0.0.1:$uri"
        else
            curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
        fi
    } | {
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }
    echo
    echo
}

nohup ${PROJ_DIR}/target/debug/cnosdb-meta --config ${PROJ_DIR}/meta/config/config_8931.toml > /tmp/cnosdb/logs/meta_node.4.log 2>&1 &

sleep 1

rpc 8901/add-learner       '[4, "127.0.0.1:8931"]'
echo "Node 4 added as leaner"
sleep 1

if [ $1 == "add" ]; then
    nohup ${PROJ_DIR}/target/debug/cnosdb-meta --config ${PROJ_DIR}/meta/config/config_8931.toml > /tmp/cnosdb/logs/meta_node.4.log 2>&1 &

    sleep 1

    rpc 8901/add-learner       '[4, "127.0.0.1:8931"]'
    echo "Node 4 added as leaner"
    sleep 1
    echo "Changing membership from [1] to 4 nodes cluster: [1, 2, 3, 4]"
    echo
    rpc 8901/change-membership '[1, 2, 3, 4]'
    sleep 1
    echo "Membership changed"
    sleep 1
elif [ $1 == "delete" ]; then
    echo "Changing membership from [1] to 2 nodes cluster: [1, 2]"
    echo
    rpc 8901/change-membership '[1, 2]'
    sleep 1
    echo "Membership changed"
    sleep 1
else
    nohup ${PROJ_DIR}/target/debug/cnosdb-meta --config ${PROJ_DIR}/meta/config/config_8931.toml > /tmp/cnosdb/logs/meta_node.4.log 2>&1 &

    sleep 1

    rpc 8901/add-learner       '[4, "127.0.0.1:8931"]'
    echo "Node 4 added as leaner"
    sleep 1
    echo "Changing membership from [1] to 4 nodes cluster: [1, 2, 4]"
    echo
    rpc 8901/change-membership '[1, 2, 4]'
    sleep 1
    echo "Membership changed"
    sleep 1
fi
# echo "Changing membership from [1] to 4 nodes cluster: [1, 2, 4]"
# echo
# rpc 8901/change-membership '[1, 2, 4]'
# sleep 1
# echo "Membership changed"
# sleep 1

echo "Get metrics from the leader"
rpc 8901/metrics
sleep 1