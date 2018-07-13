#!/bin/bash -e

TOOL_SHED_DIR=`pwd`/../../tool-shed
KAFKA_BOOTSTRAP_SERVERS=$(CLUSTER_TYPE=kubernetes source ${TOOL_SHED_DIR}/include/servers-staging.inc && echo $KAFKA_BOOTSTRAP_SERVERS)
KAFKA_CERTS_CONFIG=$(CLUSTER_TYPE=kubernetes source ${TOOL_SHED_DIR}/include/servers-staging.inc && echo $KAFKA_CERTS_CONFIG)

if [[ -n ${KAFKA_CERTS_CONFIG} ]]; then
	echo "Using TLS from '$KAFKA_CERTS_CONFIG'"
	KAFKA_CERTS_PREFIX_OPT="-C ${TOOL_SHED_DIR}/kubernetes/kafka/certs/${KAFKA_CERTS_CONFIG}/"
else
	echo "Not using TLS"
fi

exec ./kafka_producer.py \
	-b ${KAFKA_BOOTSTRAP_SERVERS} \
	${KAFKA_CERTS_PREFIX_OPT} \
	$*
