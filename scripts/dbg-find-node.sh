#!/usr/bin/env bash

SCRIPT_DIR="$(readlink -f "$(dirname "$0")")"

export RUST_LOG=${RUST_LOG:-solana=info,solana_runtime::message_processor=debug} # if RUST_LOG is unset, default to info
export RUST_BACKTRACE=1

help_msg () {
  cat <<EOM
$0 <node_pubkey>
EOM
}

help () {
  local error=$1
  if [[ -n "$error" ]]; then
    echo "Error: $error"
    echo
  fi
  help_msg
  if [[ -n "$error" ]]; then
    exit 1
  else
    exit 0
  fi
}

check_bin () {
  local program=$1
  command -v $program >/dev/null 2>&1 || {
    echo >&2 "Unable to locate $program. Aborting."
    exit 1
  }    
}

NODE_PUBKEY=$1

if [ -z $NODE_PUBKEY ]; then
  help "target node pubkey required"
fi

check_bin "solana-gossip"

ENTRYPOINT_MAINNETBETA="entrypoint2.mainnet-beta.solana.com:8001"
ENTRYPOINT_TESTNET="entrypoint2.testnet.solana.com:8001"
ENTRYPOINT_DEVNET="entrypoint2.devnet.solana.com:8001"

search_cluster () {
  local node_pubkey=$1
  local cluster=$2
  local entrypoint=$3
  if [ -z $node_pubkey ]; then
    echo "search_cluster requires node_pubkey arg"
    exit 1
  fi
  if [ -z $cluster ]; then
    echo "search_cluster requires cluster arg"
    exit 1
  fi
  if [ -z $entrypoint ]; then
    echo "serach_cluster requires entrypoint arg"
    exit 1
  fi
  solana-gossip spy \
    --entrypoint $entrypoint \
    --pubkey $node_pubkey \
    --timeout 60 \
    >/dev/null 2>&1 \
    && echo "found: $cluster ($entrypoint)"
}

search_cluster $NODE_PUBKEY "mainnet-beta" $ENTRYPOINT_MAINNETBETA
search_cluster $NODE_PUBKEY "testnet" $ENTRYPOINT_TESTNET
search_cluster $NODE_PUBKEY "devnet" $ENTRYPOINT_DEVNET

exit 0

############################################

#
# Loop through invalidator test cases with invalidator-client. Ctrl-C to exit.
#


while [[ $# -gt 0 ]]; do
  case $1 in
    --runtime)
      RUNTIME="$2"
      shift 2
      ;;
    --sleeptime)
      SLEEPTIME="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --rpc-adversary-keypair)
      KEYPAIR="$2"
      shift 2
      ;;
    --help)
      help
      ;;
    *)
      help "Unknown argument $1"
      ;;
  esac
done

if [ -z "$RUNTIME" ]; then
  help "--runtime argument is required"
fi

if [ -z "$SLEEPTIME" ]; then
  help "--sleeptime argument is required"
fi

if [ -n "$KEYPAIR" ]; then
  COMMON_ARGS="--rpc-adversary-keypair $KEYPAIR"
  REPAIR_SH_ARGS="$COMMON_ARGS"
fi

# Reduce ancestor hash sample size for smaller cluster size
$BIN "$COMMON_ARGS" configure-repair-parameters --ancestor-hash-repair-sample-size 2

commands=(
  "$BIN $COMMON_ARGS configure-invalidate-leader-block \
    --invalidation-kind invalidFeePayer"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-invalidate-leader-block"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-invalidate-leader-block \
    --invalidation-kind invalidSignature"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-invalidate-leader-block"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-drop-turbine-votes \
    --drop true"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-drop-turbine-votes \
    --drop false"
  "sleep $SLEEPTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test minimal_packets"
  "sleep $RUNTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test disable"
  "sleep $SLEEPTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test ping_cache_overflow"
  "sleep $RUNTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test disable"
  "sleep $SLEEPTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test unavailable_slots"
  "sleep $RUNTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test disable"
  "sleep $SLEEPTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test ping_overflow_with_orphan"
  "sleep $RUNTIME"
  "$SCRIPT_DIR/repair-tests.sh $REPAIR_SH_ARGS --test disable"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-gossip-packet-flood \
    --flood-strategy pingCacheOverflow \
    --iteration-delay-us 1000000 \
    --packets-per-peer-per-iteration 10000"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-gossip-packet-flood"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack \
    --selected-attack transferRandom"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack \
    --selected-attack createNonceAccounts"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack \
    --selected-attack allocateRandomLarge"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack \
    --selected-attack allocateRandomSmall"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack"
  "sleep $SLEEPTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack \
    --selected-attack chainTransactions"
  "sleep $RUNTIME"
  "$BIN $COMMON_ARGS configure-replay-stage-attack"
  "sleep $SLEEPTIME"
)
num_commands=${#commands[@]}

if [[ -z $ITERATIONS ]]; then
	ITERATIONS=0
else
	ITERATIONS=$((ITERATIONS * num_commands))
fi

i=0
while [ $ITERATIONS -eq 0 ] || [ $i -lt $ITERATIONS ]; do
  echo "Iteration $i"
  ${commands[$((i % num_commands))]}
  ((i++))
done
