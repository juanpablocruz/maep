#!/usr/bin/env bash
set -euo pipefail

# ---- Config you can tweak ----------------------------------------------------
DURATION="${DURATION:-60s}"
QUIESCE_LAST="${QUIESCE_LAST:-10s}"
HB="${HB:-750ms}"
SUS_MULTS="${SUS_MULTS:-3 5}" # suspect-timeout = SUS_MULT * HB (HB is 750ms here)

REPS="${REPS:-3}"

# Grids (keep small-ish by default)
NODES="${NODES:-5 10}"
LOSSES="${LOSSES:-0 0.05}"
REORDERS="${REORDERS:-0 0.2}"
DELAY_JITTER="${DELAY_JITTER:-10ms:10ms 50ms:50ms}"
SYNC_INTS="${SYNC_INTS:-7s 3s}"
WRITE_INTS="${WRITE_INTS:-3s}"
CHUNKS="${CHUNKS:-65536 131072}" # bytes

# Binary + output root
BIN="${BIN:-./maep_sim}"
OUTROOT="${OUTROOT:-out}"

# ---- Build -------------------------------------------------------------------
if [[ ! -x "$BIN" ]]; then
  echo "Building $BIN ..."
  go build -o "$BIN" ./cmd/sim
fi

safe() {
  # normalize strings for directory names
  echo -n "$1" | sed -e 's/\./p/g' -e 's/:/-/g' -e 's/\//_/g' -e 's/[^A-Za-z0-9_-]/_/g'
}

mkdir -p "$OUTROOT"

# ---- Sweep -------------------------------------------------------------------
run_count=0
for N in $NODES; do
  for LS in $LOSSES; do
    for RE in $REORDERS; do
      for DJ in $DELAY_JITTER; do
        IFS=: read -r DE JI <<<"$DJ"
        for SY in $SYNC_INTS; do
          for WR in $WRITE_INTS; do
            for CH in $CHUNKS; do
              for SUSX in $SUS_MULTS; do
                # Only works because HB is 750ms by default
                case "$SUSX" in
                3) SUS="2250ms" ;;
                5) SUS="3750ms" ;;
                *)
                  echo "Unsupported SUS_MULT=$SUSX with HB=$HB; edit script to compute it."
                  exit 1
                  ;;
                esac

                label="N$(safe $N)_loss$(safe $LS)_r$(safe $RE)_d$(safe $DE)_j$(safe $JI)_sync$(safe $SY)_write$(safe $WR)_chunk${CH}_hb$(safe $HB)_sus$(safe $SUS)"
                echo "=== $label ==="
                for r in $(seq 1 "$REPS"); do
                  OUT="$OUTROOT/$label/rep_$r"
                  mkdir -p "$OUT"
                  echo " -> run $r/$REPS -> $OUT"
                  "$BIN" -nodes "$N" -duration "$DURATION" -write-interval "$WR" -sync-interval "$SY" -heartbeat "$HB" -suspect-timeout "$SUS" -delta-chunk-bytes "$CH" -out "$OUT" -loss "$LS" -dup 0 -reorder "$RE" -delay "$DE" -jitter "$JI" -quiesce-last "$QUIESCE_LAST" -failure-period 0 -recovery-delay 5s
                  # brief pause so ports settle between runs
                  sleep 0.5
                  ((run_count++))
                done
              done
            done
          done
        done
      done
    done
  done
done

echo "Completed $run_count runs. Outputs under $OUTROOT/"
echo "Tip: python3 analyze.py $OUTROOT --out $OUTROOT/experiments.csv"
