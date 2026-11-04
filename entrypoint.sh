#!/bin/sh
# Aplicăm regula netem pentru interfața de rețea dacă pachetul iproute2/tc este instalat
if command -v tc > /dev/null 2>&1; then
    tc qdisc add dev eth0 root netem delay 20ms loss 1% 2>/dev/null || true
fi

exec "$@"