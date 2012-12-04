#!/bin/sh

for i in $(seq 50 200); do
        echo "Spawning client 131.114.137.$i"
        PYTHONPATH=. BIND_ADDR="131.114.137.$i" LD_PRELOAD=./helpers/binder/bind.so python twitter/worker.py worker-$(printf %03d $i) > /dev/null &
        sleep 2
done