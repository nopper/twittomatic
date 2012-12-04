#!/bin/sh

for i in $(seq 50 200); do
        ifconfig eth0:$i up 131.114.137.$i
done