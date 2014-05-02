#!/usr/bin/env bash

binary_name=$1
echo $binary_name

if [ "$binary_name" == "client" ]; then
    ./client1/client.py > log/client1 2>&1 &
    ./client2/client.py > log/client2 2>&1 &
    ./client3/client.py > log/client3 2>&1 &
    ./client4/client.py > log/client4 2>&1 &
fi

if [ "$binary_name" == "frontend" ]; then
    ./frontend1/frontend.py > log/frontend1 2>&1 &
    ./frontend2/frontend.py > log/frontend2 2>&1 &
fi

if [ "$binary_name" == "backend" ]; then
    ./backend/backend.py > log/backend 2>&1 &
fi

if [ "$binary_name" == "" ]; then
    ./backend/backend.py    > /dev/null 2>&1 &      
    ./frontend1/frontend.py > /dev/null 2>&1 &      
    ./frontend2/frontend.py > /dev/null 2>&1 &      
    ./client1/client.py > /dev/null 2>&1 &      
    ./client2/client.py > /dev/null 2>&1 &      
    ./client3/client.py > /dev/null 2>&1 &      
    ./client4/client.py> /dev/null 2>&1 &      

fi
