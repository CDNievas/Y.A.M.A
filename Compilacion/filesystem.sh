#!/bin/sh
cd ../FileSystem/src
echo "Compilando FileSystem"
gcc filesystem.c nodos.c principalesFS.c -o filesystem -lcommons -lpthread -lreadline
cd ../../Compilacion/
