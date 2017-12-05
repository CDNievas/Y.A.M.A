#!/bin/sh
cd ../DataNode/src
echo "Compilando Datanode"
gcc datanode.c funcionesDatanode.c -o datanode -lcommons
cd ../../Compilacion
