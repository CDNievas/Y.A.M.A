#!/bin/sh
cd ../FileSystem/src
echo "Compilando FileSystem"
gcc consola.c estadoAnterior.c filesystem.c funcionesConsola.c funcionesPath.c funcionesWorker.c funcionesYama.c nodos.c persistencia.c principalesFS.c -o filesystem -lcommons -lpthread -lreadline
cd ../../Compilacion/
