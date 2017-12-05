#!/bin/sh
cd ../FileSystem/src
echo "Compilando FileSystem"
gcc BitarrayConfiguraciones.c Consola.c EstadoAnterior.c FS.c FuncionesDirectorios.c FuncionesFS.c Nodos.c Persistencia.c Worker.c YAMA.c -o filesystem -lcommons -lpthread -lreadline
cd ../../Compilacion/
