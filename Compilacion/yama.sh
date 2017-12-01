cd ../YAMA/src
echo "Compilando YAMA"
gcc balanceoDeCargas.c funcionesYAMA.c reduccionGlobal.c reduccionLocal.c serializaciones.c transformacion.c YAMA.c -o yama -lcommons -lpthread