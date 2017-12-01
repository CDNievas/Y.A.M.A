echo "Instalando commons"
cd ../
git clone https://github.com/sisoputnfrba/so-commons-library
cd so-commons-library
make install
cd ../
echo "Instalando readline"
sudo apt-get install libreadline6 libreadline6-dev
echo "Compilando Datanode"
cd tp-2017-2c-ElTPEstaBien/DataNode/src/
gcc funcionesDatanode.c datanode.c -o datanode -lcommons
echo "Compilando FileSystem"
cd ../../FileSystem/src
gcc BitarrayConfiguraciones.c  Consola.c EstadoAnterior.c FS.c FuncionesFS.c Nodos.c Persistencia.c Worker.c YAMA.c -o filesystem -lcommons -lpthread -lreadline
echo "Compilando YAMA"
cd ../../YAMA/src
gcc balanceoDeCargas.c funcionesYAMA.c reduccionGlobal.c reduccionLocal.c serializaciones.c transformacion.c YAMA.c -o yama -lcommons -lpthread
echo "Compilando Worker"
cd ../../Worker/src
gcc worker.c -o worker -lcommons -lpthread -lm
echo "Compilando Master"
cd ../../Master/src
gcc master.c -o master -lcommons -lpthread
