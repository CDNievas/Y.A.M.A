cd ../Worker/src
echo "Compilando Worker"
gcc worker.c -o worker -lcommons -lpthread -lm