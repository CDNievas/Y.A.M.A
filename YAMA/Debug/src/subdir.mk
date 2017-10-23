################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/YAMA.c \
../src/balanceoDeCargas.c \
../src/funcionesYAMA.c \
../src/reduccionGlobal.c \
../src/reduccionLocal.c \
../src/serializaciones.c \
../src/transformacion.c 

OBJS += \
./src/YAMA.o \
./src/balanceoDeCargas.o \
./src/funcionesYAMA.o \
./src/reduccionGlobal.o \
./src/reduccionLocal.o \
./src/serializaciones.o \
./src/transformacion.o 

C_DEPS += \
./src/YAMA.d \
./src/balanceoDeCargas.d \
./src/funcionesYAMA.d \
./src/reduccionGlobal.d \
./src/reduccionLocal.d \
./src/serializaciones.d \
./src/transformacion.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


