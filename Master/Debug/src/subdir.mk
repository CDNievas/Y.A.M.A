################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/almacenadoFinal.c \
../src/funcionesMaster.c \
../src/master.c \
../src/reduccionGlobal.c \
../src/reduccionLocal.c \
../src/transformaciones.c 

OBJS += \
./src/almacenadoFinal.o \
./src/funcionesMaster.o \
./src/master.o \
./src/reduccionGlobal.o \
./src/reduccionLocal.o \
./src/transformaciones.o 

C_DEPS += \
./src/almacenadoFinal.d \
./src/funcionesMaster.d \
./src/master.d \
./src/reduccionGlobal.d \
./src/reduccionLocal.d \
./src/transformaciones.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


