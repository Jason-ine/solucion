@echo off
title SIPLoader - Ejecucion Automatica
color 0A
setlocal enabledelayedexpansion

:: Configuración
set "JAR_NAME=SIPLoader.jar"
set "MAX_RESTARTS=20"
set "DELAY_SECONDS=30"
set "RESTART_COUNT=0"

:: Mensaje inicial
echo *******************************************************
echo *  Iniciando SIPLoader en modo automatico (boton apachado)  *
echo *  Reinicios maximos: %MAX_RESTARTS%                     *
echo *******************************************************
echo.

:loop
:: Incrementar contador de reinicios
set /a "RESTART_COUNT+=1"

:: Registrar inicio
echo [%date% %time%] Intento !RESTART_COUNT! de %MAX_RESTARTS% >> SIPLoader_runner.log
echo Iniciando aplicacion en modo automatico...

:: Ejecutar JAR (sin --manual para que el botón inicie apachado)
java -jar "%JAR_NAME%"

:: Verificar si se cerró inesperadamente
if %errorlevel% neq 0 (
    echo.
    echo [%date% %time%] ERROR: La aplicacion se cerro (Codigo: %errorlevel%). Reiniciando... >> SIPLoader_runner.log
    echo ¡La aplicacion se cerro! Reiniciando en %DELAY_SECONDS% segundos...
    
    :: Verificar máximo de reinicios
    if !RESTART_COUNT! geq %MAX_RESTARTS% (
        echo.
        echo [%date% %time%] ERROR: Maximo de reinicios alcanzado (%MAX_RESTARTS%) >> SIPLoader_runner.log
        echo ¡Maximo de reinicios alcanzado! No se volvera a intentar.
        pause
        exit /b 1
    )
    
    :: Esperar antes de reiniciar
    timeout /t %DELAY_SECONDS% /nobreak >nul
    goto loop
)

:: Si llegó aquí, la aplicación se cerró correctamente
echo.
echo [%date% %time%] Aplicacion finalizada correctamente >> SIPLoader_runner.log
echo Aplicacion cerrada correctamente.
pause