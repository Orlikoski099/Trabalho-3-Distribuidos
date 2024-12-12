@echo off
REM Instala o Pyro5
pip install Pyro5

REM Inicia o NS em uma nova janela
start python -m Pyro5.nameserver

REM Aguarda alguns segundos para garantir que o NS esteja ativo
timeout /t 5 /nobreak > nul

REM Inicia o líder em uma nova janela
start python broker.py Líder-Epoca1 lider

REM Inicia os votantes em novas janelas
start python broker.py Votante1 votante
start python broker.py Votante2 votante

REM Inicia o observador em uma nova janela
start python broker.py Observador1 observador

REM Incia o Consumidor
start python consumer.py

REM Inicia o Produtor
start python producer.py

REM Lista os itens no NS
python -m Pyro5.nsc list
