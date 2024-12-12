#!/bin/bash
pip install Pyro5
# Inicia o NS
python -m Pyro5.nameserver
# Inicia o líder
python broker.py Líder-Epoca1 lider 
# Inicia os votantes
python broker.py Votante1 votante 
python broker.py Votante2 votante 
# Inicia o observador
python broker.py Observador1 observador 
# Lista os itens no NS
python3 -m Pyro5.nsc list