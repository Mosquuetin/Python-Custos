import os
import time

inicio = time.perf_counter()

print('-----------------Inicio BB Sispag-----------------')
os.system("unificado_bbsispag_v2.py")

print('-----------------Inicio Bradesco Selenium-----------------')
os.system("unificado_bradescoselenium_v2.py")

print('-----------------Inicio Saidas-----------------')
os.system("unificado_saidas_v2.py")

print('-----------------Inicio Demais Pix-----------------')
os.system("unificado_demaispix_v2.py")

print('-----------------Inicio StarkBank-----------------')
os.system("unificado_starkbank_v2.py")

print('-----------------Inicio Gerais-----------------')
os.system("unificado_gerais_v2.py")

print('-----------------Inicio BB Pix-----------------')
os.system("unificado_bbpix_v2.py")


fim = time.perf_counter()
print((time.strftime("%H:%M:%S", time.gmtime(fim-inicio))))