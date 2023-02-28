# https://github.com/prefeitura-rio/aquecimento-formacao-infra
# 

import sys
import numpy as np
import requests as rq
import pandas as pd
import json as json
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import os.path


def salvarHive(frame): # Etapa 6
	""" Salva tabela hive """
	print("----- Salvando Hive")
	frame['location.postcode'] = frame['location.postcode'].astype('string')
	table = pa.Table.from_pandas(frame)
	pq.write_to_dataset(table, root_path='resultado/paisEstado', partition_cols=['location.country', 'location.state'])

def agrupandoPorPaisEstado(frame): # Etapa 5
	""" Gera CSV por Pais / Estado """
	print("----- Gerando CSV")
	df1 = frame.groupby(["location.country", "location.state"])["name.first"].count()
	df1.to_csv('resultado/agrupandoPorPaisEstado.csv', sep=';')

def gerarGrafico(frame): # Etapa 4
# TODO: ajustar a imagem para amostras grandes
# TODO: FIX: Warning: Ignoring XDG_SESSION_TYPE=wayland on Gnome. Use QT_QPA_PLATFORM=wayland to run on Wayland anyway.
	""" Monta gráfico de distribuicao de idade """
	print("----- Montando gráfico")
	plt.style.use('_mpl-gallery-nogrid')
	# make data
	x = frame['dob.age']
	colors = plt.get_cmap('Blues')(np.linspace(0.2, 0.7, len(x)))
	# plot
	fig, ax = plt.subplots()
	ax.pie(x, labels=frame['dob.age'], colors=colors, radius=3, center=(4, 4),
	       wedgeprops={"linewidth": 1, "edgecolor": "white"}, frame=True)
	#ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
	#       ylim=(0, 8), yticks=np.arange(1, 8))
	#plt.show()
	fig.savefig('resultado/MyFigure.png', dpi=400, format='png')

def gerarDadosAgrupadosPorPais(frame): # Etapa 4
	""" Calcula percentual por pais """
	print("----- Calculando percentual por Pais")
	df2 = frame.groupby(['location.country']).size().reset_index(name='totais')
	df2['percent'] = df2['totais'] / df2['totais'].sum()
	df2.to_csv('resultado/pais.csv', sep=';', index=False)

def gerarDadosAgrupadosPorGenero(frame): # Etapa 4
	""" Calcula percentual por genero """
	print("----- Calculando percentual por genero")
	df2 = frame.groupby(['gender']).size().reset_index(name='totais')
	df2['percent'] = df2['totais'] / df2['totais'].sum()
	df2.to_csv('resultado/genero.csv', sep=';', index=False)
	
def formataTelefone(frame): # Etapa 3
	"""Formata os campos de telefone removendo os caracteres não alfanuméricos"""
	print("----- Formatando os telefones")
	frame['phone'] = frame['phone'].str.replace(r'[^0-9a-zA-Z:,]+', '', regex=True)
	frame['cell'] = frame['cell'].str.replace(r'[^0-9a-zA-Z:,]+', '', regex=True)

def jsonParaDataframe(resp): # Etapa 2
	"""Cria o Dataframe à partir do Json"""
	print("----- Criando o Dataframe")
	return pd.json_normalize(resp.json(), record_path=['results'])

def buscarJson(url, tamAmostra): # Etapa 1
	"""Consome a API retornando o Json"""
	print("----- Consumindo a API")
	return rq.get(url + str(tamAmostra))

def preparaSaida(): # Cria pasta para saida da execução
	if not os.path.isdir("resultado"):
	       	os.mkdir("resultado")
        
def main(url, tamAmostra): # Controla a execução do script
	"""Metodo Principal"""
	print("##### Inicio - " + str(datetime.now()))
	
	preparaSaida()

	resp = buscarJson(url, tamAmostra)
	
	df1 = jsonParaDataframe(resp)

	formataTelefone(df1)

	df1.to_csv('resultado/arq.csv', sep=';', index=False)

	gerarDadosAgrupadosPorGenero(df1)	

	gerarDadosAgrupadosPorPais(df1)	

	gerarGrafico(df1)	

	agrupandoPorPaisEstado(df1)

	salvarHive(df1)

	print("######## Fim - " + str(datetime.now()))

# Etapa 7
if len(sys.argv) == 3:
	print("***** Executando com parametros")
# TODO: Metodo para verificar os parâmetros
	main(sys.argv[1], sys.argv[2])
else:
	print("***** Executando sem parametros")
	main("https://randomuser.me/api/?results=", 100)


