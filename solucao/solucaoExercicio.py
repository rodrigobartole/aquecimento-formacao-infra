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

def salvarHive(frame): # Etapa 6
	frame['location.postcode'] = frame['location.postcode'].astype('string')
	table = pa.Table.from_pandas(frame)
	pq.write_to_dataset(table, root_path='paisEstado', partition_cols=['location.country', 'location.state'])

def agrupandoPorPaisEstado(frame): # Etapa 5
	df1 = frame.groupby(["location.country", "location.state"])["name.first"].count()
	df1.to_csv('~/sandBox/python/agrupandoPorPaisEstado.csv', sep=';')

def gerarGrafico(frame): # Etapa 4
	""" Monta gráfico de distribuicao de idade """
	plt.style.use('_mpl-gallery-nogrid')
	# make data
	x = frame['dob.age']
	colors = plt.get_cmap('Blues')(np.linspace(0.2, 0.7, len(x)))
	# plot
	fig, ax = plt.subplots()
	ax.pie(x, labels=frame['dob.age'], colors=colors, radius=3, center=(4, 4),
	       wedgeprops={"linewidth": 1, "edgecolor": "white"}, frame=True)
	ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
	       ylim=(0, 8), yticks=np.arange(1, 8))
	#plt.show()
	fig.savefig('MyFigure.png', dpi=200)

def gerarDadosAgrupadosPorPais(frame): # Etapa 4
	""" Calcula percentual por pais """
	df2 = frame.groupby(['location.country']).size().reset_index(name='totais')
	df2['percent'] = df2['totais'] / df2['totais'].sum()
	df2.to_csv('~/sandBox/python/pais.csv', sep=';', index=False)

def gerarDadosAgrupadosPorGenero(frame): # Etapa 4
	""" Calcula percentual por genero """
	df2 = frame.groupby(['gender']).size().reset_index(name='totais')
	df2['percent'] = df2['totais'] / df2['totais'].sum()
	df2.to_csv('~/sandBox/python/genero.csv', sep=';', index=False)
	
def formataTelefone(frame): # Formata os campos de telefone - Etapa 3
	"""Remove os caracteres não alfanuméricos"""
	frame['phone'] = frame['phone'].str.replace(r'[^0-9a-zA-Z:,]+', '', regex=True)
	frame['cell'] = frame['cell'].str.replace(r'[^0-9a-zA-Z:,]+', '', regex=True)

def bart(n=1): # Executa o script
	"""Consome a API"""
	response = rq.get("https://randomuser.me/api/?results=" + str(n)) # Etapa 1
	df1 = pd.json_normalize(response.json(), record_path=['results']) # Etapa 2
	print("###############################################################")
	print(df1['phone'], df1['cell'])
	print("---------------------------------------------------------------")
	formataTelefone(df1)
	df1.to_csv('~/sandBox/python/arq.csv', sep=';', index=False)
	print(df1['phone'], df1['cell'])
	print("---------------------------------------------------------------")
	gerarDadosAgrupadosPorGenero(df1)	
	print("---------------------------------------------------------------")
	gerarDadosAgrupadosPorPais(df1)	
	print("---------------------------------------------------------------")
	gerarGrafico(df1)	
	print("---------------------------------------------------------------")
	agrupandoPorPaisEstado(df1)
	print("---------------------------------------------------------------")
	salvarHive(df1)
	print("###############################################################")

bart(sys.argv[1])
