from flask import Flask
import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import numpy as np


application = Flask(__name__)

@application.route("/", methods=['GET'])
@application.route("/<method>/<mes>", methods=["GET"])
def say_hello (method="World", mes=''):
    retorno = "Hello {}!".format(method)
    connection = psycopg2.connect(user='postgres', password='postgres', host='database-1.c7xoxkhkj7po.us-east-2.rds.amazonaws.com', port='5432')
    cursor = connection.cursor()
    
    if method == "year":    

        query = "SELECT year, month, births FROM births2 WHERE year = (SELECT year FROM births2 GROUP BY year ORDER BY sum(births) DESC LIMIT 1)"

        try:
            df = pd.read_sql(query, connection)
            ano = df["year"][0]
            retorno = 'Sucesso! Ano {} com maior número de pessoas.'.format(int(ano)), 200
        except:
            retorno = "Não foi possível fazer SELECT na tabela.", 500

    if method == "mes_f":

        query = "SELECT day, month, births, gender FROM births2 WHERE gender = 'F'  AND day <= 31 AND month = (SELECT month FROM births2 GROUP BY month ORDER BY sum(births) DESC LIMIT 1)"

        try:
            df = pd.read_sql(query, connection)
            mes = df["month"][0]
            df['soma'] = df.groupby('day')["births"].transform(np.sum)
            retorno = 'Sucesso! Mês {} com maior número de mulheres.'.format(int(mes)), 200
        except:
            retorno = "Não foi possível fazer SELECT na tabela.", 500

    if method == "mes_m":

        query = "SELECT month, births, day FROM births2 WHERE gender = 'M' AND day <= 31 AND month = {}".format(int(mes))

        try:
            df = pd.read_sql(query, connection)
            retorno = 'Sucesso! Mês {} com {} homens.'.format(int(mes), df['births'].sum()), 200
        except:
            retorno = "Não foi possível fazer SELECT na tabela.", 500

    connection.close()
    return retorno


if __name__ == "__main__":
    application.debug = True
    application.run()
