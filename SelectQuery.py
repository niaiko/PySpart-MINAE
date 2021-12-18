from flask import Flask
import DBConnect

app = Flask(__name__)

@app.route('/selectAllWithParamTable/<string:nomTable>', methods=['GET'])
def SelectFromTableWithSpark(nomTable):
    try:
        spark = DBConnect.DBConnectSpark()
        table = DBConnect.ReadTableWIthSPark(nomTable)
        tempView = table.createTempView(nomTable)
        query = 'SELECT * from {} '.format(nomTable)
        df = spark.sql(query)
        to_pandans = df.toPandas()
        response = app.response_class(
            response = to_pandans.to_json(orient='table'),
            status =200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        return e
    finally:
        spark.stop()

@app.route('/calculNombreTotal/<string:nomTable>', methods=['GET'])
def CalculNombreTotalSpark(nomTable):
    try:
        spark = DBConnect.DBConnectSpark()
        table = DBConnect.ReadTableWIthSPark(nomTable)
        tempView = table.createTempView(nomTable)
        query = 'SELECT COUNT(*) from {} '.format(nomTable)
        res = spark.sql(query).head()[0]
        return str(res)
    except Exception as e:
        return e
    finally:
        spark.stop()

@app.route('/calculNumerateurParam1/<string:nomTable>/<string:colonne>/<string:value>', methods=['GET'])
def CalculNumrateurSpark(nomTable, colonne, value):
    try:
        spark = DBConnect.DBConnectSpark()
        table = DBConnect.ReadTableWIthSPark(nomTable)
        tempView = table.createTempView(nomTable)
        query = 'SELECT COUNT(*) from {} where {} = "{}"'.format(nomTable, colonne, value)
        res = spark.sql(query).head()[0]
        return str(res)
    except Exception as e:
        return e
    finally:
        spark.stop()


@app.route('/calculTauxParam1/<string:nomTable>/<string:colonne>/<string:value>', methods=['GET'])
def CalculTauxFromSpark(nomTable, colonne, value):
    total = int(CalculNombreTotalSpark(nomTable))
    num = int(CalculNumrateurSpark(nomTable, colonne, value))
    taux = (num / total) * 100
    response = app.response_class(
        response = str(taux),
        status = 200,
    )
    return response

if __name__ == '__main__':
   app.run(debug=True)