from flask import Flask
import DBConnect

app = Flask(__name__)

@app.route('/selectAll/<string:nomTable>')
def SelectFromTableWithSpark(nomTable):
    try:
        spark = DBConnect.DBConnectSpark()
        table = DBConnect.ReadTableWIthSPark(nomTable)
        tempView = table.createTempView(nomTable)
        query = 'SELECT * from {} '.format(nomTable)
        df = spark.sql(query)
        response = app.response_class(
            response= df.toJSON().collect(),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        return e
    finally:
        spark.stop()

@app.route('/calculNombreTotal/<string:nomTable>')
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

@app.route('/calculNumerateur/<string:nomTable>/<string:colonne>/<string:value>')
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


@app.route('/calculTaux/<string:nomTable>/<string:colonne>/<string:value>')
def CalculTauxFromSpark(nomTable, colonne, value):
    total = int(CalculNombreTotalSpark(nomTable))
    num = int(CalculNumrateurSpark(nomTable, colonne, value))
    taux = (num / total) * 100
    response = app.response_class(
        response= str(taux),
        status=200,
        mimetype='application/json'
    )
    return response

if __name__ == '__main__':
   app.run(debug=True)