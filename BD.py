import time
import pyodbc

class BD:

    def __get_conexaoBD(self):
        try:
            server = ''
            username = ''
            password = ''
            database = ''
            conn = pyodbc.connect(
                'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
            return conn
        except Exception as e:
            print('Erro: '+str(e))


    def getRecordSetExecSql(self, sql):
        conn = self.__get_conexaoBD()
        cursor = conn.cursor()
        cursor.execute(sql)
        resultado = cursor.fetchall()
        cursor.close()
        conn.close()
        return resultado

    def execSql(self, sql):
        conn = self.__get_conexaoBD()
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.close()
        conn.commit()
        conn.close()
