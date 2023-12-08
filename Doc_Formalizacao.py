import requests
from datetime import date, timedelta
from datetime import datetime
import time
import os
from os import listdir
from os.path import isfile, join
import pyodbc
import json
import requests
import asyncio
from pprint import pprint

from BD import *

class DocFormalizacao(BD):

    def __getDadosAfs(self):
        try:
            sql = """\
                    SELECT 
                        (CASE WHEN Tipo_Operacao NOT IN (35,36,37,38,40,42,43,44) THEN C.CPF else rl.cpf  COLLATE SQL_Latin1_General_CP1_CI_AS  END) CPF
                        ,AF.CODIGO,
                        + CAST(YEAR(AF.DATACADASTRO) AS VARCHAR) 
                        + right(replicate('0',2) + COALESCE(CAST(MONTH(AF.DATACADASTRO) AS VARCHAR),'0'), 2) 
                        + '/' + right(replicate('0',2) + COALESCE(CAST(DAY(AF.DATACADASTRO) AS VARCHAR),'0'), 2) 
                        + '/' + CAST(AF.CODIGO AS VARCHAR) + '/'  as caminho_servidor
                        --+ @local_salvar 
                        FROM AUXILIO_FINANCEIRO AF 
                        LEFT join cliente c on c.CODIGO = AF.CLIENTE
                        LEFT JOIN propostaDadosPessoaisRL rl on RL.idsimulador = af.idsimulador
                        where af.convenio = 3
                            --and af.averbador = 20095
                            --and af.codigo = 67286966
                            and af.TIPOANALISE in (3,23)
                            and af.data_cadastro >= '2023-09-01'
                            and exists (select top 1 1 from auxilio_financeiro_analise a where a.codigo = af.codigo and a.obs = 'Não foi possível realizar a análise documental por falta ou ilegibilidade de imagens. ENVIAR DOCUMENTO DE IDENTIFICAÇÃO - ENVIAR NOVA SELFIE' and a.status in (3,23))
                            and (select top 1 documento_1 from SIMPLY_ID as s where s.codigo_af = af.CODIGO order by id desc) = 0
                            order by af.DATA3 desc
                """

            return self.getRecordSetExecSql(sql)
        except ValueError:
            print(ValueError)





