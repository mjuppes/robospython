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

class Simply(BD):

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


    def __getDadosAfsSegnudaConsulta(self):
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
                    where 

                              1=1
		and cast(data3 as date) between '2023-09-18' and '2023-12-30'
		AND DATA3 IS NOT NULL
		AND (NOT EXISTS (SELECT TOP 1 1 FROM SIMPLY_ID S WHERE AF.CODIGO = S.CODIGO_AF) 
					or
					exists (SELECT TOP 1 1 FROM SIMPLY_ID S WHERE AF.CODIGO = S.CODIGO_AF and (s.response is null and s.response_2 is null))
			)
		and NOT EXISTS (SELECT TOP 1 1 FROM SIMPLY_ID S WHERE AF.CODIGO = S.CODIGO_AF and (s.response is not null or s.response_2 is not null)) 
		and exists (select top 1 1 from auxilio_financeiro_analise where Codigo = af.CODIGO and Status = 56)

        """

            return self.getRecordSetExecSql(sql)
        except ValueError:
            print(ValueError)


    
  
    def __getIdSImply(self, codigoAf):
        try:
            sql = "select top 1 id from SIMPLY_ID where codigo_af =  "+str(codigoAf)+" order by id desc"

            return self.getRecordSetExecSql(sql)
        except ValueError:
            print(ValueError)

    def getDadosSimply(self, consulta2 = False):
        try:
            if(consulta2 == False):
                dados =  self.__getDadosAfs()

            if(consulta2 == True):
                dados =  self.__getDadosAfsSegnudaConsulta()

            if len(dados) == 0:
                print('-----------------------------------------------------------------------')
                print("Sem documentos para enviar.")
                return

            for row in dados:
                cpf = row[0]
                CodigoLegado = row[1]
                pathAf = row[2]

                print(pathAf)


                dt = datetime.now()
                hour    = (str('0')+str(dt.hour))   if (dt.hour < 10)   else str(dt.hour)
                file_log = 'log_erro_'+hour+'.txt'

                import logging
                logging.basicConfig(filename=file_log, level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

                try:
                    from ftplib import FTP
                    ftp = FTP('ftpff11.facta.com.br')
                    ftp.login('ftpsite', 'facta2011')

                    path = '//Fac_Financeira/' + pathAf
                    ftp.cwd(path)

                    arquivos = ftp.nlst()

                    ftp.quit()

                except Exception as e:
                    print(f"Erro ao acessar FTP para {pathAf}: {str(e)}")
                    logging.error(f"Erro ao acessar FTP para {pathAf}: {str(e)}")
                    continue


                url = "https://app.factafinanceira.com.br/ProcessaDocumentoSimplyBanco/getProcessDocumentoSimply"

                isCNH = False

                dictDocs = {}
                for arquivo in arquivos:
                    if(arquivo.find('arqSelfieIni') != -1):
                        dictDocs['arqSelfieIni'] = arquivo

                    if(arquivo.find("arqRG_FRENTE") != -1 or arquivo.find("DocumentoFrente") != -1):
                        dictDocs['arqfotoDocumentoFrente'] = arquivo

                    if(arquivo.find("nh") != -1 or arquivo.find("cnh") != -1 or arquivo.find("CNH") != -1):
                        isCNH = True
                        dictDocs['arqfotoDocumentoFrente'] = arquivo

                if(len(dictDocs) != 0):
                    payload = {}

                    pathFile = '//FTPFF11.facta.com.br/dados_financeira_2023$/FTP/FtSiteImagens/'+str(pathAf)

                    files = [
                        ('imagemSelfie',(dictDocs['arqSelfieIni'],open(pathFile+dictDocs['arqSelfieIni'],'rb'),'image/png')),
                        ('imagemDocumento',(dictDocs['arqfotoDocumentoFrente'],open(pathFile+dictDocs['arqfotoDocumentoFrente'],'rb'),'image/png')),
                    ]

                    headers = {}

                    if(isCNH == False):
                        payload = { 'tipoDocumento'  : 'FRENTE', 'cpf' : cpf, 'CodigoLegado' : CodigoLegado }

                    if(isCNH == True):
                        payload = { 'tipoDocumento'  : 'CNH', 'cpf' : cpf, 'CodigoLegado' : CodigoLegado }

                    print('Frente')
                    response = requests.post(url, headers = headers, data = payload, files = files)

                    print(response.text)


                    if(isCNH == False):
                        dictDocs = {}
                        for arquivo in arquivos:
                            if(arquivo.find('verso') != -1 or arquivo.find('VERSO') != -1 or arquivo.find('erso') != -1):
                                dictDocs['arqVerso'] = arquivo

                        if (len(dictDocs) != 0):
                            payload = {}

                            
                            pathFile = '//FTPFF11.facta.com.br/dados_financeira_2023$/FTP/FtSiteImagens/'+str(pathAf)
                            print('Verso')

                            files = [
                                ('imagemDocumento',(dictDocs['arqVerso'],open(pathFile+dictDocs['arqVerso'],'rb'),'image/png')),
                            ]

                            headers = {}
                            
                            dadosSimply =  self.__getIdSImply(CodigoLegado)
                            for idSmply in dadosSimply:
                                payload = { 'tipoDocumento'  : 'VERSO', 'cpf' : cpf, 'CodigoLegado' : CodigoLegado, 'idsimply' : idSmply[0] }
                                response = requests.post(url, headers=headers, data=payload, files=files)
                                print(response.text)
                else:
                     print('Sem documentos basicos')
        except ValueError:
            print("error")

