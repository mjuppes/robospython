import requests
from datetime import date, timedelta
from datetime import datetime
import time
import os
from os import listdir
from os.path import isfile, join, getsize
import pyodbc
import json
import requests
import asyncio
import configparser
from pprint import pprint
import shutil

from BD import *

class DocFormalizacao(BD):

    def __init__(self):
        self.userFtp    = "ftpsite"
        self.passFtp    = "facta2011"
        self.dir_path = os.getcwd()

    def __getDadosAfs(self):
        try:
            sql = """\
                    SELECT
                        F.ID,F.CODIGO_AF, F.CODIGOAF_DOC_REUTILIZADA CODIGORETUILIZADO, A.CODIGO_VINCULADO , 
                        CAST(A.DATA_CADASTRO AS DATE) AS DATA_CADASTRO,
                        (SELECT CAST(DATA_CADASTRO AS DATE) FROM AUXILIO_FINANCEIRO WHERE CODIGO = F.CODIGOAF_DOC_REUTILIZADA)  DATA_CADASTRO_REUTILIZADO,
                        (CASE WHEN A.CODIGO_VINCULADO IS NOT NULL THEN (SELECT CAST(DATA_CADASTRO AS DATE) FROM AUXILIO_FINANCEIRO WHERE CODIGO = A.CODIGO_VINCULADO) ELSE NULL END)  DATA_CADASTRO_COD_VINCULADO
                                FROM FORMALIZACAO_DIGITAL F
                                    JOIN AUXILIO_FINANCEIRO A ON A.CODIGO = F.CODIGO_AF
                                WHERE F.CODIGOAF_DOC_REUTILIZADA IS NOT NULL
                                    AND F.assinatura_datahora >= DATEADD(HOUR, -4, GETDATE())  -- Filtra registros das últimas 2 horas
                                    AND NOT EXISTS (
                                        SELECT 1
                                            FROM documentosreutilizadosafs DR
                                        WHERE
                                            DR.codigoaf = F.CODIGO_AF
                                    )
                    ORDER BY F.assinatura_datahora DESC
                """

            return self.getRecordSetExecSql(sql)
        except ValueError:
            print(ValueError)

    def __getDadosSimplyCodReutilizado(self, codigoReutilizado):
        try:
            sql = """\
                    SELECT TOP 1
                            S.documento_1
                            ,S.assertividade_1
                            ,S.documento_2
                            ,S.assertividade_2
                            ,S.response
                            ,S.CodigoSolicitacao
                            ,S.Status
                            ,S.DataSolicitacao
                            ,S.DataProcessamento
                            ,S.datahora
                            ,S.response_2
                            ,S.CodigoSolicitacao_2
                            ,S.DataSolicitacao_2
                            ,S.DataProcessamento_2
                            ,S.datahora_2
                            ,S.Status_2
                            ,S.tipo_botao
                            ,S.documentoFace
                            ,S.assertividadeFace
                            ,S.qualidadeFace
                            ,S.FaceMatch
                            ,S.qualidade_1
                            ,S.qualidade_2
                            ,S.nomeDocumento
                    FROM  SIMPLY_ID S
                    WHERE CODIGO_AF = {codigoReutilizado}
                    ORDER BY S.DataProcessamento DESC
                            """.format(codigoReutilizado = codigoReutilizado)

            return self.getRecordSetExecSql(sql)
        except ValueError:
            print(ValueError)

    def getPathHostFtp(self, data):
        hostFtp = "192.168.10.20"

        d = data.split('-')
        data = date(int(d[0]), int(d[1]), int(d[2]))

        if(data >= date(2023, 1, 1)):
            #hostFtp = "FTPFF11.facta.com.br"
            hostFtp = "//FTPFF11.facta.com.br/dados_financeira_2023$/FTP/FtSiteImagens/"
        elif(data >= date(2022, 1, 1)):
            hostFtp = "//FIN-FS18.facta.com.br/dados_financeira_2022$/FTP/FtSiteImagens/"
        #elif(data >= date(2021, 1, 1)):
            #hostFtp = "FAC-FS27.facta.com.br"

        return hostFtp

    def getPastaFtp(self, data, codigoAf):
        d = data.split('-')
        anoMes = str(d[0])+str(d[1])
        dia = d[2]
        #pastaFtp = "//Fac_Financeira/"+anoMes+"/"+dia+"/"+str(codigoAf)+"/"
        pastaFtp = anoMes+"/"+dia+"/"+str(codigoAf)+"/"

        return pastaFtp

    def copyFiles(self, codigoAf, hostFtp, pastaFtp):
        #Defina as variáveis correspondentes
        dir_path_local = f"C:\\python_script\\enviadocformalizacao\\{codigoAf}"

        #Verifique se o diretório existe e, se não, crie-o
        if not os.path.exists(dir_path_local):
            os.makedirs(dir_path_local, 0o755, True)

        #self.dir_path_servidor = "//FTPFF11.facta.com.br/dados_financeira_2023$/FTP/FtSiteImagens/"+pastaFtp
        self.dir_path_servidor = hostFtp+pastaFtp

        path = self.dir_path_servidor
        arquivos = self.__getArquivosDiretorio(path)

        for arquivo in arquivos:
            if 'FRENTE' in arquivo.upper() or 'VERSO' in arquivo.upper() or 'CNH' in arquivo.upper():
                file_path_origem = join(path, arquivo)
                file_path_destino = join(dir_path_local, arquivo)

                # Copia o arquivo para a pasta local
                shutil.copy(file_path_origem, file_path_destino)

    def sendFtp(self,codigoAf, hostFtp, pastaFtpEnvio, cordigoreutilizado, novocodigo, removePath=True):
        try: 
            arquivos_copiado = []
            dir_path_local = f"C:\\python_script\\enviadocformalizacao\\{codigoAf}"

            arquivos = self.__getArquivosDiretorio(dir_path_local)
            print(arquivos)

            #self.dir_path_servidor = "//FTPFF11.facta.com.br/dados_financeira_2023$/FTP/FtSiteImagens/"+pastaFtpEnvio
            self.dir_path_servidor = hostFtp+pastaFtpEnvio

            dir_path_destino = self.dir_path_servidor

            for arquivo in arquivos:
                file_path_origem = join(dir_path_local, arquivo)

                arquivo_destino = arquivo.replace(cordigoreutilizado, novocodigo)
                file_path_destino = join(dir_path_destino, arquivo_destino)

                if(removePath == True):
                    shutil.move(file_path_origem, file_path_destino)
                else:
                    shutil.copy(file_path_origem, file_path_destino)

                arquivos_copiado.append(arquivo)

                print(f'Arquivo {arquivo} movido para: {file_path_destino}')
        except Exception as e:
            print(f"Erro ao mover aqruivo : {e}")
        finally:
            if(removePath):
                shutil.rmtree(dir_path_local)

        return arquivos_copiado
    def __getArquivosDiretorio(self, dir_path_servidor):
        try:
            path = dir_path_servidor
            print('ACESSANDO FTP: '+path)

            files = [f for f in listdir(path) if isfile(join(path, f))]

            return files
        except Exception as e:
            print('ERRO: '+str(e))

    def verificaPasta(self, hostFtp, pastaFtp):
        #self.dir_path_servidor = "//FTPFF11.facta.com.br/dados_financeira_2023$/FTP/FtSiteImagens/"+pastaFtp
        self.dir_path_servidor = hostFtp+pastaFtp

        path = self.dir_path_servidor
        files = self.__getArquivosDiretorio(path)

        tipoArquivos = []
        arquivoVazio = False;

        for file in files:
            if 'FRENTE' in file.upper():
                tipoArquivos.append('FRENTE')

                file_path = join(path, file)
                file_size = getsize(file_path)
                if(file_size == 0):
                    arquivoVazio = True

            if 'VERSO' in file.upper():
                tipoArquivos.append('VERSO')
                
                file_path = join(path, file)
                file_size = getsize(file_path)
                if(file_size == 0):
                    arquivoVazio = True

            if 'CNH' in file.upper():
                tipoArquivos.append('CNH')

                file_path = join(path, file)
                file_size = getsize(file_path)
                if(file_size == 0):
                    arquivoVazio = True
        
        if((('FRENTE' in tipoArquivos and 'VERSO' in tipoArquivos) or ('CNH' in tipoArquivos))):
            if(arquivoVazio):
                return False

            return True

        return False

    def trata_valor(self, valor):
        return valor if valor is not None else ''

    def __insertTabelaSimply(self, codigoAF,dadosSimply):
        try:

            documento_1 = self.trata_valor(dadosSimply[0].documento_1) 
            assertividade_1 = self.trata_valor(dadosSimply[0].assertividade_1) 
            documento_2 = self.trata_valor(dadosSimply[0].documento_2) 
            assertividade_2 = self.trata_valor(dadosSimply[0].assertividade_2) 
            response = self.trata_valor(dadosSimply[0].response)
            CodigoSolicitacao = self.trata_valor(dadosSimply[0].CodigoSolicitacao)
            Status = self.trata_valor(dadosSimply[0].Status)
            DataSolicitacao = self.trata_valor(dadosSimply[0].DataSolicitacao)
            DataProcessamento = self.trata_valor(dadosSimply[0].DataProcessamento)


            response_2 = self.trata_valor(dadosSimply[0].response_2)
            CodigoSolicitacao_2 = self.trata_valor(dadosSimply[0].CodigoSolicitacao_2)
            DataSolicitacao_2 = self.trata_valor(dadosSimply[0].DataSolicitacao_2)
            DataProcessamento_2 = self.trata_valor(dadosSimply[0].DataProcessamento_2)
            
            Status_2 = self.trata_valor(dadosSimply[0].Status_2)
            tipo_botao = self.trata_valor(dadosSimply[0].tipo_botao)
            documentoFace = self.trata_valor(dadosSimply[0].documentoFace)
            assertividadeFace = self.trata_valor(dadosSimply[0].assertividadeFace)
            qualidadeFace = self.trata_valor(dadosSimply[0].qualidadeFace)
            FaceMatch = self.trata_valor(dadosSimply[0].FaceMatch)
            qualidade_1 = self.trata_valor(dadosSimply[0].qualidade_1)
            qualidade_2 = self.trata_valor(dadosSimply[0].qualidade_2)
            nomeDocumento = self.trata_valor(dadosSimply[0].nomeDocumento)

            sql_insert = f"""
                INSERT INTO simply_id (
                    codigo_af,
                    documento_1,
                    assertividade_1,
                    documento_2,
                    assertividade_2,
                    response,
                    CodigoSolicitacao,
                    Status,
                    DataSolicitacao,
                    DataProcessamento,
                    datahora,
                    response_2,
                    CodigoSolicitacao_2,
                    DataSolicitacao_2,
                    DataProcessamento_2,
                    datahora_2,
                    Status_2,
                    tipo_botao,
                    documentoFace,
                    assertividadeFace,
                    qualidadeFace,
                    FaceMatch,
                    qualidade_1,
                    qualidade_2,
                    nomeDocumento,
                    origem
                ) VALUES (
                    '{codigoAF}',
                    '{documento_1}',
                    '{assertividade_1}',
                    '{documento_2}',
                    '{assertividade_2}',
                    '{response}',
                    '{CodigoSolicitacao}',
                    '{Status}',
                    '{DataSolicitacao}',
                    '{DataProcessamento}',
                     GETDATE(),
                    '{response_2}',
                    '{CodigoSolicitacao_2}',
                    '{DataSolicitacao_2}',
                    '{DataProcessamento_2}',
                     GETDATE(),
                    '{Status_2}',
                    '{tipo_botao}',
                    '{documentoFace}',
                    '{assertividadeFace}',
                    '{qualidadeFace}',
                    '{FaceMatch}',
                    '{qualidade_1}',
                    '{qualidade_2}',
                    '{nomeDocumento}',
                    'E'
                )
            """

            self.execSql(sql_insert)
        except Exception as e:
            print(e)      


    def __insertTabelaDocReutilizado(self, dados_insercao):
        try:
            
            fields = {
                'enderecoftp' : 'NULL',
                'dataprocessamento': 'NULL',
                'codigoafvinculado': 'NULL',
                'enderecoftpvinculado': 'NULL',
                'dataprocessamentovinculado': 'NULL',
            }

            for key, valor_padrao in fields.items():
                if key in dados_insercao and dados_insercao[key]:
                    fields[key] = f"'{dados_insercao[key]}'"


            # Instrução SQL de inserção para a nova tabela
            sql_insert = f"""
                INSERT INTO documentosreutilizadosafs ({', '.join(dados_insercao.keys())})
                VALUES (
                    '{dados_insercao["codigoreutilizado"]}',
                    '{dados_insercao["enderecoftporigem"]}',
                    '{dados_insercao["arquivoscopiados"]}',
                    '{dados_insercao["codigoaf"]}',

                    {fields["enderecoftp"]},
                    {fields["dataprocessamento"]},
                    {fields["codigoafvinculado"]},
                    {fields["enderecoftpvinculado"]},
                    {fields["dataprocessamentovinculado"]}
                )
            """

            self.execSql(sql_insert)
        except Exception as e:
            print(e)     

    def getDocumentosAfs(self):
        try:
            dadosAf = self.__getDadosAfs()

            for row in dadosAf:
                ID                          = str(row[0])

                CODIGO_AF                   = str(row[1])
                DATA_CADASTRO               = str(row[4])

                CODIGO_REUTILIZADO          = str(row[2])
                DATA_CADASTRO_REUTILIZADO   = str(row[5])

                CODIGO_VINCULADO = str(row[3]) if row[3] is not None else None
                
                DATA_CADASTRO_COD_VINCULADO = str(row[6])

                dt = datetime.now()
                hour    = (str('0')+str(dt.hour))   if (dt.hour < 10)   else str(dt.hour)
                file_log = 'log_erro_'+hour+'.txt'

                import logging
                logging.basicConfig(filename=file_log, level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

                try:
                    existeArquivoVinculado = True

                    hostFtpDestino = self.getPathHostFtp(DATA_CADASTRO) # Host de Origem
                    pastaFtpArquivosDestino = self.getPastaFtp(DATA_CADASTRO, CODIGO_AF) # Pasta de onde vamos reutilizar as imagens

                    existeArquivo = self.verificaPasta(hostFtpDestino, pastaFtpArquivosDestino)


                    if(CODIGO_VINCULADO is not None):
                        
                        hostFtpDestinoCodVinculado = self.getPathHostFtp(DATA_CADASTRO_COD_VINCULADO) # Host de Origem
                        pastaFtpArquivosDestinoCodVinculado = self.getPastaFtp(DATA_CADASTRO_COD_VINCULADO, CODIGO_VINCULADO) # Pasta de onde vamos reutilizar as imagens
                        existeArquivoVinculado = self.verificaPasta(hostFtpDestinoCodVinculado, pastaFtpArquivosDestinoCodVinculado)

                    if(existeArquivo == False or existeArquivoVinculado == False):
                        
                        dados_insercao = {}

                        hostFtpOrigem = self.getPathHostFtp(DATA_CADASTRO_REUTILIZADO) # Host de Origem
                        pastaFtpArquivosOrigem = self.getPastaFtp(DATA_CADASTRO_REUTILIZADO, CODIGO_REUTILIZADO) # Pasta de onde vamos reutilizar as imagens

                        #Copia os arquivos para a pasta local
                        self.copyFiles(CODIGO_AF, hostFtpOrigem, pastaFtpArquivosOrigem)

                        dados_insercao['codigoreutilizado'] = CODIGO_REUTILIZADO
                        dados_insercao['enderecoftporigem'] = hostFtpOrigem+''+pastaFtpArquivosOrigem

                        dadosSimply = self.__getDadosSimplyCodReutilizado(CODIGO_REUTILIZADO)

                        if(existeArquivo == False):
                            
                            remove_pasta = CODIGO_VINCULADO is None #Se tiver cod vinculado nao vamos remover a pasta
                            arquivos_copiados = self.sendFtp(CODIGO_AF, hostFtpDestino, pastaFtpArquivosDestino , CODIGO_REUTILIZADO, CODIGO_AF, remove_pasta)

                            dados_insercao['arquivoscopiados']          = ";".join(arquivos_copiados)
                            dados_insercao['codigoaf']          = CODIGO_AF
                            dados_insercao['enderecoftp']       = hostFtpDestino+''+pastaFtpArquivosDestino
                            dados_insercao['dataprocessamento'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

                            #Copia os dados da tabela SIMPLY_ID para a nova af
                            
                            self.__insertTabelaSimply(CODIGO_AF, dadosSimply)

                        
                            dados_insercao['codigoafvinculado']          = ""
                            dados_insercao['enderecoftpvinculado']       = ""
                            dados_insercao['dataprocessamentovinculado'] = ""



                        if(CODIGO_VINCULADO is not None and existeArquivoVinculado == False):

                            hostFtpDestino = self.getPathHostFtp(DATA_CADASTRO_COD_VINCULADO) # Host de Origem
                            pastaFtpArquivosDestino = self.getPastaFtp(DATA_CADASTRO_COD_VINCULADO, CODIGO_VINCULADO) # Pasta de onde vamos reutilizar as imagens

                            if(existeArquivoVinculado == False and existeArquivo == True):
                                arquivos_copiados = self.sendFtp(CODIGO_AF, hostFtpDestino, pastaFtpArquivosDestino, CODIGO_REUTILIZADO, CODIGO_VINCULADO)
                                dados_insercao['arquivoscopiados']  = ";".join(arquivos_copiados)
                                dados_insercao['codigoaf']          = CODIGO_AF
                                dados_insercao['enderecoftp']       = ""
                                dados_insercao['dataprocessamento'] = ""
                            else:
                                self.sendFtp(CODIGO_AF, hostFtpDestino, pastaFtpArquivosDestino, CODIGO_REUTILIZADO, CODIGO_VINCULADO)

                            dados_insercao['codigoafvinculado']          = CODIGO_VINCULADO
                            dados_insercao['enderecoftpvinculado']       = hostFtpDestino+''+pastaFtpArquivosDestino
                            dados_insercao['dataprocessamentovinculado'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                            
                            self.__insertTabelaSimply(CODIGO_VINCULADO, dadosSimply)

                    
                        
                        self.__insertTabelaDocReutilizado(dados_insercao)

                    else:
                        print("Arquivos ja existe na pasta: " + hostFtpDestino + "/" + pastaFtpArquivosDestino + "/" + str(CODIGO_AF))

                except Exception as e:
                    print(f"Erro ao acessar FTP para {pastaFtpArquivosOrigem}: {str(e)}")
                    logging.error(f"Erro ao acessar FTP para {pastaFtpArquivosOrigem}: {str(e)}")
                    continue

        except ValueError:
            print(ValueError)

