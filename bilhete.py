from datetime import date, timedelta
from datetime import datetime
import urllib.request
import os
import io
from os import listdir
from os.path import isfile, join
import shutil
import hashlib
from pprint import pprint
import base64
from  zipfile import ZipFile
from api import *
import ssl
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning



class Bilhete():

    def __init__(self):
        self.tipo_ccb = "seguro_prestamista_parametros"
        self.dir_path_servidor = "M:/PROGRAMACAO/Lidio/ccbs/"

        self.dir_path_ccb = os.getcwd()

        self.url_pdf = "https://app.factaseguradora.com.br/gerador_pdf/gerar_ccb.php?"
        self.const_token = 'api_gerar_ccb_'

        self.path_ccbZip = "C:/python_script/bilhete/"
        self.path_ccbDestino = "M:/PROGRAMACAO/Lidio/ccbs/"

    def __getTotken(self):
        try:
            data  = date.today().strftime('%Y-%m-%d')
            token = self.const_token+str(data)
            token = hashlib.md5(token.encode())
            return token.hexdigest()
        except Exception as e:
            print('ERRO AO GERAR TOKEN')

    def __getUrl(self, codigoAf, vlrComIof, iof, vlrSemIof, parcelas, capital, taxa):
        try:
            token = self.__getTotken()
            codigo = base64.b64encode(codigoAf.encode()).decode('utf-8')
            url = self.url_pdf+'codigo='+str(codigo)+'&tipo_ccb='+self.tipo_ccb+'&token='+token+"&vlrComIof="+vlrComIof+"&iof="+iof+"&vlrSemIof="+vlrSemIof+"&parcelas="+parcelas+"&capital="+capital+"&taxa="+taxa
            return url

        except Exception as e:
            print('ERRO AO GERAR URL')

    def __getUrlProduto20(self, codigoAf):
        try:
            token = self.__getTotken()
            codigo = base64.b64encode(codigoAf.encode()).decode('utf-8')

            self.tipo_ccb = "bilhete_seguro_produto_20"
            url = self.url_pdf+'codigo='+str(codigo)+'&tipo_ccb='+self.tipo_ccb+'&token='+token
            return url

        except Exception as e:
            print('ERRO AO GERAR URL')

    def __getUrlBilheteSeguroIndividual(self, codigoAf):
        try:
            token = self.__getTotken()
            codigo = base64.b64encode(codigoAf.encode()).decode('utf-8')

            self.tipo_ccb = "bilhete_seguro_individual"
            url = self.url_pdf+'codigo='+str(codigo)+'&tipo_ccb='+self.tipo_ccb+'&token='+token
            return url

        except Exception as e:
            print('ERRO AO GERAR URL')

    def __getUrlSinab(self, codigoAf):
        try:
            token = self.__getTotken()
            codigo = base64.b64encode(codigoAf.encode()).decode('utf-8')

            self.tipo_ccb = "sinab_familia_protegida"
            url = self.url_pdf+'codigo='+str(codigo)+'&tipo_ccb='+self.tipo_ccb+'&token='+token
            return url

        except Exception as e:
            print('ERRO AO GERAR URL')

    def __makePdf(self, filename, url):
        try:
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
            response = requests.get(url,verify=False)

            if response.status_code == 200:
                with open(filename + ".pdf", 'wb') as file:
                    file.write(response.content)
                print(f'O arquivo {filename}.pdf foi baixado com sucesso!')
            else:
                print(f'Erro ao baixar o arquivo. Status Code: {response.status_code}')

            '''
            response = urllib.request.urlopen(url)

            file = open(filename + ".pdf", 'wb')
            file.write(response.read())
            file.close()
            '''

            return True

        except Exception as e:
            print(str(e))
            print('ERRO AO GERAR URL 33')

    def __downloadPdf(self, codigoAf = "", vlrComIof = "", iof = "", vlrSemIof = "", parcelas  = "", capital = "", taxa = ""):
        try:
            dir_path = self.__buildDir()
            filename = dir_path+"/pdf_"+str(codigoAf)

            print('GEROU PDF BILHETE: '+str(codigoAf))
            url = self.__getUrl(codigoAf, vlrComIof, iof, vlrSemIof, parcelas, capital, taxa)

            #print(url) #LINKS
            self.__makePdf(filename, url)

            pdfName = "pdf_"+str(codigoAf)+".pdf"
            return pdfName
            
        except Exception as e:
            print('ERRO AO GERAR O PDF: '+str(e))

    def __downloadPdfSinab(self, codigoAf):
            try:
                dir_path = self.__buildDir()
                filename = dir_path+"/pdf_"+str(codigoAf)

                url = self.__getUrlSinab(codigoAf)

                self.__makePdf(filename, url)

                print('GEROU PDF CCB: '+str(codigoAf))

                pdfName = "pdf_"+str(codigoAf)+".pdf"
                return pdfName

            except Exception as e:
                print('ERRO AO GERAR O PDF: ')

    def __downloadPdfProduto20(self, codigoAf):
            try:
                dir_path = self.__buildDir()
                filename = dir_path+"/pdf_"+str(codigoAf)

                url = self.__getUrlProduto20(codigoAf)

                self.__makePdf(filename, url)
                
                print('GEROU PDF CCB: '+str(codigoAf))
                pdfName = "pdf_"+str(codigoAf)+".pdf"
                return pdfName

            except Exception as e:
                print('ERRO AO GERAR O PDF: ')

    def __downloadPdfBilheteSeguroIndividual(self, codigoAf):
            try:

                dir_path = self.__buildDir()
                filename = dir_path+"/pdf_"+str(codigoAf)

                url = self.__getUrlBilheteSeguroIndividual(codigoAf)

                self.__makePdf(filename, url)

                print('GEROU PDF CCB: '+str(codigoAf))
                pdfName = "pdf_"+str(codigoAf)+".pdf"
                return pdfName

            except Exception as e:
                print('ERRO AO GERAR O PDF: ')

    def __buildDir(self, pasta = 'ccbs'):
        try:
            dir_path = self.dir_path_ccb
            dir_path = str(dir_path)+'/'+str(pasta)

            if(os.path.exists(dir_path) == False):
                os.mkdir(dir_path)

            return dir_path

        except Exception as e:
            print('ERRO AO CRIAR PASTA')

    def makeZip(self,tipo_ccb, listPdfs):
        try:
            
            dateNow = datetime.now()
            tipo_ccb = tipo_ccb +"_"+ str(dateNow.hour) + "_" + str(dateNow.minute) + "_" + str(dateNow.second)

            ccbZip = "ccbs_"+tipo_ccb+".zip"

            with ZipFile(ccbZip, "w") as newZipFile:
                for pdf in listPdfs:
                    newZipFile.write('ccbs/'+pdf)
                    os.remove(self.dir_path_ccb+"/ccbs/" + pdf)

            return ccbZip
    
        except Exception as e:
            print(str(e))

    def moveZipCcbs(self, ccbsZip):
        try:
            print("Movendo arquivo para: "+self.path_ccbDestino+ ccbsZip)
            shutil.copyfile(self.path_ccbZip + ccbsZip, self.path_ccbDestino + ccbsZip)
            os.remove(self.path_ccbZip + ccbsZip)

        except Exception as e:
            print(str(e))
            print('ERRO AO GRAVAR LOG')

    def __getArquivosDiretorio(self, path):
        try:
            files = []
            for arquivo in listdir(path):
                if(".txt" in arquivo and arquivo not in ".pdf"):
                    files.append(arquivo)
            
            return files

        except Exception as e:
            return False

    def getDadosTxt(self):
        try:
            arquivo = self.__getArquivosDiretorio(self.dir_path_servidor)


            if(len(arquivo) == 0 or len(arquivo) == False):
                return None, False

            path_arquivo = self.dir_path_servidor + arquivo[0]

            arquivo = open(path_arquivo, 'r')
            dados = []

            for linha in arquivo:
                linha = linha.replace("\n", "")
                dados.append(linha)

            arquivo.close()

            if(len(dados) == 0):
                dateNow = datetime.now()
                novo_arquivo = path_arquivo.replace(".txt", "")+"_arquivo_vazio_"+ str(dateNow.hour) + "_" + str(dateNow.minute) + "_" + str(dateNow.second)
                os.rename(path_arquivo, novo_arquivo)
                return None, False

            validTipo = False

            tipo = str(dados[0]).split(';')[0]

            if(tipo == "P"):
                validTipo = True
            if(tipo == "S"):
                validTipo = True
            if(tipo == "P20"):
                validTipo = True
            if(tipo == "SI"):
                validTipo = True

            if(validTipo == False):
                dateNow = datetime.now()
                novo_arquivo = path_arquivo.replace(".txt", "")+"_arquivo_sem_tipo_"+ str(dateNow.hour) + "_" + str(dateNow.minute) + "_" + str(dateNow.second)
                os.rename(path_arquivo, novo_arquivo)
                return None, False

            os.remove(path_arquivo)
            return tipo, dados

        except OSError as e:
            return None, False

    def run(self):

        
        dadosTxt = []

        tipo, dadosTxt = self.getDadosTxt()
        

        if(dadosTxt == False):
            return False
		
        if(tipo == "P"):
            self.runPrestamista(dadosTxt)
        if(tipo == "S"):
            self.runSinab(dadosTxt)
        if(tipo == "P20"):
            self.runProduto20(dadosTxt)
        if(tipo == "SI"):
            self.runBilheteSeguroIndividual(dadosTxt)

    def getAfsBilhetes(self):

        cliente_cpf = '0198120507'
        retornoApiSeguro    = API(str(self.ambiente)).seguro(str(cliente_cpf), str(retornoToken))
        retornoApiSeguroAF  = json.JSONEncoder().encode(retornoApiSeguro)


    def gravaLog(self, txt = ''):
        try:
            dateNow = datetime.now()
            log_erro = "log_erro_"+ str(dateNow.hour) + "_" + str(dateNow.minute) + "_" + str(dateNow.second)+".log"
            with io.open(self.dir_path_servidor+log_erro, 'a', encoding = 'utf-8') as f:
                f.writelines(txt)
        except Exception as e:
            print('ERRO AO GRAVAR LOG')

    def runPrestamista(self, dadosTxt):
        try:
            listPdfs = []

            for dtxt in dadosTxt:
                row = str(dtxt).split(';')

                if(len(row) == 8):
                   pdfName = self.__downloadPdf((row[1]).strip(), (row[2]).strip(), (row[3]).strip(), (row[4]).strip(), (row[5]).strip(), (row[6]).strip(), (row[7]).strip())
                   listPdfs.append(pdfName)
                else:
                   dtxt = str(dtxt).strip()
                   if(dtxt == ''):
                       dtxt = 'Vazio'

                   self.gravaLog('Erro com registro: '+str(dtxt)+'\n')

            #quit()
            ccbZip = self.makeZip('prestamista', listPdfs)
            self.moveZipCcbs(ccbZip)

        except Exception as e:
            print(str(e))
            print('ERRO AO RODAR ROBO.')

    def runSinab(self, dadosTxt):
        try:
            listPdfs = []
            for dtxt in dadosTxt:
                row = str(dtxt).split(';')
                pdfName = self.__downloadPdfSinab(str(row[1]).strip())

                listPdfs.append(pdfName)

            ccbZip = self.makeZip('sinab', listPdfs)
            self.moveZipCcbs(ccbZip)

        except Exception as e:
            print(str(e))
            print('ERRO AO RODAR ROBO.')


    def runProduto20(self, dadosTxt):
        try:
            listPdfs = []
            for dtxt in dadosTxt:
                row = str(dtxt).split(';')
                pdfName = self.__downloadPdfProduto20(str(row[1]).strip())
                listPdfs.append(pdfName)
            
            ccbZip = self.makeZip('sinab', listPdfs)
            self.moveZipCcbs(ccbZip)

        except Exception as e:
            print('ERRO AO RODAR ROBO.')


    def runBilheteSeguroIndividual(self, dadosTxt):
        try:
            listPdfs = []
            for dtxt in dadosTxt:
                row = str(dtxt).split(';')
                pdfName = self.__downloadPdfBilheteSeguroIndividual(str(row[1]).strip())
                listPdfs.append(pdfName)
                
            ccbZip = self.makeZip('seguro_individual', listPdfs)
            self.moveZipCcbs(ccbZip)

        except Exception as e:
            print('ERRO AO RODAR ROBO.')
