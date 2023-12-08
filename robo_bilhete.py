from bilhete import *
import time 

while True:
    try:
        print("Iniciando Processo bilhete...")
        print('---------------------------------------------')
        bilhete = Bilhete()
        if(bilhete.run() == False):
            print('Sem bilhete para processar')

        del bilhete
        print(time.ctime(), 'Aguardando 10 minutos  para o proximo processo...')
        time.sleep(10)
    except Exception as e:
        print(str(e))
        print('Erro ao executar a processo. Aguardando 3 segundos para reiniciar...')
        time.sleep(3)