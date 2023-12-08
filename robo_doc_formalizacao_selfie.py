from DocFormalizacaoSelfie import *
import time 

while True:
    try:
        print("Iniciando processo robo Envia Documentos Formalizacao aguarde...")
        print('--------------------------------------------------------------------')

        docFormalizacao = DocFormalizacao()
        docFormalizacao.getDocumentosAfs()
        del docFormalizacao

        print('-----------------------------------------------------------------------')
        print('Fim')
        print('-----------------------------------------------------------------------')
        print(time.ctime(), 'Aguardando 30 minuto para o proximo processo...')
        time.sleep(1800)
    except Exception as e:
        print(str(e))
        print('Erro ao executar a processo. Aguardando 3 segundos para reiniciar...')
        time.sleep(3)