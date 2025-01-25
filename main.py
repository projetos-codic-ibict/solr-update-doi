from datetime import datetime
import pysolr

# Configuração do Solr
solr_url = 'http://localhost:8080/solr/biblio'  # Substitua pelo URL do seu Solr e nome do core
solr = pysolr.Solr(solr_url, timeout=60)

# Caminho para o arquivo .tsv
arquivo_tsv = '/home/jesielsilva/oasisbr_doi.tsv'  # Substitua pelo caminho do seu arquivo .tsv

# Caminho para o arquivo de log
arquivo_log = 'log.txt'  # Nome do arquivo de log

# Função para registrar logs
def registrar_log(mensagem):
    """
    Registra uma mensagem no arquivo de log com a data e hora atual.
    """
    with open(arquivo_log, 'a', encoding='utf-8') as log:
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log.write(f"{data_hora} - {mensagem}\n")

try:
    # Abre o arquivo .tsv e processa cada linha
    with open(arquivo_tsv, 'r', encoding='utf-8') as file:
        next(file)  # Pula o cabeçalho (se houver)
        
        contador_doi = 0  # Contador de registros com DOIs
        contador_atualizacoes = 0  # Contador para controlar o número de atualizações
        documentos_atualizados = []  # Lista para armazenar os documentos atualizados

        for linha in file:
            try:
                # Divide a linha em colunas
                colunas = linha.strip().split('\t')

                # Extrai os dados
                doi = colunas[0]
                oasisbr_id = colunas[1]
                # title = colunas[2]
                # year = colunas[3]

                # Consulta o documento pelo oasisbr_id
                results = solr.search(f'id:{oasisbr_id}')

                # Verifica se o documento foi encontrado
                if len(results) > 0:
                    documento = results.docs[0]  # Pega o primeiro documento retornado

                    # Verifica se o campo "dc.identifier.doi.none.fl_str_mv" já existe no documento
                    if 'dc.identifier.doi.none.fl_str_mv' not in documento:
                        # Adiciona o novo campo "dc.identifier.doi.none.fl_str_mv" ao documento
                        documento['dc.identifier.doi.none.fl_str_mv'] = doi

                        # Adiciona o documento atualizado à lista
                        documentos_atualizados.append(documento)
                        contador_atualizacoes += 1

                        registrar_log(f"Documento {oasisbr_id} atualizado com o campo 'dc.identifier.doi.none.fl_str_mv': {doi}")
                    else:
                        contador_doi += 1
                        registrar_log(f"O campo 'dc.identifier.doi.none.fl_str_mv' já existe no documento {oasisbr_id}")
                else:
                    registrar_log(f"Nenhum documento encontrado com o ID: {oasisbr_id}")

                # Realiza o commit a cada 1000 registros atualizados
                if contador_atualizacoes >= 1000:
                    solr.add(documentos_atualizados)  # Envia os documentos atualizados de volta ao Solr
                    solr.commit()  # Confirma a atualização
                    registrar_log(f"Commit realizado para {contador_atualizacoes} documentos atualizados.")
                    
                    contador_doi = contador_doi + contador_atualizacoes
                    # Reinicia o contador e a lista de documentos atualizados
                    contador_atualizacoes = 0
                    documentos_atualizados = []

            except Exception as e:
                registrar_log(f"Erro ao processar o registro {oasisbr_id}: {e}")
                # Reinicia o contador e a lista de documentos atualizados
                contador_atualizacoes = 0
                documentos_atualizados = []


        # Realiza o commit final para os documentos restantes que não atingiram 1000
        if contador_atualizacoes > 0:
            solr.add(documentos_atualizados)  # Envia os documentos atualizados de volta ao Solr
            solr.commit()  # Confirma a atualização
            registrar_log(f"Commit final realizado para {contador_atualizacoes} documentos atualizados.")
        registrar_log(f"Total de registros com DOIs: {contador_doi}")

except Exception as e:
    registrar_log(f"Total de registros com DOIs: {contador_doi}")
    registrar_log(f"Erro geral ao processar o arquivo ou atualizar o Solr: {e}")