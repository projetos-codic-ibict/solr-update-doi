from datetime import datetime
import pysolr

# Configuração do Solr
solr_url = 'http://localhost:8080/solr/biblio'  # Substitua pelo URL do seu Solr e nome do core
solr = pysolr.Solr(solr_url, timeout=600)

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
        for linha in file:
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

                    # Atualiza o documento no Solr
                    solr.add([documento])  # Envia o documento atualizado de volta ao Solr
                    solr.commit()  # Confirma a atualização

                    registrar_log(f"Documento {oasisbr_id} atualizado com o campo 'dc.identifier.doi.none.fl_str_mv': {doi}")
                else:
                    registrar_log(f"O campo 'dc.identifier.doi.none.fl_str_mv' já existe no documento {oasisbr_id}")
            else:
                registrar_log(f"Nenhum documento encontrado com o ID: {oasisbr_id}")

except Exception as e:
    registrar_log(f"Erro ao processar o arquivo ou atualizar o Solr: {e}")