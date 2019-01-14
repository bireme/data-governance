# ----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Serviço sincronização de dados da tabela biblioref_referencesource do MySQL para o MongoDB
#
# ----------------------------------------------------------------------------------------------------------------------

#
# Bibliotecas
#

from datetime import datetime
import pymysql
from pymongo import MongoClient

#
# Conexão MySQL
#

ConexaoMySQL = pymysql.connect(user='dba-bir', password='#Dbasql67!', host='192.168.1.20', database='fi_admin_bigdata')

CursorSelectMySQL = ConexaoMySQL.cursor()
CursorUpdateMySQL = ConexaoMySQL.cursor()

#
# Conexão MongoDB
#

ConexaoMongoDB = MongoClient('mongodb://admin:#Dbasql67!@192.168.1.230:27017,192.168.1.231:27017,192.168.1.232:27017')
BancoDadosMongoDB = ConexaoMongoDB.fi_admin
CollectionMongoDB = BancoDadosMongoDB.biblioref_referencesource

#
# Colunas da query utilizada no MySQL
#

ColunasQuerySelectMySQL = ('action','id_control_update','reference_ptr_id','individual_author_monographic','corporate_author_monographic','title_monographic','english_title_monographic','pages_monographic','volume_monographic','individual_author_collection','corporate_author_collection','title_collection','english_title_collection','total_number_of_volumes','title_serial','volume_serial','issue_number','issn','thesis_dissertation_leader','thesis_dissertation_institution','thesis_dissertation_academic_title','publisher','edition','publication_city','symbol','isbn','publication_country_id'
,'flag_bigdata')

#
# Query que será executada no MySQL
#

QuerySelectMySQL = """select cubdr.action, cubdr.id_control_update, if(cubdr.action='D', cubdr.id_row_object, t.reference_ptr_id),  individual_author_monographic, corporate_author_monographic, title_monographic, english_title_monographic, pages_monographic, volume_monographic, individual_author_collection, corporate_author_collection, title_collection, english_title_collection, total_number_of_volumes, title_serial, volume_serial, issue_number,issn, thesis_dissertation_leader, thesis_dissertation_institution, thesis_dissertation_academic_title, publisher, edition, publication_city, symbol, isbn, publication_country_id, 'L' as flag_bigdata from biblioref_referencesource t right join control_updates_bigdata_rows cubdr on t.reference_ptr_id = cubdr.id_row_object where (t.reference_ptr_id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencesource' and cubdr.object = 'biblioref_referencesource' and processing_indicator = 'N') or cubdr.action = 'D') and cubdr.processing_indicator = 'N' order by cubdr.instant"""

CursorSelectMySQL.execute(QuerySelectMySQL)

#
# Inicializa estrutura do documento MongodB
#

DocumentMongoDB = dict()

#
# Loop que percorre as linhas selecionadas na query do MySQL
# para gerar o insert no Mongo
#

for (row) in CursorSelectMySQL:

    #
    # Atribui ao Id default do Mongo o mesmo Id do MySQL
    #

    DocumentMongoDB['_id'] = str(row[2])

    #
    # Inicia loop para construção da estrutura do documento Mongo
    #

    for i in range(0, len(row)):

        if i < 2:

            #
            # Se a coluna é o Id da tabela origem, a mesma é desprezada
            #

            continue

        else:

            if row[0] != 'D':

                LinhaMongoDB = "".join(ColunasQuerySelectMySQL[i])

                #
                # Converte para string
                #

                Coluna = str(row[i])

                #
                # Adiciona a coluna e valor a estrutura da linha
                #

                DocumentMongoDB[LinhaMongoDB] = Coluna

    #
    # Inseri linha no MongoDB
    #

    if row[0] == 'I':

        CollectionMongoDB.insert_one(DocumentMongoDB)

    #
    # Atualiza linha no MongoDB
    #

    if row[0] == 'U':

        CollectionMongoDB.update_one({ '_id' : str(row[2])}, { '$set' : DocumentMongoDB })

    #
    # Apaga linha no MongoDB
    #

    if row[0] == 'D':

        CollectionMongoDB.delete_one({ '_id' : str(row[2])})

    #
    # Atualiza indicador de linha já processada no MySQL
    #

    QueryUpdateMySQL = """update control_updates_bigdata_rows set processing_indicator = 'Y' where id_control_update = %i""" % (row[1])

    CursorUpdateMySQL.execute(QueryUpdateMySQL)

    ConexaoMySQL.commit()

    Action = {'I':'Insert', 'U':'Update', 'D':'Delete'}

    print('{} {} {}'.format(datetime.now(), Action[row[0]], DocumentMongoDB))

    print('')
    print('---')
    print('')

#
# Encerra conexão MySQL
#

CursorSelectMySQL.close()

ConexaoMySQL.close()
