# ----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Serviço sincronização de dados da tabela biblioref_reference do MySQL para o MongoDB
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
CollectionMongoDB = BancoDadosMongoDB.biblioref_reference

#
# Colunas da query utilizada no MySQL
#

ColunasQuerySelectMySQL = ('action','id_control_update','id','created_time','updated_time','status','reference_title','cooperative_center_code','literature_type','treatment_level','electronic_address','record_type','descriptive_information','text_language','internal_note','publication_date','publication_date_normalized','total_number_of_references','time_limits_from','time_limits_to','person_as_subject','non_decs_region','abstract','transfer_date_to_database','author_keyword','item_form','type_of_computer_file','type_of_cartographic_material','type_of_journal','type_of_visual_material','specific_designation_of_the_material','general_note','formatted_contents_note','additional_physical_form_available_note','reproduction_note','original_version_note','institution_as_subject','local_descriptors','software_version','LILACS_original_id','created_by_id','updated_by_id','check_tags','publication_type','LILACS_indexed','BIREME_reviewed','interoperability_source','indexer_cc_code','flag_bigdata')

#
# Query que será executada no MySQL
#

QuerySelectMySQL = """select cubdr.action, cubdr.id_control_update, if(cubdr.action='D', cubdr.id_row_object, t.id), created_time, updated_time, status, reference_title, cooperative_center_code, literature_type, treatment_level, electronic_address, record_type, descriptive_information, text_language, internal_note, publication_date, publication_date_normalized, total_number_of_references, time_limits_from, time_limits_to, person_as_subject, non_decs_region, abstract, transfer_date_to_database, author_keyword, item_form, type_of_computer_file, type_of_cartographic_material, type_of_journal, type_of_visual_material, specific_designation_of_the_material, general_note, formatted_contents_note, additional_physical_form_available_note, reproduction_note, original_version_note, institution_as_subject, local_descriptors, software_version, LILACS_original_id, created_by_id, updated_by_id, check_tags, publication_type, LILACS_indexed, BIREME_reviewed, interoperability_source, indexer_cc_code, 'L' as flag_bigdata from biblioref_reference t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object where (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_reference' and cubdr.object = 'biblioref_reference' and processing_indicator = 'N') or cubdr.action = 'D') and cubdr.processing_indicator = 'N' order by cubdr.instant"""

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
