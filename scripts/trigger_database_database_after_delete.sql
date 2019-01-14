delimiter $$

create trigger database_database_after_delete	after delete on database_database for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de deletes no MongoDB da tabela database_database
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('database_database', old.id, 'D', 'N');

end
$$

delimiter;
