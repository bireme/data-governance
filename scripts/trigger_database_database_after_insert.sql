delimiter $$

create trigger database_database_after_insert	after insert on database_database for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de inserts no MongoDB da tabela database_database
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('database_database', new.id, 'I', 'N');

end
$$

delimiter;
