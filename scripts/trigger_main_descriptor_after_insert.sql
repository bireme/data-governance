delimiter $$

create trigger main_descriptor_after_insert	after insert on main_descriptor for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de inserts no MongoDB da tabela main_descriptor
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('main_descriptor', new.id, 'I', 'N');

end
$$

delimiter;
