delimiter $$

create trigger biblioref_referencealternateid_after_delete	after delete on biblioref_referencealternateid for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        Jos� Marcello Lopes
# Data:         13.01.2019
#
# Vers�o:       1.0
#
# Objetivo:     Trigger para gerar o controle de deletes no MongoDB da tabela biblioref_referencealternateid
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referencealternateid', old.id, 'D', 'N');

end
$$

delimiter;
