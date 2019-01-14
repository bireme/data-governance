delimiter $$

create trigger biblioref_referencealternateid_after_update	after update on biblioref_referencealternateid for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de updates no MongoDB da tabela biblioref_referencealternateid
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referencealternateid', new.id, 'U', 'N');

end
$$

delimiter;
