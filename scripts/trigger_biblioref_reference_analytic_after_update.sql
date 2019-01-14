delimiter $$

create trigger biblioref_referenceanalytic_after_update	after update on biblioref_referenceanalytic for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de updates no MongoDB da tabela biblioref_referenceanalytic
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referenceanalytic', new.reference_ptr_id, 'U', 'N');

end
$$

delimiter;
