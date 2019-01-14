delimiter $$

create trigger biblioref_referenceanalytic_after_delete	after delete on biblioref_referenceanalytic for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de deletes no MongoDB da tabela biblioref_referenceanalytic
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referenceanalytic', old.reference_ptr_id, 'D', 'N');

end
$$

delimiter;
