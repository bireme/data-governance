delimiter $$

create trigger biblioref_reference_after_update	after update on biblioref_reference for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de updates no MongoDB da tabela biblioref_reference
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_reference', new.id, 'U', 'N');

end
$$

delimiter;
