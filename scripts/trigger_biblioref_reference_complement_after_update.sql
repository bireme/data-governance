delimiter $$

create trigger biblioref_referencecomplement_after_update	after update on biblioref_referencecomplement for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        Jos� Marcello Lopes
# Data:         13.01.2019
#
# Vers�o:       1.0
#
# Objetivo:     Trigger para gerar o controle de updates no MongoDB da tabela biblioref_referencecomplement
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referencecomplement', new.id, 'U', 'N');

end
$$

delimiter;
