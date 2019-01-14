delimiter $$

create trigger biblioref_referenceanalytic_after_insert	after insert on biblioref_referenceanalytic for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        Jos� Marcello Lopes
# Data:         13.01.2019
#
# Vers�o:       1.0
#
# Objetivo:     Trigger para gerar o controle de inserts no MongoDB da tabela biblioref_referenceanalytic
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referenceanalytic', new.reference_ptr_id, 'I', 'N');

end
$$

delimiter;
