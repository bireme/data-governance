-- DROP TABLE st_desc_quali_checktag;

CREATE TABLE st_desc_quali_checktag (
	id 				int,
	alternate_id 	varchar(55) NULL, 
	fi 				varchar(50) NULL,
	issn 			varchar(50) NULL,
	check_tags 		varchar(100) NULL,
	tipo_desc		varchar(20) null,
	dt_in 			timestamp null,
	dt_up 			timestamp null
);
