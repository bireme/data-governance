truncate table control_updates_bigdata_rows;

delete from control_updates_bigdata_rows where object = 'biblioref_referencecomplement';

--

select * from control_updates_bigdata_rows;
select * from control_updates_bigdata_rows order by 1 desc;
select count(*) from control_updates_bigdata_rows;

select * from control_updates_bigdata_rows where processing_indicator = 'N';
select count(*) from control_updates_bigdata_rows where processing_indicator = 'N';

update control_updates_bigdata_rows set processing_indicator = 'N';

select * from control_updates_bigdata_rows where processing_indicator = 'Y';
select count(*) from control_updates_bigdata_rows where processing_indicator = 'Y';

update control_updates_bigdata_rows set processing_indicator = 'Y';

--

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'biblioref_reference', id, 'I', 'N' from biblioref_reference where id not in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_reference');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'biblioref_referencealternateid', id, 'I', 'N' from biblioref_referencealternateid where id not in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencealternateid');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'biblioref_referenceanalytic', reference_ptr_id, 'I', 'N' from biblioref_referenceanalytic where reference_ptr_id not in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referenceanalytic');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'biblioref_referencecomplement', id, 'I', 'N' from biblioref_referencecomplement where id not in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencecomplement');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'biblioref_referencelocal', id, 'I', 'N' from biblioref_referencelocal where id not in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencelocal');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'biblioref_referencesource', reference_ptr_id, 'I', 'N' from biblioref_referencesource where reference_ptr_id not in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencesource');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'database_database', id, 'I', 'N' from database_database where id not in (select id_row_object from control_updates_bigdata_rows where object = 'database_database');

insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator)
select 'main_descriptor', id, 'I', 'N' from main_descriptor where id not in (select id_row_object from control_updates_bigdata_rows where object = 'main_descriptor');

--

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      biblioref_reference t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object 
where 
      (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_reference' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'biblioref_reference' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      biblioref_referencealternateid t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object 
where 
      (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencealternateid' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'biblioref_referencealternateid' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      biblioref_referenceanalytic t right join control_updates_bigdata_rows cubdr on t.reference_ptr_id = cubdr.id_row_object 
where 
      (t.reference_ptr_id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referenceanalytic' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'biblioref_referenceanalytic' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      biblioref_referencecomplement t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object 
where 
      (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencecomplement' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'biblioref_referencecomplement' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      biblioref_referencelocal t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object 
where 
      (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencelocal' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'biblioref_referencelocal' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      biblioref_referencesource t right join control_updates_bigdata_rows cubdr on t.reference_ptr_id = cubdr.id_row_object 
where 
      (t.reference_ptr_id in (select id_row_object from control_updates_bigdata_rows where object = 'biblioref_referencesource' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'biblioref_referencesource' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      database_database t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object 
where 
      (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'database_database' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'database_database' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

select 
      cubdr.action, cubdr.id_control_update, t.*
from 
      main_descriptor t right join control_updates_bigdata_rows cubdr on t.id = cubdr.id_row_object 
where 
      (t.id in (select id_row_object from control_updates_bigdata_rows where object = 'main_descriptor' and processing_indicator = 'N') or cubdr.action = 'D') and 
      cubdr.object = 'main_descriptor' and
      cubdr.processing_indicator = 'N' 
order by 
      cubdr.instant;

--

select * from biblioref_reference order by id desc limit 10;

--

delete from biblioref_reference where id = 968825;