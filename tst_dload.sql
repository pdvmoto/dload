
--
-- tst_dload.sql: create tables and parent-data for loading
-- origin: also tst_ins_label.py
-- 

drop table tst_item_label ;
drop table tst_item ;
drop table tst_parent ;
drop table tst_source ;

drop table tst_load ;

set echo on
set feedback on

create table tst_load ( 
  name varchar2(32)
, payload varchar(128)
);

create table tst_source (
  id          number generated always as identity not null primary key
, created_dt  date default sysdate
, src_name    varchar2 ( 32 ) 
, src_descr   varchar2 ( 256 ) 
, src_comment varchar2 ( 256 )
--, primary key ( id )
) ;

create table tst_parent (
  id          number generated always as identity not null primary key
, created_dt  date              default sysdate
, par_name    varchar2 ( 32 )   not null
, src_id      number            not null
, par_comment varchar2 ( 256 )
, constraint tst_parent_fk_src foreign key ( src_id ) references tst_source ( id ) 
-- consider name unique
);

create table tst_item ( 
  id          number generated always as identity not null primary key
, created_dt  date              default sysdate
, par_id      number            not null
, src_id      number            not null
, itm_name    varchar2 ( 128 )  not null
, constraint tst_item_fk_par foreign key ( par_id ) references tst_parent ( id ) 
, constraint tst_item_fk_src foreign key ( src_id ) references tst_source ( id ) 
);

-- indexes on item, check validity after direct-inserts
create index tst_item_name on tst_item ( itm_name ) ;
create index tst_item_par  on tst_item ( par_id, id ) ;

create table tst_item_label (
  id          number generated always as identity not null primary key
, created_dt  date              default sysdate 
, itm_id      number            not null
, src_id      number            not null
, label       varchar2 ( 128 )  not null
, label_score number            default 0.001 not null
, label_comment varchar2 ( 256 )
, constraint tst_itl_fk_itm foreign key ( itm_id ) references tst_item ( id ) 
, constraint tst_itl_fk_src foreign key ( src_id ) references tst_source ( id ) 
);

-- put some seed data in..
insert into tst_source ( src_name, src_descr, src_comment ) 
                values ( 'src1', 'src1_desc', 'src1_comment') ;

insert into tst_source ( src_name, src_descr, src_comment ) 
                values ( 'src2', 'src2_desc', 'src2_comment'),  
                       ( 'src3', 'src3_desc', 'src3_comment'),  
                       ( 'src4', 'src4_desc', 'src4_comment'),  
                       ( 'src5', 'src5_desc', 'src5_comment'),  ;

-- 10 more sources
insert into tst_source (src_name, src_descr )
select 
 'src' ||  to_char ( level)   src_name
, to_char ( level ) || 'long description' ||  to_char ( level)   src_desc
from dual connect by level < 11 ;


insert into tst_parent ( par_name, src_id, par_comment ) 
 select 'par1_name', max_src_id, 'par1_comment' 
   from  ( select max ( id ) max_src_id from tst_source) mxsrc
;

-- add 100 parents
insert into tst_parent (par_name, src_id, par_comment )
select 
 'parent_' ||  to_char ( level)   par_name
, mod (level, 10) +1 as src_id
, 'Par:'|| to_char( level )|| ' long description' ||  to_char ( level)   src_desc
from dual connect by level < 101;


-- add a lot of items...
insert into tst_item ( src_id, par_id, itm_name )
select max_src_id, max_par_id, 'item1_name'
from ( select max ( id ) max_src_id from tst_source ) mxsrc
   , ( select max ( id ) max_par_id from tst_parent ) mxpar
;

commit ; 


-- to test Direct-load + append: Add records to tst_Items..
-- par_id, src_id, item_name  (two relations, 1 name...)



-- 10 sources
insert into tst_source (src_name, src_descr )
select 
 'src' ||  to_char ( level)   src_name
, to_char ( level ) || 'long description' ||  to_char ( level)   src_desc
from dual connect by level < 11 ;


-- 100 parents
insert into tst_parent (par_name, src_id, par_comment )
select 
 'parent_' ||  to_char ( level)   par_name
, mod (level, 10) +1 as src_id
, 'Par:'|| to_char( level )|| ' long description' ||  to_char ( level)   src_desc
from dual connect by level < 101;

-- some items..
insert /*+ APPEND */  into tst_item ( par_id, src_id, itm_name )
select 
  mod ( level, 100) + 1 par_id
, mod ( level, 10 ) + 1 src_id
, to_char( level) || 'name'
from dual connect by level < 11;

commit ;

analyze table tst_item ;

select count (*) nr_extenst from user_extents
where 1=1 
and segment_name like 'TST_ITEM';

select ut.table_name, ut.num_rows, ut.empty_blocks 
-- , ut.* 
from user_tables  ut
where ut.table_name like 'TST_ITEM'; 

truncate table tst_item ;

analyze table tst_item compute statistics ; 

select ut.table_name, ut.num_rows, ut.empty_blocks 
-- , ut.* 
from user_tables  ut
where ut.table_name like 'TST_ITEM'; 

