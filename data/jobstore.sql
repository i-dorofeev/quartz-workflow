-- noinspection SqlDialectInspectionForFile
select * from JS_HIERARCHY;

insert into JS_HIERARCHY VALUES (1, NULL);

insert into JS_HIERARCHY VALUES (2, 1);

insert into JS_HIERARCHY VALUES (3, 2);
insert into JS_HIERARCHY
select 3, PARENT_ID from JS_HIERARCHY where id=2;

insert into JS_HIERARCHY VALUES (4, 3);
insert into JS_HIERARCHY
  select 4, PARENT_ID from JS_HIERARCHY where id=3;

insert into JS_HIERARCHY VALUES (5, 4);
insert into JS_HIERARCHY
  select 5, PARENT_ID from JS_HIERARCHY where id=4;

select id from JS_HIERARCHY where PARENT_ID = 2 or id = 2;

/** save new job **/
insert into JS_DATA values ('1', NULL, NULL);
insert into JS_HIERARCHY VALUES ('1', NULL);

insert into JS_DATA values ('2', NULL, NULL);
insert into JS_HIERARCHY VALUES ('2', '1');
insert into JS_HIERARCHY
  select '2', PARENT_ID from JS_HIERARCHY where id='1';

insert into JS_DATA values ('3', NULL, NULL);
insert into JS_HIERARCHY VALUES ('3', '2');
insert into JS_HIERARCHY
  select '3', PARENT_ID from JS_HIERARCHY where id='2';

insert into JS_DATA values ('4', NULL, NULL);
insert into JS_HIERARCHY VALUES ('4', '3');
insert into JS_HIERARCHY
  select '4', PARENT_ID from JS_HIERARCHY where id='3';

/**  traverse by rootId **/
select d.* from JS_DATA d
  join JS_HIERARCHY h on d.ID=h.ID
  where h.PARENT_ID='1' or h.ID='1';


DELETE from JS_HIERARCHY;