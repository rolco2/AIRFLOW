/*데이터로딩 및 언로딩*/
USE ROLE SYSADMIN;

USE WAREHOUSE COMPUTE_WH;

/*데이터베이스 생성*/
CREATE OR REPLACE DATABASE DEMO6_DB
    COMMENT = "Database for all Chapter 6 example";
/*스키마 생성*/
CREATE OR REPLACE SCHEMA WS   COMMENT = "Schema for worksheet Insert Examples";
CREATE OR REPLACE SCHEMA UI   COMMENT = "Schema for web UI Uploads";
CREATE OR REPLACE SCHEMA SNOW COMMENT = "Schema for SnowSQL Loads";

CREATE OR REPLACE WAREHOUSE LOAD_WH
    COMMENT = "Warehouse for CH 6 Load Examples";


/* 정형 및 반정형 데이터를 위한 단일행 삽입*/
USE ROLE SYSADMIN;

CREATE OR REPLACE TABLE TABLE1
(
     ID        integer 
,    F_NAME    string
,    L_NAME    STRING
,    CITY      STRING
)
COMMENT = "Single-Row Insert for Structured Data using Explicitly Specified Values";
/*데이터 삽입*/
INSERT INTO TABLE1 VALUES(1, 'Anthony', 'Robinson', 'Atlanta');
/*데이터 확인*/
SELECT * FROM TABLE1;

/*데이터 추가 삽입*/
INSERT INTO TABLE1 VALUES(2, 'PEGGY', 'MATHISON', 'BIRMINGHAM');
/*데이터 확인*/
SELECT * FROM TABLE1;
붕

/* 반정형 데이터 삽입을 위한 테이블 생성*/
CREATE OR REPLACE TABLE TABLE2
(
     ID          INTEGER 
,    VARIANT1    VARIANT

)
COMMENT = "Single-Row Insert for SEMI-Structured JSON Data ";

/*JSON 형식 데이터 INSERT*/
INSERT INTO TABLE2 
SELECT 1, PARSE_JSON(' {"F_NAME": "ANTHONY", "L_NAME" : "ROBBINSON", "CITY" : "ATLANTA" }');
INSERT INTO TABLE2 
SELECT 2, PARSE_JSON(' {"F_NAME": "PEGGY", "L_NAME" : "MATHISON", "CITY" : "BIRMINGHAM" }');

SELECT * FROM TABLE2;

/*멀티 행 삽입*/
CREATE OR REPLACE TABLE TABLE3
(
     ID        integer 
,    F_NAME    string
,    L_NAME    STRING
,    CITY      STRING
)
COMMENT = "Multi-Row Insert for Structured Data using Explicitly Specified Values";

/*데이터 insert*/

INSERT INTO TABLE3 (ID, F_NAME, L_NAME, CITY) VALUES
    (1, 'Anthony', 'Robinson', 'Atlanta')
,   (2, 'PEGGY', 'MATHISON', 'BIRMINGHAM')
;

SELECT * FROM TABLE3;


/*WITH 절 INSERT*/
CREATE OR REPLACE TABLE TABLE4
(
     ID        integer 
,    F_NAME    string
,    L_NAME    STRING
,    CITY      STRING
);

INSERT INTO TABLE4 (ID, F_NAME, L_NAME, CITY) 
WITH CTE AS (
    SELECT  ID
         ,  F_NAME
         ,  L_NAME
         ,  CITY
      FROM  TABLE3
)
SELECT * fROM CTE;

SELECT * FROM TABLE4;

/*JOIN 절 INSERT*/
CREATE OR REPLACE TABLE TABLE8
(
     ID        integer 
,    ZIP_CODE  integer
,    CITY      STRING
,    STATE     STRING
);
CREATE OR REPLACE TABLE TABLE9
(
     ZIP_CODE  integer
,    CITY      STRING
,    STATE     STRING
,    L_NAME    STRING
);
CREATE OR REPLACE TABLE TABLE10
(
     ID        integer 
,    ZIP_CODE  integer
,    CITY      STRING
,    L_NAME     STRING
);

INSERT INTO TABLE8 VALUES(1, 30301, 'BIRMINGHAM', 'ALABAMA');
INSERT INTO TABLE8 VALUES(2, 40301, 'ATLANTA', 'GEORGIA');
INSERT INTO TABLE9 VALUES(30301, 'BIRMINGHAM', 'ALABAMA','AA');
INSERT INTO TABLE9 VALUES(40301, 'ATLANTA', 'GEORGIA','BB');

INSERT INTO TABLE10 (ID, ZIP_CODE, CITY, L_NAME)
SELECT A.ID, A.ZIP_CODE, A.CITY, B.L_NAME
FROM       TABLE8  A
INNER JOIN TABLE9  B ON B.ZIP_CODE = A.ZIP_CODE ;

SELECT * FROM TABLE10;

/*반정형 데이터 INSERT*/
CREATE OR REPLACE TABLE TABLE11 
(VARIANT1 VARIANT) ;

INSERT INTO TABLE11
SELECT parse_json(column1)
from values('{"_id"  : "1",
              "name" : {"first" : "anthony","second" : "Robinson"},
              "company" : "Pascal",
              "email"   : "abc@pascal.com",
              "phone"   : "0101112222"}'),
              ('{"_id"  : "2",
              "name" : {"first" : "PEGGY","second" : "MATHISON"},
              "company" : "Ada",
              "email"   : "abc@Ada.com",
              "phone"   : "01022223333"}')
;


/* 멀티 테이블 INSERT*/

CREATE OR REPLACE TABLE TABLE15 
( ID INTEGER, FIRST_NAME STRING, LAST_NAME STRING, CITY_NAME STRING) ;

INSERT INTO TABLE15 (ID,FIRST_NAME, LAST_NAME,CITY_NAME ) VALUES 
(1, '김', '철수', '서울'),
(2, '이', '영희', '인천'),
(3, '박', '주영', '부천'),
(6, '나', '문희', '안양') ;


CREATE OR REPLACE TABLE TABLE16
( ID INTEGER, FIRST_NAME STRING, LAST_NAME STRING, CITY_NAME STRING) ;

CREATE OR REPLACE TABLE TABLE17 
( ID INTEGER, FIRST_NAME STRING, LAST_NAME STRING, CITY_NAME STRING) ;


INSERT ALL
    WHEN ID < 5 THEN 
        INTO TABLE16
    WHEN ID < 3 THEN 
        INTO TABLE16
        INTO TABLE17
    WHEN ID = 1 THEN
        INTO TABLE16 (ID, FIRST_NAME) VALUES (ID, FIRST_NAME)
    ELSE 
        INTO TABLE17
SELECT ID, FIRST_NAME, LAST_NAME, CITY_NAME FROM TABLE15;


SELECT * FROM TABLE16 ORDER BY ID;
SELECT * FROM TABLE17;


/*ARRAY_INSERT*/

CREATE OR REPLACE TABLE TABLE18
(
    ARRAY VARIANT
)
COMMENT = "Insert Array" ;


INSERT INTO TABLE18
SELECT ARRAY_INSERT(array_construct(0,1,2,3), 4, 4);
INSERT INTO TABLE18
SELECT ARRAY_INSERT(array_construct(0,1,2,3), 7, 4);

SELECT * FROM TABLE18;

/*객체 삽입*/
CREATE OR REPLACE TABLE TABLE19
(
    OBJECT VARIANT
)
COMMENT = "Insert object";

INSERT INTO TABLE19
SELECT OBJECT_INSERT(OBJECT_CONSTRUCT('a',1,'b',2,'c',3),'d',4);

SELECT * FROM TABLE19;

/* null이 포함되면 insert 가 안된다. */
INSERT INTO TABLE19
SELECT OBJECT_INSERT(object_construct('a',1,'b',2,'c',3), 'd', ' ');

INSERT INTO TABLE19
SELECT OBJECT_INSERT(object_construct('a',1,'b',2,'c',3), 'd', 'null');

INSERT INTO TABLE19
SELECT OBJECT_INSERT(object_construct('a',1,'b',2,'c',3), 'd', null);

INSERT INTO TABLE19
SELECT OBJECT_INSERT(object_construct('a',1,'b',2,'c',3), null,'d');

SELECT * FROM TABLE19;







