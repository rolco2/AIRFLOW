use role sysadmin;


/* 새로운 계정 생성 */

USE ROLE USERADMIN;

CREATE OR REPLACE USER ADAM
PASSWORD = '123'
LOGIN_NAME = ADAM
DISPLAY_NAME = ADAM
EMAIL = 'GYWNS9559@NAVER.COM'
MUST_CHANGE_PASSWORD = TRUE;

/* 동적 데이터 마스킹을 테스트하기 위해 인적 자원 롤 생성*/
USE ROLE USERADMIN;

CREATE OR REPLACE ROLE HR_ROLE;
CREATE OR REPLACE ROLE AREA1_ROLE;
CREATE OR REPLACE ROLE AREA2_ROLE;

/* SYSADMIN 역할은 객체 생성하는 데 사용*/

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE DEMO07_DB;              -- DATABASE 생성
CREATE OR REPLACE SCHEMA TAG_LIBRARY;              -- SCHEMA 생성
CREATE OR REPLACE SCHEMA HRDATA;                   -- SCHEMA 생성
CREATE OR REPLACE SCHEMA CH7DATA;                  -- SCHEMA 생성
CREATE OR REPLACE TABLE DEMO07_DB.CH7DATA.RATING
(
     EMP_ID    INTEGER 
,    RATING    INTEGER
,    DEPT_ID   INTEGER
,    AREA      INTEGER
) ;


/* 데이터 삽입 */

INSERT INTO DEMO07_DB.CH7DATA.RATING VALUES
(1, 77, '100', 1),
(2, 88, '100', 1),
(3, 72, '101', 1),
(4, 94, '200', 2),
(5, 88, '300', 3),
(6, 91, '400', 3);

/* SECURITYADMIN 역할을 사용자에게 할당*/

USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE HR_ROLE ;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AREA1_ROLE ;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AREA2_ROLE ;
 

/* 필요한 객체에 사용 권한을 부여 */

GRANT USAGE ON DATABASE DEMO07_DB TO ROLE HR_ROLE;
GRANT USAGE ON DATABASE DEMO07_DB TO ROLE AREA1_ROLE;
GRANT USAGE ON DATABASE DEMO07_DB TO ROLE AREA2_ROLE;

GRANT USAGE ON SCHEMA DEMO07_DB.CH7DATA TO ROLE HR_ROLE;
GRANT USAGE ON SCHEMA DEMO07_DB.CH7DATA TO ROLE AREA1_ROLE;
GRANT USAGE ON SCHEMA DEMO07_DB.CH7DATA TO ROLE AREA2_ROLE;
GRANT USAGE ON SCHEMA DEMO07_DB.HRDATA  TO ROLE HR_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA DEMO07_DB.CH7DATA TO ROLE HR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO07_DB.CH7DATA TO ROLE AREA1_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO07_DB.CH7DATA TO ROLE AREA2_ROLE;


/* 새사용자에게 세 가지 역할을 할당 */
GRANT ROLE HR_ROLE TO USER ADAM;
GRANT ROLE AREA1_ROLE TO USER ADAM;
GRANT ROLE AREA2_ROLE TO USER ADAM;
-- GRANT ROLE AREA1_ROLE TO USER SYSADMIN;
-- GRANT ROLE AREA2_ROLE TO USER SYSADMIN;

/* HR_ROLE에 SELECT & INSERT 권한 부여 ACCOUNTADMIN 역할로 해야함*/
USE ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DEMO07_DB.HRDATA TO ROLE HR_ROLE;
GRANT INSERT ON FUTURE TABLES IN SCHEMA DEMO07_DB.HRDATA TO ROLE HR_ROLE;


/* ACCESS_HISTORY 뷰 ( 가장 쿼리를 많이한 사용자) */

USE ROLE ACCOUNTADMIN;

SELECT USER_NAME, COUNT(*) USES 
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
GROUP BY USER_NAME 
ORDER BY USES DESC;



/* 가장 자주 사용되는 테이블 */

SELECT OBJ.VALUE::objectName::STRING TABLENAME, COUNT(*) USES 
FROM   SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    TABLE(FLATTEN(BASE_OBJECTS_ACCESSED)) OBJ
GROUP BY TABLENAME 
ORDER BY USES DESC;

SELECT *
FROM   BASE_OBJECTS_ACCESSED;


/* TIME TRAVEL 명령어*/
-- 보존기간 설정 
USE ROLE SYSADMIN;
ALTER DATABASE DEMO07_DB 
SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- 확인 
USE ROLE SYSADMIN;
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, RETENTION_TIME
FROM   DEMO07_DB.INFORMATION_SCHEMA.TABLES;

--Time Travel test 
USE ROLE SYSADMIN; 
UPDATE DEMO07_DB.CH7DATA.RATING AREA SET AREA = 4;

SELECT *
FROM  DEMO07_DB.CH7DATA.RATING;

SELECT * FROM DEMO07_DB.CH7DATA.RATING AT (offset => -60*5);  -- 5분전 데이터 확인 / 초단위 

/*테이블 변경 or 생성*/
CREATE OR REPLACE TABLE DEMO07_DB.CH7DATA.RATING
AS
SELECT * FROM DEMO07_DB.CH7DATA.RATING AT (offset => -60*5);

/*쿼리 ID 이력을 사용하여 동일한 작업을 수행 가능*/

SELECT * FROM DEMO07_DB.CH7DATA.RATING before(statement => '<Query ID from Histroy>');

/* 테이블 삭제 및 복구 */
DROP TABLE DEMO07_DB.CH7DATA.RATING;
SELECT * FROM DEMO07_DB.CH7DATA.RATING;

UNDROP TABLE DEMO07_DB.CH7DATA.RATING;
SELECT * FROM DEMO07_DB.CH7DATA.RATING;


/* 객체 태킹 */
USE ROLE SYSADMIN;
CREATE OR REPLACE SCHEMA DEMO07_DB.TAG_LIBRARY;


-- 해당 태그에 사용할 수 있는 값을 나열하는 주석을 포함 
CREATE OR REPLACE TAG Classification; 
ALTER TAG Classification set comment =
  " Tag table or Views with one of the following classification values :
   'Confidential', 'Restricted', 'Internal', 'Public'";

-- 개인식별정보가 포함된 필드를 식별하는 열수준에서 연결된 태그 생성
CREATE OR REPLACE TAG PII;
ALTER TAG PII set comment = "Tag TAbles or Views with PII with one or more
                             of the following values: 'phone', 'Email', 'Address'";


USE ROLE SYSADMIN;
CREATE OR REPLACE TABLE DEMO07_DB.HRDATA.EMPLOYEES
(
     EMP_ID    INTEGER
,    RNAME     VARCHAR(50)
,    LNAME     VARCHAR(50)
,    SSN       VARCHAR(50)
,    EMAIL     VARCHAR(50)
,    DEPT_ID   INTEGER
,    DEPT      VARCHAR(50)
);

INSERT INTO DEMO07_DB.HRDATA.EMPLOYEES VALUES (0, 'First','Last','000-00-0000','eamil@email.com', 100, 'IT');

/*전체 테이블에 값이 기밀인 분류 태그를 할당*/
ALTER TABLE DEMO07_DB.HRDATA.EMPLOYEES
SET  TAG DEMO07_DB.TAG_LIBRARY.Classification="Confidential";

/* 개별 열에 두 개의 태그를 할당 */
ALTER TABLE DEMO07_DB.HRDATA.EMPLOYEES MODIFY EMAIL
 SET  TAG DEMO07_DB.TAG_LIBRARY.PII = "Email";

ALTER TABLE DEMO07_DB.HRDATA.EMPLOYEES MODIFY SSN
 SET  TAG DEMO07_DB.TAG_LIBRARY.PII = "SSN";

-- 태그 값 확인

SELECT SYSTEM$GET_TAG('Classification','DEMO07_DB.HRDATA.EMPLOYEES', 'table');


/*동적 데이터 마스킹 */

USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE masking policy DEMO07_DB.HRDATA.emailmask
AS
(val string) returns string ->
CASE WHEN current_role() in ('HR_ROLE') THEN val
     ELSE '**MASKED**'
END ;

ALTER TABLE DEMO07_DB.HRDATA.EMPLOYEES modify column EMAIL
set masking policy DEMO07_DB.HRDATA.emailmask;

CREATE OR REPLACE masking policy DEMO07_DB.HRDATA.SSNmask
AS
(val string) returns string ->
CASE WHEN current_role() in ('HR_ROLE') THEN val
     ELSE '**MASKED**'
END ;

ALTER TABLE DEMO07_DB.HRDATA.EMPLOYEES modify column SSN
set masking policy DEMO07_DB.HRDATA.SSNmask;


