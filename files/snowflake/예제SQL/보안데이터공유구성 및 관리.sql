/* 데이터베이스 생성 */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE DEMO10_DB;
USE SCHEMA DEMO10_DB.PUBLIC;

CREATE OR REPLACE TABLE SHARINGDATA(I INTEGER);


/* 아웃 바운드 공유 만들기*/

-- 데이터 컨슈머가 사용할 공유에 대한 엑세스 권한을 할당
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE SHARE DEMO10_SHARE2;
GRANT USAGE ON DATABASE DEMO10_DB TO SHARE DEMO10_SHARE2;
GRANT USAGE ON SCHEMA DEMO10_DB.PUBLIC TO SHARE DEMO10_SHARE2;

GRANT SELECT ON TABLE DEMO10_DB.PUBLIC.SHARINGDATA TO SHARE DEMO10_SHARE2;


/* 계정이 있는 컨슈머 추가 */
ALTER SHARE <name_of_share> ADD ACCOUNTS = <name_of_consumer_account>;


/* 여러 국가에 있는 데이터 엑세스 */

USE ROLE ACCOUNTADMIN; 
USE DATABASE DEMO10_DB;
CREATE OR REPLACE SCHEMA PRIVATE;

CREATE OR REPLACE TABLE DEMO10_DB.PRIVATE.SENSITIVE_DATA
(
     NATION     STRING
,    PRICE      FLOAT
,    SIZE       INT
,    ID         STRING
);


INSERT INTO DEMO10_DB.PRIVATE.SENSITIVE_DATA VALUES
('USA', 123.5, 10, 'REGION1'),
('USA', 89.2,  14, 'REGION1'),
('USA', 99.0,  35, 'REGION2'),
('USA', 58.0,  22, 'REGION2'),
('USA', 112.6, 18, 'REGION2'),
('USA', 144.2, 15, 'REGION2'),
('USA', 96.8,  22, 'REGION3'),
('USA', 107.4, 19, 'REGION4') ;

-- 개별 계정에 대한 매핑을 보유할 테이블을 생성
CREATE OR REPLACE TABLE DEMO10_DB.PRIVATE.SHARING_ACCESS
(
     ID                    STRING
,    SNOW_FLAKE_ACCOUNT    STRING
);

INSERT INTO SHARING_ACCESS VALUES('REGION1', current_account());

-- RESION2 및 RESION3에 해당 계정과 연결된 값을 할당 

INSERT INTO SHARING_ACCESS VALUES('REGION2', 'ACCT2');
INSERT INTO SHARING_ACCESS VALUES('REGION3', 'ACCT3');



SELECT * FROM SHARING_ACCESS;


/* SENSITIV_DATA 기본 테이블의 모든 데이터를 SHARING_ACCESS 매핑 테이블과 
   결합할 보안 뷰 생성
*/

CREATE OR REPLACE SECURE VIEW DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA 
AS
SELECT  NATION
     ,  PRICE
     ,  SIZE
FROM          DEMO10_DB.PRIVATE.SENSITIVE_DATA AD
INNER JOIN    DEMO10_DB.PRIVATE.SHARING_ACCESS SA  ON     AD.ID = SA.ID
                                                   AND    SA.SNOW_FLAKE_ACCOUNT = current_account()
;                                                



SELECT *  fROM DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA ;

/* PUBLIC 역할에 권한 부여 / 수행할 수 있는 모든 역할의 보안 뷰에 SLEECT 권한 부여*/
GRANT SELECT ON DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA TO PUBLIC;

SELECT * FROM DEMO10_DB.PRIVATE.SENSITIVE_DATA;



/* 세션 변수를 사용하여 보안 뷰에서 무엇을 볼 수 있는지 확인  ACCT2로 변경 */
ALTER SESSION SET SIMULATED_dATA_SHARING_CONSUMER ='ACCT2';
SELECT * FROM DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA;

/* 세션 변수를 사용하여 보안 뷰에서 무엇을 볼 수 있는지 확인  ACCT3ACCOUNT로 변경 */
ALTER SESSION SET SIMULATED_dATA_SHARING_CONSUMER ='ACCT3';
SELECT * FROM DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA;


/* 원래 계정으로 되돌리기 */
ALTER SESSION UNSET simulated_data_sharing_consumer;


/* 세 공유 만들기 */
USE DATABASE DEMO10_DB;
USE SCHEMA DEMO10_DB.PUBLIC;
CREATE OR REPLACE SHARE NATIONS_SHARED; 

SHOW SHARES;

/* 새공유에 권한을 부여 */
GRANT USAGE ON DATABASE DEMO10_DB TO SHARE NATIONS_SHARED;
GRANT USAGE ON SCHEMA DEMO10_DB.PUBLIC TO SHARE NATIONS_SHARED;
GRANT SELECT ON DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA TO SHARE NATIONS_SHARED;














