USE ROLE SYSADMIN; 
USE WAREHOUSE COMPUTE_WH;

--데이터 베이스 생성 
CREATE OR REPLACE DATABASE DEMO_02;


-- 파일 형식 오브젝트 만들기 
CREATE OR REPLACE FILE FORMAT mycsvformat_local
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  SKIP_HEADER = 1;

  
-- CSV 데이터 파일을 위한 스테이지 만들기 
CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat_local;


--UI 웹에선 지원을 안해서 SnowSQL을 설치 후 cmd에서 해야한다. 

cmd> snowsql -a <그룹-계정> -u <사용자>
ex)  snowsql -a siorynu-iv51385 -u parkhj

-- CSV 샘플 데이터 파일 스테이징 하기 
PUT file://C:\temp\load\contacts*.csv /@my_csv_stage AUTO_COMPRESS=TRUE; 



-- CSV포멧형으로 스테이지 만들기
CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat
  URL = 's3://snowflake-docs';
  
  
  -- 테이블로 데이터 이동

COPY INTO mycsvtable
  FROM @my_csv_stage/tutorials/dataloading/contacts1.csv
  ON_ERROR = 'skip_file';

LIST @~;