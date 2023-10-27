
-- 3. Staging and Loading Data
/*
샘플 트랜잭션 정형 데이터를 Snowflake 에 로드할 준비부터 시작하겠습니다.

이 섹션은 다음과 같은 단계로 진행됩니다.
• 데이터베이스 및 테이블 생성
• 외부 스테이지(External Stage) 생성
• 데이터에 대한 파일 형식(File Format) 생성
Snowflake 데이터 로딩하는 방법은 아래 매뉴얼에서 확인하기 바랍니다.
Snowflake 에 데이터 로딩하기: https://docs.snowflake.com/ko/user-guide-dataload.html
*/


-- Snowflake 로 데이터 가져오기 
/*
사용할 데이터는 자전거 공유 서비스의 트랜잭션 데이터입니다. 특정 정거장에서
자전거를 빌려서 특정 장소에 반납하면 하나의 트랜잭션(한 번의 Trip)이
완결되고 하나의 레코드로 저장됩니다. 이 데이터는 미국 동부 지역의 Amazon
AWS S3 버킷에 미리 저장되어 있으며, 이동 시간, 위치, 사용자 유형, 성별, 나이
등에 관한 관련 정보로 구성됩니다. AWS S3 에서 이 데이터는 6,150 만 행,
377 개의 객체로 표현되고 1.9GB 로 압축되어 있습니다.
*/


-- 데이터베이스 및 테이블 생성 
/*
먼저, 정형 데이터를 로딩하는 데 사용할 CITIBIKE 라는 이름의 데이터베이스를
생성합니다.
Databases 탭으로 이동합니다. 만들기를 클릭하고, 데이터베이스
이름을 CITIBIKE 로 지정한 뒤, 마침을 클릭합니다.
*/

-- Warehouse 권한 설정 --
/*
Snowflake trial 계정 생성 시 AccountAdmin Role이 compute_wh 를 
소유함에 따라 sysadmin에서 사용할수 있도록 권한 부여
*/
use role accountadmin;
grant usage, operate, modify on warehouse compute_wh to role sysadmin;

-- SQL 로 데이터베이스 생성 및 컨텍스트 설정 
/*
앞에서 UI 로 진행했던 과정은 SQL 을 통해서도 진행할 수 있습니다.
*/
/* 먼저 어떤 Role 과 Warehouse 를 사용할 지 컨텍스트를 지정합니다. */
use role sysadmin;
use warehouse compute_wh;
/* citibike 데이터베이스를 생성합니다. */
create or replace database citibike;
/* 테이블을 생성할 데이터베이스와 스키마를 컨텍스트로 지정합니다. */
use database citibike;
use schema public;



/* 
다음으로 샘플 트랜잭션 정형 데이터를 로드하는 데 사용할 TRIPS 라는 테이블을
만듭니다. UI 를 사용하는 대신 워크시트를 사용하여 테이블을 생성하는 DDL 을
실행합니다. 다음 SQL 텍스트를 워크시트에 복사합니다.

데이터를 로딩할 테이블을 해당 칼럼으로 생성합니다. */
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);
/* 생성된 테이블과 설정된 parameter 를 확인합니다. */
show tables like 'tri%' in citibike.public;
/* alter table 명령으로 parameter 를 설정합니다. 예를 들어, Change_tracking 을
true 로 설정 하면 테이블에 일어난 모든 change 를 활용해서 CDC 작업을 수행할
수 있습니다. */
alter table trips set change_tracking = true;
/* 테이블의 각 컬럼 정보를 확인합니다. */
desc table trips;




-- 명령을 실행하는 다양한 옵션. 
/*
SQL 명령은 UI 를 통해서나, Worksheets 탭을 통해, SnowSQL 명령행 도구를
사용해서, ODBC/JDBC 를 통해 선택한 SQL 편집기를 이용해서 또는 Python 이나
Spark 커넥터를 통해 실행할 수 있습니다.
- SnowSQL(CLI 클라이언트) : https://docs.snowflake.com/ko/userguide/snowsql.html
- ODBC 드라이버: https://docs.snowflake.com/ko/user-guide/odbc.html
커서를 명령 내 어디든 두고 페이지 상단의 파란색 실행 버튼을 클릭하여 쿼리를
실행하십시오. 또는 바로 가기 키 [Ctrl]/[Cmd]+[Enter]를 이용하십시오.
TRIPS 테이블이 생성되었는지 확인합니다. 워크시트 하단에 "테이블 TRIPS 가
성공적으로 생성됨" 메시지를 표시하는 결과 섹션이 표시되어야 합니다.

워크시트의 왼쪽 상단에 있는 HOME 아이콘을 클릭하여 Databases 탭으로
이동합니다. 그런 다음 Data > Databases 를 클릭합니다. 데이터베이스
목록에서 CITIBIKE > PUBLIC > TABLES 를 클릭하여 새로 생성된 TRIPS 테이블을
확인합니다. 

방금 생성한 테이블 구조를 보려면 TRIPS 및 Columns 탭을 클릭합니다.
*/


-- Create an External Stage - 20P
/*
데이터베이스 테이블로 읽을 데이터는 외부 S3 버킷에 준비되어 있으므로, 이
데이터를 사용하기 전에 먼저 외부 버킷의 위치를 지정하는 단계를 생성해야
합니다.
- 아마존 S3 에서 대량 로드: https://docs.snowflake.com/ko/user-guide/data-loads3.html

Databases 탭에서 CITIBIKE 데이터베이스와 PUBLIC 스키마를
클릭합니다. Stage 탭에서 Create 버튼을 클릭한 다음 Stages > Amazon S3 를
클릭합니다.

열리는 "Create Securable Object" 대화 상자에서 SQL 문에서 다음 값을
바꿉니다.
stage_name: citibike_trips
url: s3://snowflake-workshop-lab/citibike-trips-csv/
참고: URL 끝에 마지막 슬래시(/)를 포함해야 합니다. 
*/


-- SQL 로 External Stage 생성하기 

/* 외부 스테이지를 생성합니다.
AWS S3 의 버킷을 그대로 데이터 파일을 저장하는 외부 스테이지로 활용할 수
있습니다. */
create or replace stage citibike_trips 
url='s3://snowflake-workshop-lab/citibike-trips-csv/';

list @citibike_trips;

/* 
citibike_trips 스테이지에 저장된 데이터 파일의 현황을 검토합니다.

마지막으로 실행시킨 쿼리의 결과를 통해서 전체 사이즈, 평균 파일 크기,
파일 갯수를 확인합니다. */
select
 floor(sum($2) / power(1024, 3), 1) total_compressed_storage_gb,
 floor(avg($2) / power(1024, 2), 1) avg_file_size_mb,
 count(*) as num_files
from
 table(result_scan(last_query_id()));


-- Create File Format 
/* 데이터 파일에 저장한 데이터의 구조를 반영하는 File Format 을 생성합니다. */
create or replace file format csv type='csv'
 compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
 skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
 error_on_column_count_mismatch = false escape = 'none'
escape_unenclosed_field = '\134'
date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file
format for ingesting data to snowflake';



/* 파일 포맷이 생성되었는 지 확인합니다. */
show file formats in database citibike;

/* 메타데이터를 통해서 각 파일의 경로 및 이름과 레코드 카운트에 대한 정보를
확인합니다. */
select metadata$filename, metadata$file_row_number from @citibike_trips
(file_format => csv);

-- Copy Into <Table>
/*
이 섹션에서는 데이터 웨어하우스와 COPY 명령을 사용하여 방금 생성한 Snowflake 테이블에 정형 데이터 대량 로드 (bulk loading)를 진행합니다.
*/


-- 데이터 로드를 위한 웨어하우스 크기 조정 
/*
데이터 로딩은 많은 컴퓨팅 리소스를 사용할 수도 있어서 데이터 로딩을 하기
전에 가상 웨어하우스의 성능을 높이도록 하겠습니다.

Snowflake 의 컴퓨팅 노드는 가상 웨어하우스 (Virtual Warehouse)라고 하며
워크로드가 데이터 로드, 쿼리 실행 또는 DML 작업을 수행하는지 여부에 따라
워크로드에 맞춰 동적으로 크기를 늘리거나 줄일 수 있습니다. 각 워크로드는
자체 데이터 웨어하우스를 보유할 수 있으므로 리소스 경합이 없습니다.

Warehouses 탭(Compute 아래)으로 이동합니다. 여기에서 기존 웨어하우스를
모두 보고 사용 추세를 분석할 수 있습니다.
상단 오른쪽 상단 모서리에 있는 + Warehouse 옵션을 확인하세요. 여기에서 새
웨어하우스를 빠르게 추가할 수 있습니다. 단, 30 일 체험판 환경에 포함된 기존
웨어하우스 COMPUTE_WH 를 사용합니다.
COMPUTE_WH 웨어하우스 행을 클릭합니다. 그런 다음 위의 오른쪽 상단
모서리에 있는 …(점 점)을 클릭하고 Edit 을 선택해서 Size 를 조정할 수
있습니다.

Size 드롭다운은 웨어하우스의 용량을 선택하는 곳입니다. 더 큰 데이터
로드 작업이나 컴퓨팅 집약적인 쿼리의 경우 더 큰 웨어하우스가
권장됩니다. 이 데이터 웨어하우스의 Size 를 X-Small 에서 Small 로
변경합니다. Save Warehouse 버튼을 클릭합니다.
*/


-- 데이터 로드
/*
이제 COPY 명령을 실행하여 데이터를 앞서 생성한 TRIPS 테이블로 로드할 수
있습니다.

Worksheet 탭에서 컨텍스트가 올바르게 설정되었는지 확인합니다.
Role: SYSADMIN Warehouse: COMPUTE_WH Database: CITIBIKE Schema = PUBLIC
*/
/* 
워크시트에서 다음의 문을 실행하여 구성한 데이터를 테이블로 로드하고 실행
시간을 체크합니다.
citibike_trips 외부 스테이지에 있는 데이터 파일을 csv 파일 포맷에 맞춰서
trips 테이블에 로딩합니다. */
copy into trips from @citibike_trips file_format=csv pattern= '.*csv.*' ;
/* 테이블에 로딩된 데이터를 확인합니다. */
select * from trips limit 20;
/*
결과 창에서 로드된 각 파일의 상태를 확인해야 합니다. 로드가 완료되면 오른쪽
하단의 Query Details 창에서 마지막으로 실행된 명령문에 대한 다양한 상태,
오류 통계 및 시각화를 스크롤할 수 있습니다.

그런 다음 Home 아이콘을 클릭한 다음 Compute > History 을
클릭하여 History 탭으로 이동합니다. 목록 맨 위에서 마지막으로 실행된 COPY
INTO 문을 선택합니다. 쿼리가 실행하기 위해 취한 단계, 쿼리 세부 정보, 가장
비싼 노드 및 추가 통계를 살펴 볼 수 있습니다.
*/


/*
이제 더 큰 웨어하우스로 TRIPS 테이블을 다시 로드하여 추가 컴퓨팅 리소스가
로드 시간에 미치는 영향을 살펴보겠습니다.
워크시트로 돌아가서 TRUNCATE TABLE 명령을 사용하여 모든 데이터와
메타데이터를 지웁니다.
*/
/* trips 테이블의 데이터와 메타데이터를 지웁니다. */
truncate table trips;

/*
결과에 "Query produced no results"(쿼리에서 결과가 생성되지 않음)이
표시되어야 합니다.
다음 ALTER WAREHOUSE 를 사용하여 웨어하우스 크기를 large 으로 변경합니다.
*/
/* 명령어를 통해서 직접 가상 웨어하우스의 크기를 large 로 변경합니다. */
alter warehouse compute_wh set warehouse_size='large';

/* 웨어하우스의 변경 사항을 확인 */
show warehouses;

/*
이제 성능의 향상된 가상 웨어하우스를 활용해서 다시 데이터를 로딩합니다
*/
/* 데이터를 로딩하기 전에 validation_mode 로 데이터 로딩시 발생할 에러를
미리 체크할 수 있습니다. */
copy into trips from @citibike_trips file_format=csv pattern= '.*csv.*'
validation_mode=return_all_errors;
/* citibike_trips 외부 스테이지에 있는 데이터 파일을 csv 파일 포맷에 맞춰서
trips 테이블에 로딩합니다. */
copy into trips from @citibike_trips file_format=csv pattern= '.*csv.*' ;

/* 테이블에 로딩된 데이터를 확인합니다. */
select * from trips sample (50 rows);
/*
로드가 완료되면 Query History 페이지로 다시 이동합니다.
(Home 아이콘 > Activity > Query History ). 두 COPY INTO 명령의 시간을 비교하십시오.
*/


-- Transforming Data During a Load
/*
Copy into <Table> 명령은 Copy Into <Table> From (SELECT 구문)를 통해서
데이터 파일에서 데이터를 로드하기 전에 변환 작업을 진행할 수 있습니다.
*/


/* 데이터 파일에서 일부 데이터를 선별적으로 테이블을 만들기 위해서 새로운
테이블을 생성합니다. 원본 데이터에 없는 tripid 라는 새로운 칼럼도 구성합니다.
*/
create or replace table trips_agg
(tripid number autoincrement,
tripduration integer,
start_station_name string,
end_station_name string,
bikeid integer);

/* 테이블의 각 컬럼 정보를 확인합니다. */
desc table trips_agg;
/* 데이터 파일에서 필요한 컬럼만 추출해서 테이블로 로딩하고 tripid 는
자동으로 생성되도록 합니다. */
copy into trips_agg(tripduration, start_station_name, end_station_name, bikeid)
   from (select t.$1, t.$5, t.$9, t.$12 from @citibike_trips t)
   file_format=csv pattern='.*csv.*';


select * from trips_agg limit 20;


/* 데이터 파일에서 일부 데이터를 전처리해서 로딩하기 위해서 새로운 테이블을
생성합니다. 원본 데이터에 없는 tripid 라는 새로운 칼럼도 구성하고 bikeid 는
스트링 타입으로 저장하고 membership 에는 NULL 값이 없도록 저장하고자
합니다. */
create or replace table trips_cust
(tripid number autoincrement,
tripduration integer,
start_station_name string,
end_station_name string,
bikeid string,
membership_type string);
desc table trips_cust;
/* 데이터 파일에서 필요한 컬럼만 추출하고 해당하는 function 을 통해서 데이터
로딩 전에 변환 작업을 수행합니다. */
copy into trips_cust (tripduration, start_station_name, end_station_name, bikeid,
membership_type)
 from (select t.$1, t.$5, t.$9, to_varchar(t.$12), ifnull(t.$13,'Free Membership')
from @citibike_trips t)
 file_format=csv pattern='.*csv.*';
 
select * from trips_cust limit 20;




-- Create External Table - 35P
/*
외부 테이블은 데이터 레이크를 Snowflake 와 통합하기 위한 효과적인
방법입니다. 일반적인 테이블에서 데이터는 데이터베이스에 저장되지만, 외부
테이블에서 데이터는 외부 스테이지(External Stage)의 파일에 저장됩니다. 외부
테이블은 파일 이름, 버전 식별자 및 관련 속성과 같은 데이터 파일에 대한 파일
수준 메타데이터를 저장합니다. 이를 통해 데이터베이스 내부에 있는 것처럼 외부
스테이지에서 파일에 저장된 데이터를 쿼리할 수 있습니다. 외부 테이블은 COPY
INTO <테이블> 문에서 지원하는 모든 형식으로 저장된 데이터에 액세스할 수
있습니다.

외부 테이블은 읽기 전용이므로 DML 작업을 수행할 수 없지만, 쿼리 및 조인
작업에는 외부 테이블을 사용할 수 있습니다. 외부 테이블에 대해 뷰를 생성할 수
있습니다.

데이터베이스 외부에 저장된 데이터를 쿼리하는 것은 기본 데이터베이스 테이블을
쿼리하는 것보다 느릴 수 있지만, 외부 테이블을 기반으로 하는 Materialized
View 는 쿼리 성능을 향상시킬 수 있습니다.

*/
/* 외부 스테이지에 있는 경로와 파일 이름을 확인합니다. */
select metadata$filename from @citibike_trips;
/* 앞에서 지정한 file_format 에 따라 데이터 파일의 컬럼을 해석해서 외부
테이블에 대한 메타데이터만 저장합니다. 기본적으로 데이터는 variant 타입의
value 컬럼에 저장이 되므로 여기서 컬럼을 순서대로 추출해서 형변환을 하는
형태로 외부 테이블의 컬럼을 정의합니다. */
create or replace external table trips_ext
(tripduration integer as (value:c1::integer),
starttime timestamp as (value:c2::timestamp),
stoptime timestamp as (value:c3::timestamp),
start_station_id integer as (value:c4::integer),
start_station_name string as (value:c5::string),
start_station_latitude float as (value:c6::float),
start_station_longitude float as (value:c7::float),
end_station_id integer as (value:c8::integer),
end_station_name string as (value:c9::string),
end_station_latitude float as (value:c10::float),
end_station_longitude float as (value:c11::float),
bikeid integer as (value:c12::integer),
membership_type string as (value:c13::string),
usertype string as (value:c14::string),
birth_year integer as (value:c15::integer),
gender integer as (value:c16::integer))
location=@citibike_trips/
auto_refresh=false
file_format=csv;
/* 수동으로 refresh 를 실행해서 최신 업데이트를 반영합니다. */
alter external table trips_ext refresh;


/* 외부 테이블에 쿼리를 바로 실행해서 결과를 확인합니다. 먼저 value 컬럼에
데이터가 어떻게 들어 있는 지 확인합니다. */
select value from trips_ext limit 10;
/* 일반 컬럼들을 확인합니다. */
select tripduration, start_station_name, end_station_name from trips_ext limit 10;
/* 성능을 향상시키기 위해서 쿼리 결과를 저장하는 Materialized View 를
생성합니다. */
create materialized view trips_mat as
select tripduration, start_station_name end_station_name, bikeid from trips_ext
where tripduration > 10;
/* trips_mat view 에 쿼리를 실행해서 결과를 확인합니다. */
select * from trips_mat limit 100;


select 1;


