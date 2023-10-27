/*
4. Database Objects and Working with Queries
38P ~
*/


-- 데이터 분석을 위한 가상 웨어하우스 생성 및 워크로드 격리 - P38
/*
데이터 로드를 위한 가상 웨어하우스를 통해서 데이터 로딩 작업은 진행하고
있기 때문에, 데이터 분석을 실행하기 위한 새로운 가상 웨어하우스를 생성해서
분석 작업의 워크로드를 격리합니다.

Compute > Warehouses 탭으로 이동하여 + Warehouse 를 클릭하고 새
웨어하우스의 이름을 ANALYTICS_WH 로 지정하고 크기를 Large 로 설정합니다
*/


-- SQL 로 분석용 가상 웨어하우스 생성 및 설정 - P39
/* 데이터 분석용으로 large 크기의 새로운 가상 웨어하우스를 생성합니다. 5 분
이상 쿼리 요청이 없으면 자동으로 suspend 되도록 설정을 하고 쿼리 요청이
들어 오면 깨어 나서 요청을 처리하는 형태로 비용을 최적화합니다. */
create or replace warehouse analytics_wh
warehouse_size='large'
auto_suspend=300
auto_resume=true;


-- 데이터 분석하기 - P39
/*실제로는 분석 사용자가 SYSADMIN 이 아닌 다른 역할을 수행할 수도 있습니다.
뒤 세션에서 RBAC(Role Based Access Control)에 대해서 더 알아 보도록
하겠습니다.
그리고 일반적으로 Tableau, Looker, PowerBI 등과 같은 비즈니스 인텔리전스
제품을 통해서 많은 분석이 이루어 집니다. 고급 분석을 위해 Datarobot, Dataiku,
AWS Sagemaker 와 같은 데이터 과학 도구나 기타 광범위한 파트너
에코시스템의 솔루션들이 Snowflake 를 네이티브 하게 지원합니다.
JDBC/ODBC, Spark 또는 Python 을 활용하는 모든 기술이 Snowflake 의 데이터에
대한 분석을 실행할 수 있습니다.

- 커넥터 & 드라이버: https://docs.snowflake.com/ko/user-guide/connsdrivers.html

작업 중인 워크시트로 이동하여 마지막 섹션에서 생성한 새 웨어하우스를
사용하도록 웨어하우스를 변경합니다. 워크시트 컨텍스트는 다음과 같아야
합니다.
Role: SYSADMIN Warehouse: ANALYTICS_WH (L) Database: CITIBIKE Schema
= PUBLIC

*/


/* 
아래의 쿼리를 실행하여 trips 데이터 샘플을 확인합니다.

데이터 분석용 컨텍스트를 확인합니다. 웨어하우스는 분석용 웨어하우스를
사용하도록 하겠습니다. */
use role sysadmin;
use warehouse analytics_wh;
use database citibike;
use schema public;
select * from trips limit 20;


/* 
시간대 별로 이용 현황, 평균 이동 시간 및 평균 이동 거리 등 기본적인 집계는
기존 SQL 구문의 group by 를 활용해서 진행할 수 있으며, 다른 분석 쿼리들도
익숙한 SQL 구문에 따라서 다양하게 테스트해 볼 수 있습니다.

자전거를 대여한 시간에서 시간대를 추출해서 각 시간대 별로 이용 현황을
집계합니다. 데이터 분석용 컨텍스트를 확인합니다.
아래는 하버사인 삼각함수를 통해서 위도 경도 데이터로 대략적인 이동 거리를
계산했지만 그 외에 다양한 지리 정보 함수를 함께 제공합니다. */
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude,
end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;
/* 자전거 트립이 많고 이동 거리가 길었던 시간대를 집계합니다. 여기서는 지리
공간 함수를 사용해서 평균 이동 거리를 계산했습니다. */
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(st_distance(st_makepoint(start_station_latitude, start_station_longitude),
st_makepoint(end_station_latitude, end_station_longitude)) / 1000) as "avg distance (km)"
from trips
group by 1
having "num trips" > 100 and "avg distance (km)" > 1.5
order by 1;


/* 2013 년 이래로 가장 인기 있는 루트를 집계합니다. */
select
start_station_name,
end_station_name,
count(*) as "num trips"
from trips
where starttime > '2013'
group by 1, 2 order by 3 desc;





-- 사용자 정의 함수(UDF)를 분석에 활용하는 방법 
/*
Snowflake 는 디폴트로 제공하는 광범위한 함수들 외에도 사용자가 원하는 대로
함수를 정의하여 활용할 수 있는 방법을 제공합니다. SQL 이외에도 Java, Python
등 다양한 프로그래밍 언어를 지원하기 때문에 복잡한 비즈니스 로직을 사용자
정의 함수로 구성해서 활용할 수 있습니다.

Snowflake 의 사용자 정의 함수는 단일 값을 리턴 하는 경우 외에도 테이블
형식으로 여러 행을 결과값으로 리턴 할 수도 있습니다.
*/

/* 해당 년도와 쿼터를 입력 받아서 그 기간에 가장 높은 이동 시간을 결과로
제공하는 사용자 정의 함수를 구성합니다. */
create or replace function fn_max_duration(per_quarter integer, per_year integer)
returns integer
as
$$
select
max(tripduration)
from trips
where date_part(year, starttime) = per_year
group by date_part(quarter, starttime)
having date_part(quarter, starttime) = per_quarter
$$;
/* 생성된 사용자 정의 함수도 데이터베이스 오브젝트 이므로 UI 의 데이터베이스
탶에서 생성된 것을 확인 합니다.
사용자 정의 함수를 SQL 문에서 사용하는 방법은 다른 함수와 동일합니다. */
select fn_max_duration(3, 2013), fn_max_duration(2, 2014), fn_max_duration(3,
2017);
/* Secure 사용자 정의 함수를 생성하면 테이블 데이터는 물론 비즈니스 로직도
Secure Share 를 통해서 공유할 수 있습니다. */
create or replace secure function fn_sec_max_duration(per_quarter integer,
per_year integer)
returns integer
as
$$
select
max(tripduration)
from trips
where date_part(year, starttime) = per_year
group by date_part(quarter, starttime)
having date_part(quarter, starttime) = per_quarter
$$;
select fn_sec_max_duration(3, 2013), fn_max_duration(2, 2014), fn_max_duration(3,
2017);


-- Result Cache - P43
/*
Snowflake 에는 지난 24 시간 동안 실행된 모든 쿼리의 결과를 보유하고 있는
결과 캐시가 있습니다. 이는 웨어하우스 전반에 걸쳐 사용할 수 있으므로 기본
데이터가 변경되지 않았다면, 한 사용자에게 반환된 쿼리 결과를 동일한 쿼리를
실행하는 해당 시스템의 다른 사용자가 사용할 수 있습니다. 이러한 반복 쿼리는
매우 빠르게 반환될 뿐만 아니라 컴퓨팅 크레딧도 전혀 사용하지 않습니다.
*/

/* 처음 실행되는 쿼리는 시간과 동일한 쿼리를 결과 캐쉬에서 제공하는 시간을
비교해서 확인합니다. */
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude,
end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;
/*
결과가 캐시되었기 때문에 두 번째 쿼리가 훨씬 더 빠르게 실행되었음을
오른쪽 Query Details 창에서 확인합니다.
*/

/*
Zero-Copy Clone - 44P
Snowflake 를 사용하면 ‘제로 카피 클론’이라고도 하는 테이블, 스키마 및
데이터베이스의 클론을 몇 초 안에 생성할 수 있습니다. 클론을 생성할 때 원본
객체에 있는 데이터의 스냅샷을 찍으며 복제된 객체에서 이를 사용할 수
있습니다. 복제된 객체는 쓰기 가능하고 클론 원본과는 독립적입니다. 따라서
원본 객체 또는 클론 객체 중 하나에 이뤄진 변경은 다른 객체에는 포함되지
않습니다.

‘제로 카피 클론’ 생성의 일반적인 사용 사례는 개발 및 테스팅이 사용하는 운영
환경을 복제하여 운영 환경에 부정적인 영향을 미치지 않게 두 개의 별도 환경을
설정하여 관리할 필요가 없도록 테스트하고 실험하는 것입니다.

워크시트에서 다음 명령을 실행하여 trips 테이블의 개발(dev) 테이블 복제본을
만듭니다.
*/

/* 운영 환경의 trips 테이블을 클로닝해서 데이터의 복사 없이 새로운 개발
테이블을 생성합니다. */
create table trips_dev clone trips;
/*
CITIBIKE 데이터베이스 아래의 개체 트리를 확장하고 trips_dev 라는 새 테이블이
표시되는지 확인합니다. 이제 개발 팀은 trips 테이블이나 다른 개체에 영향을
주지 않고 이 테이블을 사용하여 업데이트 또는 삭제를 포함하여 원하는 모든
작업을 수행할 수 있습니다.
*/




-- Semi-structured Data and Data Pipelines - part2 시작 지점
-- Semi-Structured Data and Working with Queries - P46
/*
날씨가 자전거 이용 횟수에 어떻게 영향을 미치는지 확인하고자 하기 위해서
JSON 형식으로 저장된 반정형 데이터를 로딩 합니다.

• 공개된 S3 버킷에 보관된 JSON 형식의 날씨 데이터 로드
• 뷰 생성 및 SQL 점 표기법 (dot notation)을 사용해 반정형 데이터를 쿼리
• JSON 데이터를 이전에 로드된 TRIPS 데이터에 조인하는 쿼리를 실행
• 날씨 및 자전거 이용 횟수 데이터를 분석하여 관계 파악

JSON 데이터는 57,900 행, 61 개 객체 및 2.5MB 압축으로 이루어 진 압축된
JSON 문서로 AWS S3 에 저장되어 있습니다.
*/


-- 데이터베이스 및 테이블 생성 - 47P
/*
먼저, 워크시트를 통해, 반정형 데이터를 저장하는 데 사용할 WEATHER 라는
이름의 데이터베이스를 생성합니다.
*/
/* 새 데이터베이스 생성하기 */
create or replace database weather;


/* 테이블을 생성하기 위한 컨텍스트를 설정합니다. 새로 생성한 weather
데이터베이스의 public 스키마에서 작업을 합니다. */
use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

/* JSON 데이터를 로딩하려고 하기 때문에 반정형 데이터 타입을 네이티브하게
지원하는 variant 타입의 컬럼을 가진 테이블을 생성합니다. */
create or replace table json_weather_data (v variant);

/*
워크시트 하단의 결과 창에서 JSON_WEATHER_DATA 테이블이 생성되었는지
확인합니다.
*/


-- External Stage 생성 - P49
/*
워크시트에서 다음 명령을 사용하여 AWS S3 에서 반정형 JSON 데이터가
저장되는 버킷을 가리키는 외부 스테이지를 설정합니다.
*/

/* public s3 의 버킷을 가리키는 외부 스테이지를 생성합니다. */
create or replace stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';

/*
LIST 명령을 통해서 파일 목록을 살펴보도록 하겠습니다.
압축된 JSON 파일들이 저장되어 있는 것을 확인할 수 있습니다.
*/
list @nyc_weather;
/* 이 결과에서 바로 전체 사이즈와 평균 파일 사이즈를 알아 보도록 하겠습니다.
*/
select
 floor(sum($2) / power(1024, 2), 1) total_compressed_storage_mb,
 floor(avg($2) / power(1024, 1), 1) avg_file_size_kb,
 count(*) as num_files
from
 table(result_scan(last_query_id()));


/*
테이블로 로딩하기 전에 실제로 데이터 파일로 저장되어 있는 데이터의 내용도
바로 살펴 보도록 하겠습니다.
*/
/* 외부 스테이지에 있는 데이터 파일에 대해서 바로 쿼리를 실행해서 내용을
살펴 볼 수 있습니다. JSON 타입으로 해석하는 File Format 을 먼저 생성한
다음에 select 문에서 설정을 해 줍니다. */
create or replace file format my_json_format type = 'json';

select $1
From @nyc_weather/ (file_format => my_json_format)
Limit 10;
/* 메타 데이터도 함께 결과를 확인해 보겠습니다. Parse_json 은 문자열을 Variant
형식으로 변환하는 유용한 함수입니다. */
select
 metadata$filename, metadata$file_row_number, parse_json($1)
from
 @nyc_weather/ (file_format => my_json_format);

/*
 섹션에서는 웨어하우스를 사용하여 S3 버킷의 데이터를 이전에
생성한 JSON_WEATHER_DATA 테이블로 로드합니다.
워크시트에서 아래 COPY 명령을 실행하여 데이터를 로드합니다.
SQL 명령에서도 FILE FORMAT 객체를 인라인으로 지정할 수 있는 방법을 사용할
수도 있습니다.

정형 데이터를 로드했던 이전 섹션에서 파일 형식을 자세하게 정의해야
했습니다. 여기에 있는 JSON 데이터는 형식이 잘 지정되어 있기 때문에 기본
설정을 사용해 간단하게 JSON 유형을 로딩하고 결과를 살펴 보겠습니다.
*/
/* 먼저 Variant 타입의 컬럼에 JSON 데이터를 로딩하겠습니다. */
copy into json_weather_data
from @nyc_weather
file_format = (type=json);

select * from json_weather_data limit 20;
/*
결과에서 한 행을 클릭하여 오른쪽 패널에 형식 JSON 을 표시해서 어떤 형태의
데이터 인지 살펴 보겠습니다.
*/


-- View 및 Materialized View - P52
/*
원본 테이블은 JSON 형식의 컬럼이기 때문에 뷰를 사용하면 쿼리 결과에
테이블처럼 액세스할 수 있습니다. Snowflake 는 여러 가지 뷰의 형태를 제공하는
데 그 중에 Materialized View 는 쿼리의 결과를 저장하기 때문에 저장 용량을
필요로 하지만 성능을 향상 시킬 수 있습니다. Snowflake 엔터프라이즈 이상에서
Materialized View 를 사용할 수 있습니다.
*/
/* 원본 테이블이 Variant 타입의 JSON 구조체이므로 분석가들에게 익숙한
테이블 형태의 뷰를 생성해서 분석을 용이하게 합니다.
JSON 구조체에서 계층에 따라서 .(닷)으로 원하는 컬럼을 구성하고 ::으로
형변환을 해 주는 형식으로 구성합니다. */
create view json_weather_data_view as
select
v:time::timestamp as observation_time,
v:city.id::int as city_id,
v:city.name::string as city_name,
v:city.country::string as country,
v:city.coord.lat::float as city_lat,
v:city.coord.lon::float as city_lon,
v:clouds.all::int as clouds,
(v:main.temp::float)-273.15 as temp_avg,
(v:main.temp_min::float)-273.15 as temp_min,
(v:main.temp_max::float)-273.15 as temp_max,
v:weather[0].main::string as weather,
v:weather[0].description::string as weather_desc,
v:weather[0].icon::string as weather_icon,
v:wind.deg::float as wind_dir,
v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

/* 뷰의 내용을 다른 테이블 형식의 데이터처럼 확인을 합니다. */
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01' limit 20;


-- JOIN 연산 - P55
/* 
trips 테이블에 weather view 를 left outer join 으로 시간대 별로 조인해서 날씨
조건 별로 전체 트립의 개수를 집계하면 각 날씨에 따른 이용 횟수의 관계를
알아 볼 수 있습니다. 
*/
select weather as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;
/* 
이 연산의 결과를 Secure view 로 생성해서 안전하게 공유하는 데 활용할 수도
있습니다. Secure view 는 어떤 SQL 구문을 통해서 이 결과가 나왔는 지에 대한
DDL 정보를 권한이 있는 사용자(예, 소유자)만 열람할 수 있습니다. 
*/
create or replace secure view weather_trips_sv as
select weather as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;



select * from weather_trips_sv;

show views;

