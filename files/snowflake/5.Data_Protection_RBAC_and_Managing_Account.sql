-- 5. Data Protection, RBAC and Managing Account
-- 57P

use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;
/*
Time Travel
타임 트래블 기능으로 사전 구성 가능한 기간 내 어느 시점이든 데이터에 액세스할 수 있습니다. 기본 기간은 24시간이며 Snowflake 엔터프라이즈 에디션으로는 90일 까지 가능합니다.
*/
drop table json_weather_data;

/*
json_weather_data 테이블에서 SELECT 문을 실행합니다. 기본 테이블이
삭제되었기 때문에 결과 창에 오류가 나타나야 합니다.
*/
select * from json_weather_data limit 10;


/*
이제 이 테이블을 다음과 같이 복구합니다.
*/
undrop table json_weather_data;

select * from json_weather_data_view limit 10;


-- Table Rollback - 59P

use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

update trips set start_station_name = 'mistake';

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


/*
백업을 하지 않은 상황이라도 Snowflake 에서는 아래 처럼 마지막 UPDATE
명령의 쿼리 ID 를 찾아 $QUERY_ID 라는 변수에 저장하면 됩니다.
*/
set query_id =
(select query_id from table(information_schema.query_history_by_session
(result_limit=>5))
where query_text like 'update%' order by start_time limit 1);

/*
올바른 스테이션 이름을 가진 테이블을 다음과 같이 다시 만듭니다.
*/
create or replace table trips as
(select * from trips before (statement => $query_id));


/*
다음과 같이 SELECT 문을 다시 실행하여 스테이션 이름이 복구되었는지
확인합니다.
*/
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;



-- Role Based Access Control - 62P
/*워크시트 권한 문제가 있으면 UI에서 변경*/
use role accountadmin;

-- create user junior_dba_user PASSWORD='abc123'; 
/*
다음 명령을 사용하여 역할을 생성하고 할당합니다. GRANT ROLE 명령을
실행하기 전에 <user>를 사용자 이름으로 바꿉니다.
*/
create role junior_dba;
-- grant role junior_dba to user <user>;
grant role junior_dba to user admin;
/*
SYSADMIN 과 같은 역할로 이 작업을 수행하려고 하면, 권한이 부족하여 실패할
것입니다. 기본적으로 SYSADMIN 역할은 새로운 역할이나 사용자를 생성할 수
없습니다.
워크시트 컨텍스트를 다음과 같이 JUNIOR_DBA 역할로 변경합니다.
*/
/*워크시트 권한 문제가 있으면 UI에서 변경*/
use role junior_dba;



/*
새로 생성된 역할에는 웨어하우스에 대한 사용 권한이 없기 때문에 웨어하우스 가 선택되지 않습니다. 
ADMIN 역할로 다시 전환하여 수정하고 COMPUTE_WH 웨어 하우스에 사용 권한을 부여해 보겠습니다.
*/
/*워크시트 권한 문제가 있으면 UI에서 변경*/
use role accountadmin;
grant usage on warehouse compute_wh to role junior_dba;
/*
JUNIOR_DBA 역할로 다시 전환합니다. 이제 COMPUTE_WH를 사용할 수 있습니다.
*/
/*워크시트 권한 문제가 있으면 UI에서 변경*/
use role junior_dba;
use warehouse compute_wh;
/*
마지막으로 왼쪽의 데이터베이스 개체 브라우저 패널에서 
CITIBIKE 및 WEATHER 데이터베이스가 더 이상 나타나지 않는 것을 확인할 수 있습니다. 
이는 JUNIOR_DBA 역할에 액세스 권한이 없기 때문입니다.

ACCOUNTADMIN 역할로 다시 전환하고 CITIBIKE 및 WEATHER 데이터베이스를 보고 사용하는 데 필요한 
USAGE 권한을 JUNIOR_DBA에 부여합니다.
*/
/*워크시트 권한 문제가 있으면 UI에서 변경*/
use role accountadmin;
grant usage on database citibike to role junior_dba; 
grant usage on database weather to role junior_dba;
/*
JUNIOR_DBA 역할로 다음과 같이 전환합니다.
*/
/*워크시트 권한 문제가 있으면 UI에서 변경*/
use role junior_dba;
/*
이제 CITIBIKE 및 WEATHER 데이터베이스가 나타나는지 확인하십시오. 
나타나지 않는다면 새로 고침 아이콘을 클릭하여 시도하십시오.
*/

