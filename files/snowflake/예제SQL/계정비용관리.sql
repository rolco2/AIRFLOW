/*준비작업*/

USE ROLE SYSADMIN; 
USE WAREHOUSE COMPUTE_WH;

-- 가상 웨어하우스 생성 --
CREATE OR REPLACE WAREHOUSE VW2_WH WITH WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE, INITIALLY_SUSPENDED=TRUE;

CREATE OR REPLACE WAREHOUSE VW3_WH WITH WAREHOUSE_SIZE = SMALL
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE, INITIALLY_SUSPENDED=TRUE;

CREATE OR REPLACE WAREHOUSE VW4_WH WITH WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE, INITIALLY_SUSPENDED=TRUE;

CREATE OR REPLACE WAREHOUSE VW5_WH WITH WAREHOUSE_SIZE = SMALL
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE, INITIALLY_SUSPENDED=TRUE;

CREATE OR REPLACE WAREHOUSE VW6_WH WITH WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE, INITIALLY_SUSPENDED=TRUE;


-- 가상 웨어하우스를 리소스 모니터에 할당
CREATE RESOURCE MONITOR ;
-- 기존 리소스 모니터를 수정
ALTER RESOURCE MONITOR ;
-- 기존 리소스 모니터 보기    
SHOW RESOURCE MONITOR ;
-- 기존 리소스 모니터 삭제     
DROP RESOURCE MONITOR ;
    

/* 비용관리 TEST */

USE ROLE ACCOUNTADMIN; 

CREATE OR REPLACE RESOURCE MONITOR MONITOR1_RM WITH CREDIT_QUOTA =5000
TRIGGERS  on 50  percent do notify 
          on 75  percent do notify
          on 100 percent do notify
          on 110 percent do notify
          on 125 percent do notify;

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET RESOURCE_MONITOR = MONITOR1_RM;


/* 가상 웨어하우스 모니터 생성 */
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR MONITOR5_RM WITH CREDIT_QUOTA =1500
TRIGGERS  on 50  percent do notify 
          on 75  percent do notify
          on 100 percent do notify
          on 110 percent do notify
          on 125 percent do notify;

USE ROLE ACCOUNTADMIN;
ALTER WAREHOUSE VW2_WH SET RESOURCE_MONITOR = MONITOR5_RM;

/* 리소스 모니터 확인 */

USE ROLE ACCOUNTADMIN;
SHOW RESOURCE MONITORS;


/* 테스트 생성 */

USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR2_RM WITH CREDIT_QUOTA =500
TRIGGERS  on 50  percent do notify 
          on 75  percent do notify
          on 100 percent do notify
          on 100 percent do suspend
          on 110 percent do notify
          on 110 percent do suspend_immediate;
         
ALTER WAREHOUSE VW3_WH SET RESOURCE_MONITOR = MONITOR2_RM;


USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITRO6_RM WITH CREDIT_QUOTA =500
TRIGGERS  on 50  percent do notify 
          on 75  percent do notify
          on 100 percent do notify
          on 100 percent do suspend
          on 110 percent do notify
          on 110 percent do suspend_immediate;

ALTER WAREHOUSE VW6_WH SET RESOURCE_MONITOR = MONITRO6_RM;




USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR04_RM WITH CREDIT_QUOTA =500
TRIGGERS  on 50  percent do notify 
          on 75  percent do notify
          on 100 percent do notify
          on 100 percent do suspend
          on 110 percent do notify
          on 110 percent do suspend_immediate;

ALTER WAREHOUSE VW6_WH SET RESOURCE_MONITOR = MONITOR04_RM;


/* 리소스 모니터 확인 */
USE ROLE ACCOUNTADMIN;
SHOW RESOURCE MONITORS;
 








































    
