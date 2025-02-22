FOR V IN
(SELECT DISTINCT A.JOB_NAME FROM
(
(SELECT JOB_NAME,LOG_DATE FROM SYS.ALL_SCHEDULER_JOB_RUN_DETAILS
WHERE OWNER = 'RISK_DWH' AND (STATUS='STOPPED' OR STATUS='FAILED') AND TRUNC(LOG_DATE,'DD')=TRUNC(CURRENT_DATE,'DD')) A
LEFT JOIN
(SELECT JOB_NAME,LOG_DATE FROM SYS.ALL_SCHEDULER_JOB_RUN_DETAILS
WHERE OWNER = 'RISK_DWH' AND (STATUS='SUCCEEDED') AND TRUNC(LOG_DATE,'DD')=TRUNC(CURRENT_DATE,'DD') ) B
ON (A.JOB_NAME=B.JOB_NAME AND B.LOG_DATE>A.LOG_DATE)
LEFT JOIN
(SELECT JOB_NAME FROM SYS.USER_SCHEDULER_RUNNING_JOBS) C
ON A.JOB_NAME=C.JOB_NAME
LEFT JOIN
(SELECT job_name,next_run_date FROM SYS.user_SCHEDULER_JOBS
WHERE ENABLED='TRUE' AND trunc(next_run_date,'DD')=TRUNC(CURRENT_DATE,'DD') AND NEXT_RUN_DATE>CURRENT_DATE
AND EXTRACT(MINUTE FROM (NEXT_RUN_DATE-CURRENT_DATE))>=10) D
ON A.JOB_NAME=D.JOB_NAME)
WHERE B.JOB_NAME IS NULL AND C.JOB_NAME IS NULL AND D.JOB_NAME IS NULL)
LOOP
DBMS_SCHEDULER.SET_ATTRIBUTE
( V.JOB_NAME,
ATTRIBUTE => 'start_date',
value => sysdate + INTERVAL '1' MINUTE);
end LOOP;
