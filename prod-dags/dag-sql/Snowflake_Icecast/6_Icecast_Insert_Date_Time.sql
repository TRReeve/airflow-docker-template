insert into CURATED.ICECAST_DATE_TIME (STREAM_ID, DATE_TIME, DATE, TIME,   ENTRY_FLG, EXIT_FLG ,SECS_LISTENED)
select
STREAM_ID,
minutes.minute,
minutes.DATE,
minutes.minute,
	(CASE WHEN DATE_TRUNC('MINUTE', REQUEST_START_TIME) = minutes.minute THEN 1 ELSE 0 END) AS ENTRY_FLG,
	(CASE WHEN DATE_TRUNC('MINUTE', REQUEST_END_TIME) = minutes.minute THEN 1 ELSE 0 END) AS EXIT_FLG,
	(CASE WHEN ENTRY_FLG = 1 THEN 60 - SECOND(REQUEST_START_TIME)
		WHEN EXIT_FLG =1 THEN SECOND(REQUEST_END_TIME) ELSE

			60
		END ) AS SECS_LISTENED

from CURATED.ICECAST
inner  join refdata.public.minutes
on minutes.minute >= date_trunc('minute',REQUEST_START_TIME)
and minutes.minute <= REQUEST_END_TIME
WHERE
FILE_NAME =  %(file_name)s
--pre-filter dates to limit set returned as agressively as possible - snowflake will cross join and then filter, so using the whole table is expensive
	AND minute >= (select min(date_trunc('minute',REQUEST_START_TIME)) from CURATED.ICECAST WHERE FILE_NAME =  %(file_name)s)
	and minute <= (select max(REQUEST_END_TIME) from CURATED.ICECAST WHERE FILE_NAME = FILE_NAME =  %(file_name)s);
