insert into PROCESSED.ICECAST_MINUTE_SUMMARY (STREAM_NAME,	"DATE",	"TIME",	DATE_TIME,	MINUTE_SESSIONS,	ENTRIES,	EXITS,	TOTAL_SECONDS,	QUARTER_HOUR_SESSIONS,	HOUR_SESSIONS,	DAY_SESSIONS,	LAST_REFRESHED)
						with stationMinutes as
						(
									select
						STREAM_NAME
						,date
						,time
						,TIMESTAMP_NTZ_FROM_PARTS(date, time) as DateTime
						from (select distinct STREAM_NAME from DS_ICECAST.CURATED.ICECAST where ICECAST.duration >= 60 and ICECAST.Request_end_time >= %(from_date_string)s   ) x
						CROSS join (SELECT distinct date, time from DS_ICECAST.CURATED.ICECAST_DATE_TIME where  date >= %(from_date_string)s and date < %(to_date_string)s )
						where date >= %(from_date_string)s and date < %(to_date_string)s
						)
						, sourceData as
						(
						  select
						    Date
						    ,time
						    ,STREAM_NAME
						    ,ICECAST_DATE_TIME.STREAM_ID
						    ,ENTRY_FLG
						    ,EXIT_FLG
						    ,SECS_LISTENED
						  from DS_ICECAST.CURATED.ICECAST_DATE_TIME
						    inner join DS_ICECAST.CURATED.ICECAST on ICECAST_DATE_TIME.STREAM_ID = ICECAST.stream_id
						        where ICECAST.duration >= 60
						    and  date >= %(from_date_string)s and date < %(to_date_string)s
						  and ICECAST.Request_end_time >= %(from_date_string)s
						)
						, daySummary as
						(


						select
						Date
						,STREAM_NAME
						,count(distinct sourceData.STREAM_ID) as sessions
						from sourceData
						group by
						Date
						,STREAM_NAME

						)
						, hourSummary as
						(

						select
						Date
						,STREAM_NAME
						,HOUR
						,count(distinct sourceData.STREAM_ID) as sessions
						from sourceData
						inner join refdata.public.time on sourceData.time = time.time
						group by
						Date
						,STREAM_NAME
						,HOUR
						)

						, quarterhourSummary as
						(

						select
						Date
						,STREAM_NAME
						,QUARTERHOUR
						,count(distinct sourceData.STREAM_ID) as sessions
						from sourceData
						inner join refdata.public.time on sourceData.time = time.time
						group by
						Date
						,STREAM_NAME
						,QUARTERHOUR
						)

						, minutesummmary as (

						select
						Date
						,STREAM_NAME
						,Time
						,TIMESTAMP_NTZ_FROM_PARTS(date,time) as DATETIME
						,count(sourceData.STREAM_ID) as sessions
						,sum(ENTRY_FLG) as entries
						,sum(EXIT_FLG) as exits
						,sum(SECS_LISTENED) as TotalSeconds
						from sourceData
						group by
						Date
						,STREAM_NAME
						,Time
						,TIMESTAMP_NTZ_FROM_PARTS(date,time)
						)

						select
						stationMinutes.STREAM_NAME
						,stationMinutes.date
						,stationMinutes.time
						,stationMinutes.DateTime
						,minutesummmary.sessions as MinuteSessions
						,minutesummmary.entries
						,minutesummmary.exits
						,minutesummmary.TotalSeconds
						,quarterhourSummary.sessions as QuarterHourSessions
						,hourSummary.sessions as HourSessions
						,daySummary.sessions as DaySessions
						,CURRENT_TIMESTAMP()  as updated_Time
						from stationMinutes
						left join minutesummmary on stationMinutes.STREAM_NAME = minutesummmary.STREAM_NAME
										and stationMinutes.DateTime = minutesummmary.DateTime

						left join quarterhourSummary on stationMinutes.STREAM_NAME = quarterhourSummary.STREAM_NAME
										and stationMinutes.Time = quarterhourSummary.QUARTERHOUR
										and stationMinutes.date = quarterhourSummary.date

						left join hourSummary on stationMinutes.STREAM_NAME = hourSummary.STREAM_NAME
										and stationMinutes.Time = hourSummary.HOUR
										and stationMinutes.date = hourSummary.date

						left join daySummary on stationMinutes.STREAM_NAME = daySummary.STREAM_NAME
										and stationMinutes.Time = '00:00:00'
										and stationMinutes.date = daySummary.date
						order by stationMinutes.STREAM_NAME