insert into   CURATED.ICECAST_UNFILTERED
 (
    STREAM_ID ,
       REQUEST_START_TIME ,
       REQUEST_END_TIME ,
       IPADDRESS ,
       "METHOD" ,
       URL ,
       STREAM_NAME ,
       LATITUDE ,
       LONGITUDE ,
       KRUX_ID ,
       PLAYER_ID ,
       LISTENER_ID ,
       NIELSEN_ID ,
       GDPR_CONSENT ,
       DO_NOT_TRACK ,
       NIELSEN_URL_SEGMENTS ,
       KRUX_URL_SEGMENTS ,
       PROTOCOL ,
       STATUS_CODE ,
       REFERRER ,
       USERAGENT ,
       DURATION ,
       FILE_NAME ,
       FILE_ROW_NUMBER,
       LOAD_TIME ,
       SERVER_NAME ,
       SERVER_LOG_TIME)

with processed_icecast as (
    select
      STREAM_ID
     ,IPADDRESS
     ,IDENTITY
     ,USERNAME
     ,TO_TIMESTAMP(REQUEST_END_TIME,'DD/MON/YYYY:HH24:MI:SS TZHTZM') as REQUEST_END_TIME_DT
     ,case when length(duration) < 15
        then dateadd(second,-1*duration, REQUEST_END_TIME_DT)
        else null
      end as REQUEST_START_TIME
     ,"METHOD"
  ,REQUEST
     ,split(REQUEST, ' ')[0] AS URL
     ,split(REQUEST, ' ')[1] AS PROTOCOL
     ,STATUS_CODE
     ,BYTES
     ,REFERRER
     ,USERAGENT
     ,case when length(duration) < 15 then DURATION else null end as  DURATION
     ,FILE_NAME
     ,FILE_ROW_NUMBER
     ,LOAD_TIME
	 ,PARSE_URL('HTTP://www.abc.com' || URL,1):parameters as url_params
     ,url_params:"aw_0_1st.playerid"::string as PlayerId
     ,regexp_substr(url_params:"amsparams", 'playerid:([^;]+);',1, 1, 'e') AS PlayerId2
     ,url_params:"nmcuid"::string as NielsenId
     ,PARSE_URL('HTTP://www.abc.com' || REPLACE(URL,'&amp;','&'),1):parameters."kuid"::string as kuid
     ,PARSE_URL('HTTP://www.abc.com' || URL,1):path::string as StreamName
     ,url_params:"listenerid"::string as listenerid
     ,url_params:"aw_0_awz.listenerid"::string as listenerid2

     ,url_params:"aw_0_req.gdpr"::string as Gdpr_consent
     ,url_params:"aw_0_req.lmt"::string as Do_Not_track
     ,url_params:"aw_0_1st.nmc"::string as Nielsen_Url_Segments
     ,url_params:"kxsegs"::string as Krux_Url_segments

     ,url_params:"aw_0_1st.gpslat"::string as lat1
     ,url_params:"lat"::string as lat2
     ,url_params:"aw_0_1st.gpslong"::string as lon1
     ,url_params:"lon"::string as lon2
    ,FILE_DATE_TIME
    from RAW.ICECAST
	where FILE_NAME =  %(file_name)s
)

 SELECT
  STREAM_ID,
    REQUEST_START_TIME,
    REQUEST_END_TIME_DT,
    IPADDRESS,
    "METHOD",
    URL,
    StreamName,
    TRY_CAST(coalesce(lat1, lat2) as number(15,10)) as LATITUDE,
    TRY_CAST(coalesce(lon1, lon2) as number(15,10)) as LONGITUDE,
    case when kuid is not null then kuid when PlayerId is not null and PlayerId!= 'UKRP' then coalesce(listenerid,listenerid2) end as KRUX_ID,
    coalesce(PlayerId,PlayerId2) As PlayerId,
    coalesce(listenerid,listenerid2) as LISTENER_ID,
    NielsenId,
       GDPR_CONSENT ,
       DO_NOT_TRACK ,
       NIELSEN_URL_SEGMENTS ,
       KRUX_URL_SEGMENTS ,
    PROTOCOL,
    STATUS_CODE,
    REFERRER,
    USERAGENT,
    DURATION,
    FILE_NAME,
    FILE_ROW_NUMBER,
    LOAD_TIME,
    split(split(FILE_NAME,'/')[5], '.access.log')[0]::string as SERVER_NAME,
    FILE_DATE_TIME as SERVER_LOG_TIME
from processed_icecast