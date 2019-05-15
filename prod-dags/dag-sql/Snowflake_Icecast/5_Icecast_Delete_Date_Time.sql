delete from CURATED.ICECAST_DATE_TIME
where stream_id in (select stream_id from CURATED.ICECAST_unfiltered where FILE_NAME = %(file_name)s )
