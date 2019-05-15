copy into RAW.ICECAST_ROWS(DATA_ROW, FILE_NAME , FILE_DATE_TIME, FILE_ROW_NUMBER, LOAD_TIME  )
from (select
    s.$1,
    metadata$filename as FILE_NAME,
      TO_TIMESTAMP_NTZ (split(split(FILE_NAME,'/')[5], 'access.log.')[1]::string::string,'YYYYMMDD_HH24MISS.gz'),
      METADATA$FILE_ROW_NUMBER,
    CURRENT_TIMESTAMP()
from @RAW.ICECAST_S3 s)
FILES = (%(file_name)s)
on_error = 'CONTINUE'
FORCE=TRUE