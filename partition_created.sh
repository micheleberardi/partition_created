#!/bin/bash
#
# command line
# ./partition_created.sh username password basename tablename data_from data_to
#
MYSQL=`which mysql`
mysqltablename=$4

FROM=$5 # Starting date from command line
TO=$6 # Finish partition date from command line

yf=$(echo $FROM | cut -d\- -f1)
mf=$(echo $FROM | cut -d\- -f2 |sed 's/^0*//')
df=$(echo $FROM | cut -d\- -f3 |sed 's/^0*//')
yt=$(echo $TO | cut -d\- -f1)
mt=$(echo $TO | cut -d\- -f2 |sed 's/^0*//')
dt=$(echo $TO | cut -d\- -f3 |sed 's/^0*//')
[ "$yf" -gt "$yt" ] && echo "FROM date greater than to date" && exit
[ "$yf" -eq "$yt" ] && [ "$mf" -gt "$mt" ] && echo "FROM date greater than TO date" && exit
[ "$yf" -eq "$yt" ] && [ "$mf" -gt "$mt" ] && [ "$df" -gt "$dt" ] && echo "FROM date greater than TO date" && exit


while [[ "$yf" -ne "$yt" || "$mf" -ne "$mt"  ]] ; do
    mf=$(($mf +1))

    if [ "$mf" -eq 13 ] ; then
        mf=1
        yf=$(($yf +1))
    fi
    PARTITION=$(echo "P${yf}${mf}")
    [ ${#mf} -eq 1 ] && PARTITION=$(echo "P${yf}0${mf}")
    TIMESTAMP=$(date -d "${yf}-${mf}-1" +%s)
    DATE=$(date -d @$TIMESTAMP "+%Y-%m-%d")
    case $mysqltablename in
        log_history)
              QUERY="$QUERY PARTITION $PARTITION VALUES LESS THAN ($TIMESTAMP) ENGINE = InnoDB,"
          ;;
        opt_a_historical)
              QUERY="$QUERY PARTITION $PARTITION VALUES LESS THAN ('$DATE 00:00:00') ENGINE = InnoDB,"
          ;;
    esac

done

if [[ "$df" != "" && "$dt" != ""  ]]
 then
 t_stamp_f=$(date -d $FROM +%s)
 t_stamp_t=$(date -d $TO +%s)

 while [[ "$t_stamp_f" -ne "$t_stamp_t" ]] ; do
          t_stamp_f=$(( $t_stamp_f+86400 ))
          yf=$(date -d @$t_stamp_f "+%Y-%m-%d" | cut -d\- -f1)
          mf=$(date -d @$t_stamp_f "+%Y-%m-%d" | cut -d\- -f2)
          df=$(date -d @$t_stamp_f "+%Y-%m-%d" | cut -d\- -f3)

          PARTITION2=$(echo "P${yf}${mf}${df}")
          TIMESTAMP2=$(date -d @$t_stamp_f "+%Y-%m-%d")


          case $mysqltablename in
            ts_conversion)
              {
              QUERY="$QUERY PARTITION $PARTITION2 VALUES LESS THAN ('$TIMESTAMP2 00:00:00') ENGINE = InnoDB,"
              }
            ;;
            log_trace)
              QUERY="$QUERY PARTITION $PARTITION2 VALUES LESS THAN ('$TIMESTAMP2') ENGINE = InnoDB,"
            ;;
          esac
 done

fi

QUERY=$(echo $QUERY | sed 's/\,$//')

case $mysqltablename in
        log_history)
              QUERY="ALTER TABLE $mysqltablename PARTITION BY RANGE (UNIX_TIMESTAMP(timestamp_in))($QUERY);"
          ;;
        ts_conversion)
              QUERY="ALTER TABLE $mysqltablename PARTITION BY RANGE COLUMNS(insertdatetime)($QUERY);"
          ;;
        opt_a_historical)
              QUERY="ALTER TABLE $mysqltablename PARTITION BY RANGE COLUMNS(date)($QUERY);"
          ;;
        log_trace)
              QUERY="ALTER TABLE $mysqltablename PARTITION BY RANGE COLUMNS(date_insert)($QUERY);"
          ;;

esac

#echo $QUERY

# Update table mysql
#
#--------------------------------------
$MYSQL -u$1 -p$2 -D$3 -e "$QUERY"
#--------------------------------------


