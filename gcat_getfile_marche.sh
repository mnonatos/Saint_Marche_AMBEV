#!/bin/bash

DATE_WITH_TIME=`date "+%Y%m%d-%H%M%S"`
echo `date "+%Y%m%d-%H%M%S"`" - Processing Saint Marche"

BLOB_GCAT_PATH="wasb://gcat@ambevprojselloutcube.blob.core.windows.net/SAINT_MARCHE"

T_LASTMONTH=$(/usr/bin/hive -S -e "select max(semana_fim) from core_gcat.processlog where sourcefile = 'SAINT_MARCHE' and semana_fim rlike '[0-9][0-9][0-9][0-9][0-9][0-9]';")
T_LASTMONTH=$(($T_LASTMONTH + 1))
###T_LASTMONTH="201806" 

if [[ -z `hdfs dfs -ls "$BLOB_GCAT_PATH"/* |gawk 'BEGIN{FS="/"}{if ($5 ~ /^[0-9][0-9][0-9][0-9][0-9][0-9]_[Mm][Aa][Rr][Cc][Hh][Ee]_[Cc][Nn][Pp][Jj].[Xx][Ll][Ss][Xx]/) print $5;}'` ]] 
        then
		   echo "-->> Nao encontrado arquivo no blob gcat."
		   exit 0
		else 
		   echo "-->> Encontrado arquivo no blob gcat."
		   filenames=`hdfs dfs -ls "$BLOB_GCAT_PATH"/* |gawk 'BEGIN{FS="/"}{if ($5 ~ /^[0-9][0-9][0-9][0-9][0-9][0-9]_[Mm][Aa][Rr][Cc][Hh][Ee]_[Cc][Nn][Pp][Jj].[Xx][Ll][Ss][Xx]/) print $5;}'` 
           array=($filenames)
           for((i=0;i<${#array[@]};i++))
           do
			      TEMP=${array[$i]}
				    T_MONTH=${TEMP:0:6}
		         arquivo=`echo $T_LASTMONTH`"_MARCHE_CNPJ.xlsx"
				   #verifica se achou o proximo arquivo esperado
				   if [ "$arquivo" = "$TEMP" ]
				    then  
					   echo "Achou"                       					   
				       echo "$T_MONTH"
                       #echo "${array[$i]}"
				       ##arquivo copiado do blob gcat ambprojselloutcube para o ambcluster
                       hdfs dfs -get $BLOB_GCAT_PATH/${array[$i]}  ~/raw/gcat/saint_marche/${array[$i]}
				       ##arquivo copiado para o blob ambcluster
                       hdfs dfs -put -f ~/raw/gcat/saint_marche/${array[$i]} /temp/saint_marche/orig/"gcat_saint_marche.xlsx"
				       ##exclui arquivo do raw/gcat/saint_marche
				       rm ~/raw/gcat/saint_marche/${array[$i]}
				       ##insere registro na tabela de log com o ano/mes carregado
                       /usr/bin/hive -S -e "insert into raw_gcat.processlog_saint_marche (sourcefile,targettable,semana_fim,localtimestamp) values ('SAINT_MARCHE', '${array[$i]}' ,'$T_MONTH','$DATE_WITH_TIME');"
                       
					  else echo "Nao achou ainda" 
                 exit 0   
          fi					
          done
fi

