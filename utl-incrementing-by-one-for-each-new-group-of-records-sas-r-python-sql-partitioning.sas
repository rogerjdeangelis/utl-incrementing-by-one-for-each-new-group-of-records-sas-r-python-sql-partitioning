%let pgm=utl-incrementing-by-one-for-each-new-group-of-records-sas-r-python-sql-partitioning;

Incrementing by one for each new group of records sas r python sql

    SOLUTIONS (SOME AR NOT LARGE DATA SOLUTIONS)

       1 sas datastep ( the groups do not need to be sorted but need to be grouped).
         by cluster notsorted; (BEST LARGE BIG DATA SOLUTION)
       2 sas sql
       3 r sql
       4 r no sql (can leverage to other languages)
       5 sas like the r code
       6 python sql
       7 python no sql (not easily leveraged to other languages. somewhat unusual syntax)
         have['UNIQUE_ID'] = pd.factorize(have['CLUSTER'])[0] + 1
         https://stackoverflow.com/users/18470692/ouroboros1
       8 python no sql
         crrent['UNIQUE_ID'] = (~have['CLUSTER'].duplicated()).cumsum()
         https://stackoverflow.com/users/11564487/pauls

github
https://tinyurl.com/yk44p9ym
https://github.com/rogerjdeangelis/utl-incrementing-by-one-for-each-new-group-of-records-sas-r-python-sql-partitioning


SOAPBOX ON
AS A SIDE NOTE
  SAS FEDSQL does not support 'OVER' clause or ROW_NUMBER?
SOAPBOX OFF

stackoverflow r
https://stackoverflow.com/questions/78984852/renumerate-a-column-in-a-group

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                             |                                                                          */
/*                               ADD THIS      | SAS DATASTEP SOLUTION (SELF EXPLANATORY?)                                */
/*    NAME    SCORE    CLUSTER   COLUMN GRP    |                                                                          */
/*                                             | data want;                                                               */
/*    Adam      1         22       1           |   retain grp 0;                                                          */
/*    Eddy      0         22       1           |   set sd1.have;                                                          */
/*    Boby      9         22       1           |   by cluster;                                                            */
/*                                             |   grp=ifn(first.cluster,grp+1,grp);                                      */
/*    Timy      2         26       2           | run;quit                                                                 */
/*    Carl      9         26       2           |                                                                          */
/*                                             |                                                                          */
/*    Anna      0         33       3           |                                                                          */
/*                                             |                                                                          */
/*    Paul      5         35       4           |                                                                          */
/*                                             |                                                                          */
/*    Mike      7         51       5           |                                                                          */
/*                                             |                                                                          */
/* --------------------------------------------=--------------------------------------------------------------------------*/
/*                                                       |                                                                */
/* MATCHING R and SAS CODE SAME OUTPUT (SAS PROTOTYPING) |                                                                */
/* ===================================================== |                                                                */
/*                                                       |                                                                */
/*   R CODE                                              | SAS CODE                                                       */
/*                                                       |                                                                */
/*                                                       | set sd1.have;                                                  */
/*   have<-read_sas("d:/sd1/have.sas7bdat")              | array hav %utl_numary(sd1.have,drop=name score);               */
/*                                                       |                                                                */
/*   have[1,4]=1                                         | inc=1.0;                                                       */
/*   inc=1.0                                             | output;                                                        */
/*                                                       |                                                                */
/*   for ( ro in seq(2,nrow(have),1) ) {                 | do ro=2 to dim1(hav);                                          */
/*                                                       |                                                                */
/*     if ( have[ro,3] != have[ro-1,3] ) { inc=inc+1.0 } |   if ( hav[ro,1] ne hav[ro-1,1] ) then inc=inc+1.0;            */
/*     have[ro,4]=inc                                    |   output;                                                      */
/*                                                       |                                                                */
/*     }                                                 | end;                                                           */
/*                                                       |                                                                */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
 input name $ score cluster;
cards4;
Adam 1 22
Eddy 0 22
Boby 9 22
Timy 2 26
Carl 9 26
Anna 0 33
Paul 5 35
Mike 7 51
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SD1.HAVE total obs=8                                                                                                   */
/*                                                                                                                        */
/* Obs    NAME    SCORE    CLUSTER                                                                                        */
/*                                                                                                                        */
/*  1     Adam      1         22                                                                                          */
/*  2     Eddy      0         22                                                                                          */
/*  3     Boby      9         22                                                                                          */
/*  4     Timy      2         26                                                                                          */
/*  5     Carl      9         26                                                                                          */
/*  6     Anna      0         33                                                                                          */
/*  7     Paul      5         35                                                                                          */
/*  8     Mike      7         51                                                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                       _       _            _
/ |  ___  __ _ ___    __| | __ _| |_ __ _ ___| |_ ___ _ __
| | / __|/ _` / __|  / _` |/ _` | __/ _` / __| __/ _ \ `_ \
| | \__ \ (_| \__ \ | (_| | (_| | || (_| \__ \ ||  __/ |_) |
|_| |___/\__,_|___/  \__,_|\__,_|\__\__,_|___/\__\___| .__/
                                                     |_|
*/
data want;
  set sd1.have;
  by cluster notsorted;
  grp=ifn(first.cluster,grp+1,grp);
  retain grp 0;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  WORK.WANT total obs=8 1                                                                                               */
/*                                                                                                                        */
/*   NAME    SCORE    CLUSTER    GRP                                                                                      */
/*                                                                                                                        */
/*   Adam      1         22       1                                                                                       */
/*   Eddy      0         22       1                                                                                       */
/*   Boby      9         22       1                                                                                       */
/*   Timy      2         26       2                                                                                       */
/*   Carl      9         26       2                                                                                       */
/*   Anna      0         33       3                                                                                       */
/*   Paul      5         35       4                                                                                       */
/*   Mike      7         51       5                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                              _
|___ \   ___  __ _ ___   ___  __ _| |
  __) | / __|/ _` / __| / __|/ _` | |
 / __/  \__ \ (_| \__ \ \__ \ (_| | |
|_____| |___/\__,_|___/ |___/\__, |_|
                                |_|
*/

proc sql;
    create
       view grpUnq as
    select
      cluster
     ,monotonic() as grp
    from
      sd1.have
    group
      by cluster
    having
      cluster = max(cluster)
    ;
    create
      table sqlwant as
    select
      l.*
     ,r.grp
    from
      sd1.have as l left join grpUnq as r
    on
      l.cluster = r.cluster
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* same output                                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                    _
|___ /   _ __   ___  __ _| |
  |_ \  | `__| / __|/ _` | |
 ___) | | |    \__ \ (_| | |
|____/  |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.r")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
want<- sqldf('
 select
     l.*
    ,r.unique_id
 from
     have as l left join
        (select
            cluster
           ,row_number() over (order by cluster) as unique_id
         from
            have
         group
            by cluster
         ) as r
  on
     l.cluster = r.cluster
  ')
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="rwant"
     )
;;;;
%utl_rendx;

libname sd1 "d:/sd1";
proc print data=sd1.rwant;
run;quit;

 /**************************************************************************************************************************/
 /*                                                                                                                        */
 /* R                                                                                                                      */
 /*                                                                                                                        */
 /* > want                                                                                                                 */
 /*   NAME SCORE CLUSTER unique_id                                                                                         */
 /* 1 Adam     1      22         1                                                                                         */
 /* 2 Eddy     0      22         1                                                                                         */
 /* 3 Boby     9      22         1                                                                                         */
 /* 4 Timy     2      26         2                                                                                         */
 /* 5 Carl     9      26         2                                                                                         */
 /* 6 Anna     0      33         3                                                                                         */
 /* 7 Paul     5      35         4                                                                                         */
 /* 8 Mike     7      51         5                                                                                         */
 /*                                                                                                                        */
 /* SAS                                                                                                                    */
 /*                                          UNIQUE_                                                                       */
 /*  ROWNAMES    NAME    SCORE    CLUSTER       ID                                                                         */
 /*                                                                                                                        */
 /*      1       Adam      1         22         1                                                                          */
 /*      2       Eddy      0         22         1                                                                          */
 /*      3       Boby      9         22         1                                                                          */
 /*      4       Timy      2         26         2                                                                          */
 /*      5       Carl      9         26         2                                                                          */
 /*      6       Anna      0         33         3                                                                          */
 /*      7       Paul      5         35         4                                                                          */
 /*      8       Mike      7         51         5                                                                          */
 /*                                                                                                                        */
 /**************************************************************************************************************************/

/*  _                                  _
| || |    _ __   _ __   ___  ___  __ _| |
| || |_  | `__| | `_ \ / _ \/ __|/ _` | |
|__   _| | |    | | | | (_) \__ \ (_| | |
   |_|   |_|    |_| |_|\___/|___/\__, |_|
                                    |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
source("c:/oto/fn_tosas9x.r")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
have[1,4]=1
inc=1.0
for ( ro in seq(2,nrow(have),1) ) {
  if ( have[ro,3] != have[ro-1,3] ) { inc=inc+1.0 }
  have[ro,4]=inc
  }
colnames(have)[4]<-"unique_id"
have
fn_tosas9x(
      inp    = have
     ,outlib ="d:/sd1/"
     ,outdsn ="rnosql"
     )
;;;;
%utl_rendx;

libname sd1 "d:/sd1";
proc print data=sd1.rnosql;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* same output                                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                    _ _ _
| ___|   ___  __ _ ___  | (_) | _____   _ __
|___ \  / __|/ _` / __| | | | |/ / _ \ | `__|
 ___) | \__ \ (_| \__ \ | | |   <  __/ | |
|____/  |___/\__,_|___/ |_|_|_|\_\___| |_|

*/

%utl_numary(sd1.have,drop=name score);

data sasrwant;
  set sd1.have;
  array hav %utl_numary(sd1.have,drop=name score);
  inc=1.0;
  output;
  do ro=2 to dim1(hav);
    if ( hav[ro,1] ne hav[ro-1,1] ) then inc=inc+1.0;
    output;
  end;
  keep name score cluster inc;
  stop;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* same output                                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*__                 _   _                             _
 / /_    _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| `_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| (_) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
 \___/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

%utl_pybeginx;
parmcards4;
import pyarrow.feather as feather
import tempfile
import pyperclip
import os
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
exec(open('c:/oto/fn_tosas9x.py').read())
have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat")
want=pdsql("""
 select
     l.*
    ,r.unique_id
 from
     have as l left join
        (select
            cluster
           ,row_number() over (order by cluster) as unique_id
         from
            have
         group
            by cluster
         ) as r
  on
     l.cluster = r.cluster
   """)
print(want)
fn_tosas9x(want,outlib="d:/sd1/",outdsn="rwant",timeest=3)
;;;;
%utl_pyendx;

libname sd1 "d:/sd1";
proc print data=sd1.rwant;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* PYTHON                                                                                                                 */
/*                                                                                                                        */
/*    NAME  SCORE  CLUSTER  UNIQUE_ID                                                                                     */
/* 0  Adam    1.0     22.0          1                                                                                     */
/* 1  Eddy    0.0     22.0          1                                                                                     */
/* 2  Boby    9.0     22.0          1                                                                                     */
/* 3  Timy    2.0     26.0          2                                                                                     */
/* 4  Carl    9.0     26.0          2                                                                                     */
/* 5  Anna    0.0     33.0          3                                                                                     */
/* 6  Paul    5.0     35.0          4                                                                                     */
/* 7  Mike    7.0     51.0          5                                                                                     */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/*                                    UNIQUE_                                                                             */
/* Obs    NAME    SCORE    CLUSTER       ID                                                                               */
/*                                                                                                                        */
/*  1     Adam      1         22         1                                                                                */
/*  2     Eddy      0         22         1                                                                                */
/*  3     Boby      9         22         1                                                                                */
/*  4     Timy      2         26         2                                                                                */
/*  5     Carl      9         26         2                                                                                */
/*  6     Anna      0         33         3                                                                                */
/*  7     Paul      5         35         4                                                                                */
/*  8     Mike       7         51         5                                                                                */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____               _   _                                           _
|___  |  _ __  _   _| |_| |__   ___  _ __    _ __   ___    ___  __ _| |
   / /  | `_ \| | | | __| `_ \ / _ \| `_ \  | `_ \ / _ \  / __|/ _` | |
  / /   | |_) | |_| | |_| | | | (_) | | | | | | | | (_) | \__ \ (_| | |
 /_/    | .__/ \__, |\__|_| |_|\___/|_| |_| |_| |_|\___/  |___/\__, |_|
        |_|    |___/                                              |_|
*/

%utl_pybeginx;
parmcards4;
import pyarrow.feather as feather
import tempfile
import pyperclip
import os
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
exec(open('c:/oto/fn_tosas9x.py').read())
have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat")
have['UNIQUE_ID'] = pd.factorize(have['CLUSTER'])[0] + 1
print(have)
fn_tosas9x(have,outlib="d:/sd1/",outdsn="pywant7",timeest=3)
;;;;
%utl_pyendx;

libname sd1 "d:/sd1";
proc print data=sd1.pywant7;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* PYTHON                                                                                                                 */
/*                                                                                                                        */
/*    NAME  SCORE  CLUSTER  UNIQUE_ID                                                                                     */
/* 0  Adam    1.0     22.0          1                                                                                     */
/* 1  Eddy    0.0     22.0          1                                                                                     */
/* 2  Boby    9.0     22.0          1                                                                                     */
/* 3  Timy    2.0     26.0          2                                                                                     */
/* 4  Carl    9.0     26.0          2                                                                                     */
/* 5  Anna    0.0     33.0          3                                                                                     */
/* 6  Paul    5.0     35.0          4                                                                                     */
/* 7  Mike    7.0     51.0          5                                                                                     */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/*                                    UNIQUE_                                                                             */
/* Obs    NAME    SCORE    CLUSTER       ID                                                                               */
/*                                                                                                                        */
/*  1     Adam      1         22         1                                                                                */
/*  2     Eddy      0         22         1                                                                                */
/*  3     Boby      9         22         1                                                                                */
/*  4     Timy      2         26         2                                                                                */
/*  5     Carl      9         26         2                                                                                */
/*  6     Anna      0         33         3                                                                                */
/*  7     Paul      5         35         4                                                                                */
/*  8     Mike       7         51         5                                                                                */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                _   _                             _
 ( _ )   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
 / _ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| (_) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
 \___/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

%utl_pybeginx;
parmcards4;
import pyarrow.feather as feather
import tempfile
import pyperclip
import os
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
exec(open('c:/oto/fn_tosas9x.py').read())
have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat")
have['UNIQUE_ID'] = (~have['CLUSTER'].duplicated()).cumsum()
fn_tosas9x(have,outlib="d:/sd1/",outdsn="pywant8",timeest=3)
print(have)
;;;;
%utl_pyendx;

libname sd1 "d:/sd1";
proc print data=sd1.pywant8;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* same output as 7                                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
