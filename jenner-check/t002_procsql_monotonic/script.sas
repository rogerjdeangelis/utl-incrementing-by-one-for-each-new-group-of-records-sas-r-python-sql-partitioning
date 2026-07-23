/* Source: utl-incrementing-by-one-for-each-new-group-of-records-sas-r-python-sql-partitioning.sas
   Solution 2 - SAS SQL. Builds a per-cluster group number with monotonic()
   over a grouped view, then left-joins it back to the input rows.
   Only change vs upstream: the input dataset is built in WORK instead of the
   hardcoded "libname sd1 'd:/sd1'" location. The cards4 data block and the
   PROC SQL (view grpUnq with monotonic(), the left join into sqlwant) are the
   author's, unchanged. */

data have;
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

proc sql;
    create
       view grpUnq as
    select
      cluster
     ,monotonic() as grp
    from
      have
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
      have as l left join grpUnq as r
    on
      l.cluster = r.cluster
;quit;

proc print data=sqlwant;
run;quit;
