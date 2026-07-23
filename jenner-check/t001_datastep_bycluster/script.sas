/* Source: utl-incrementing-by-one-for-each-new-group-of-records-sas-r-python-sql-partitioning.sas
   Solution 1 - SAS datastep (the author's "BEST LARGE BIG DATA SOLUTION").
   Only change vs upstream: the input dataset is built in WORK instead of the
   hardcoded "libname sd1 'd:/sd1'" location. The cards4 data block and the
   grouping logic (by cluster notsorted; grp=ifn(first.cluster,grp+1,grp)) are
   the author's, unchanged. */

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

data want;
  set have;
  by cluster notsorted;
  grp=ifn(first.cluster,grp+1,grp);
  retain grp 0;
run;quit;

proc print data=want;
run;quit;
