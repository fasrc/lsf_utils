#!/usr/bin/env perl

#
# pull statistics from a mysql db with lsf acct info
#

$database = "XXXXXXXX";
$host = "XXXXXXXX";
$port = "XXXXXXXX";
$user = "XXXXXXXX";
$pass = "XXXXXXXX";

use DBI;
$|=1;

#if ($ARGV[0] eq ""){
#   print "Usage: getstats.pl <Clustername> <days back>\n";
#   exit(0);
#}



$dbh = DBI->connect
  ("dbi:mysql:database=$database:host=$host:port=$port",
   "$user", "$pass") || die $DBI::errstr;

#$tablename = "Account_".$ARGV[0];
$tablename = "job_finish";


$sth=$dbh->prepare("select
FROM_UNIXTIME(min(startTime)),FROM_UNIXTIME(max(startTime)) from
$tablename");


$sth->execute();
($starttime,$endtime)=$sth->fetchrow();

print "Statistics for $tablename\n";
print "Start Date:\t\t$starttime\n";
print "End Date:\t\t$endtime\n";

getstat("queue");
getstat("userName");
getcmd("command");
getstat("execHost");

sub getcmd {

$id=shift(@_);

$sth=$dbh->prepare("select userName,sum(utime),
                    sum(stime),
                    $id from $tablename group by $id order by 2 desc
limit 30;");

$sth->execute;

print "Top 30 longest running jobs:\n\n"; 

printf ("%-10s %-10s %-15s %s\n\n","userName","Usr (hrs)", "stime (hrs)","Command");

while (($username,$usrtime, $systime,$cmd)=$sth->fetchrow()){

   $shortcmd = substr ($cmd,0,85);

   printf ("%-10s %-10.2f %-15.2f %s\n",$username,$usrtime,$systime,$shortcmd);

  }

print "\n\n";

}

sub getstat {

  $id = shift(@_); 
  $sth = $dbh->prepare("select sum(utime),
                               sum(stime),
                               sum(maxRMem),count(command),
                               sum(((endTime-startTime)/60)/60),
                               $id from $tablename group by $id;");

  printf ("\n%-16s %10s %10s %-15s %-10s %-6s %-6s %10s %-10s\n\n"
          ,$id,"Usr (hrs)","Sys (hrs)","  Tot (hrs)","Mem (GB)",
          " Jobs","Util (%)","Real (hrs)"," Wait (hrs)");
 
  $sth->execute();
  open TMPF, ">/tmp/lsf.stats.$$";
  $tusr=$ttot=$tsys=$tmem=$tcmd=$treal=$twait=0;
  
  while (($usrtime,$systime,$memuse,$numcmd,$startend,$usrname)=$sth->fetchrow()){
 
        if ($usrtime<0){$usrtime=0}
        if ($systime<0){$systime=0}
        $tottime=$usrtime+$systime;
        if ($memuse < 0){$memuse=0};
        $memuse=($memuse/1024);
        if ($tottime >0){
          $Util=($usrtime/$tottime)*100;
        }
        $waittime=$startend-$tottime;
        if ($waittime < 0){$waittime=0};

        # hack to remove broken broad/mit names:
        $usrname=~s/.mit.edu//g;
#       $usrname=~s/.mit.edu//g;
      
        printf (TMPF "%-15s %10.2f %10.2f %13.2f %10.1f %10d %6d %10.1f %10.1f\n",
 
        $usrname,$usrtime,$systime,$tottime,$memuse,$numcmd,$Util,$startend,$waittime);
        $tusr=$tusr+$usrtime;
        $tsys=$tsys+$systime;
        $ttot=$ttot+$tottime;
        $tmem=$tmem+$memuse;
        $tcmd=$tcmd+$numcmd;
        $treal=$treal+$startend;
        $twait=$twait+$waittime;
      }
      
  close TMPF;
  #system ("sort -r -n +1 /tmp/lsf.stats.$$");
  system ("sort -r -n /tmp/lsf.stats.$$");

  printf ("\n%-15s %10.2f %10.2f %13.2f %10.1f %10d %6s %10.1f %10.1f\n\n","Total:",
          $tusr,$tsys,$ttot,$tmem,$tcmd,"-",$treal,$twait);
  system ("rm -rf /tmp/lsf.stats.$$");
}
