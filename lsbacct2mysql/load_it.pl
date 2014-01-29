#!/util/bin/perl

#
# load up an lsb events file into mysql
#

# useage load_lsbacct <clustername> <lsbeventsfile>

# uses parse ( c code to pull out data from the lsb events files)

$database = "lsflnx";
$host = "mysql";
$user = "ensrw";
$pass = "ensembl";

use DBI;
$|=1;

#LSF_BINDIR=/usr/lsf/mnt/5.1/linux2.4-glibc2.3-x86/bin
#LSF_SERVERDIR=/usr/lsf/mnt/5.1/linux2.4-glibc2.3-x86/etc
#LSF_LIBDIR=/usr/lsf/mnt/5.1/linux2.4-glibc2.3-x86/lib
#XLSF_UIDDIR=/usr/lsf/mnt/5.1/linux2.4-glibc2.3-x86/lib/uid

#$ENV{'LSF_ENVDIR'}='/home/radon00/jcuff/lsf_accounting';



$dbh = DBI->connect
  ("dbi:mysql:database=$database:host=$host",
   "$user", "$pass") || die $DBI::errstr;


#$lsbacctfile="/usr/local/lsfv42/work/acari/logdir/lsb.acct";

if ($ARGV[1] eq "") {
        print "Useage: load_lsb_acct.pl <clustername>
<lsbeventsfilename> <append>\n";
        print "e.g. load_lsb_acct.pl Ensembl
/usr/local/lsfv42/work/acari/logdir/lsb.acct.1 append\n";
        exit(0);
}
open IN, "/home/radon00/jcuff/lsf_accounting/parse $ARGV[1]|";

#$indexload = $dbh->prepare("insert delayed into Account_$ARGV[0]
values(?,?,?,?,?,?,?,?,?,?)");

$filename="lsfloader.$$";

#reset the account table....

print "$ARGV[2]\n";

if ($ARGV[2] ne "append"){
  print "Drop DB...\n";
  $dbh->do("DROP TABLE Account_$ARGV[0]");

print "Create DB...\n";
 $dbh->do("CREATE TABLE Account_$ARGV[0]
  (id INT NOT NULL AUTO_INCREMENT,
  UsrName varchar(25),
  HostName varchar(50),
  Queue varchar(255),
  UsrTime FLOAT,
  SysTime FLOAT,
  MemUse  FLOAT,
  StartTime INTEGER,
  EndTime INTEGER,
  Status INTEGER,
  Cmd BLOB,
  KEY usrnameidx(UsrName),
  KEY startidx(StartTime),
  KEY endidx(EndTime),
  PRIMARY KEY (id));");
}


open OUT, ">/ahg/work1/LSFstats/tmp/$filename";

print "Generating Bulk upload file...\n";

while (<IN>){

  # parse the lines and insert, should all be separated by white
space...

 
($UsrName,$HostName,$Queue,$UsrTime,$SysTime,$MaxMem,$StartTime,$EndTime
,$Status,$cmd)=
        split(" ",$_,10);
   chop $cmd;
   if ($StartTime !=0 && $EndTime !=0){
     print OUT
"NULL\t$UsrName\t$HostName\t$Queue\t$UsrTime\t$SysTime\t$MaxMem\t$StartT
ime\t$EndTime\t$Status\t$cmd\n";
   }
}

close IN;
close OUT;

print "Completed Bulk upload file, now loading into Mysql...\n";

# finished dump - now do bulk upload...


$sql="load data local infile \"/ahg/work1/LSFstats/tmp/$filename\" into
table Account_$ARGV[0];";


$dbh->do("$sql");

#system ("rm /tmp/$filename");
