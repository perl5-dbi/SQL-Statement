#!/usr/bin/perl -w
$|=1;
use strict;
use lib  qw( ../lib );
use vars qw($DEBUG);
use Test::More;
eval { require DBI; require DBD::File;};
if ($@) {
    plan skip_all => "No DBI or DBD::File available";
}
elsif ($DBD::File::VERSION < '0.033' ) {
    plan skip_all => "Tests require DBD::File >= 0.33";
}
else {
    plan tests => 12;
}
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
my $dbh=DBI->connect('dbi:File(RaiseError=0):');

my $t='TEMP';
my %create = (
    lower      => "CREATE $t TABLE tbl (col INT)",
    upper      => "CREATE $t TABLE tbl (COL INT)",
    mixed      => "CREATE $t TABLE tbl (cOl INT)",
);
my %query = (
    lower      => "SELECT col FROM tbl WHERE 1=0",
    upper      => "SELECT COL FROM tbl WHERE 1=0",
    mixed      => "SELECT cOl FROM tbl WHERE 1=0",
    asterisked => "SELECT *   FROM tbl WHERE 1=0",
);

$DEBUG=0;
if ($DEBUG) {
    my $pg  = DBI->connect('dbi:Pg(RaiseError=1):dbname=test1');
#    eval { $pg->do("DROP TABLE pg") };
    $pg->do("CREATE TABLE pg (col INT, col2 INT)");
    $pg->do("INSERT INTO pg VALUES (3,7)");
    die unless 3 == $pg->selectrow_array("SELECT col FROM pg");
    if ($SQL::Statement::VERSION < '1.10') {
        $dbh->func('tbl','DBI',$pg,
            {sql=>'SELECT * FROM pg',keep_connection=>1}
        ,'ad_import');
    }
    else {
        my $pg_sth = $pg->prepare("SELECT * FROM pg WHERE 1=0");
        $pg_sth->execute;
        $dbh->do("CREATE TABLE tbl AS IMPORT(?)",{},$pg_sth);
    }
    for my $query_case(qw(lower upper mixed asterisked)) {
        my $sth = $dbh->prepare( $query{$query_case} );
        $sth->execute;
        my $msg = sprintf "imported table : %s", $query_case;
        my $col = $sth->{NAME}->[0];
        ok($col eq 'col',$msg) if $query_case eq 'lower';
        ok($col eq 'COL',$msg) if $query_case eq 'upper';
        ok($col eq 'cOl',$msg) if $query_case eq 'mixed';
        ok($col eq 'col',$msg) if $query_case eq 'asterisked';
        $sth->finish;
        $sth->{Active}=0;
    }
    $pg->do("DROP TABLE pg");
    $dbh->do("DROP TABLE IF EXISTS tbl");
    $pg->disconnect;
}

for my $create_case(qw(lower upper mixed)) {
    eval{ $dbh->do("DROP TABLE IF EXISTS tbl") };
    $dbh->do( $create{$create_case} );
    for my $query_case(qw(lower upper mixed asterisked)) {
        my $sth = $dbh->prepare( $query{$query_case} );
        $sth->execute;
        my $msg = sprintf "%s/%s", $create_case, $query_case;
        my $col = $sth->{NAME}->[0];
        ok($col eq 'col',$msg) if $query_case eq 'lower';
        ok($col eq 'COL',$msg) if $query_case eq 'upper';
        ok($col eq 'cOl',$msg) if $query_case eq 'mixed';
        if ($query_case eq 'asterisked') {
            ok($col eq 'col',$msg) if $create_case eq 'lower';
            ok($col eq 'COL',$msg) if $create_case eq 'upper';
            ok($col eq 'cOl',$msg) if $create_case eq 'mixed';
	}
        $sth->finish;
        $sth->{Active}=0;
    }
}
__END__
PostgreSQL
  Case insensitive comparisons
  Always stores in lower case
  Always returns lower case

S::S 0.x
  Case *sensitive* comparisons (if you created with "MYCOL" you can
     not query with "mycol" or "MyCol")
  Stores in mixed case
  Always returns stored case

SQLite and S::S 1.x
  Case insensitive comparisons
  Stores in mixed case
  Returns stored case for *, query case otherwise

Returns stored case for asterisked queries
  * except in 1.12 with TEMP files, upper-cases columns
Returns query case if columns are specified in query

S::S 1.12
  file-based table :  same as 1.x
  TEMP table       :  same, except upper cases on asterisked queries
  imported table   :  same, except upper cases on asterisked queries


=============================================================================
work in 0.1021
  all asterisked (l*,m*,u*)
  all where create is same as query (ll,uu,mm)
die in 0.1021
  all where query case is specified and different from create case
    (mu,ml,um,ul,lm,lu)


        ok($col eq 'COL',$msg) if $query_case eq 'asterisked';
exit;
my $tbl = 'case';
$dbh->do("CREATE TEMP TABLE $tbl (lower INT)");
my $sth = $dbh->prepare("SELECT * FROM $tbl WHERE 1=0");
$sth->execute;
printf "%s\n",join',',@{$sth->{NAME}};
$sth->finish;

$sth = $dbh->prepare("SELECT lower FROM $tbl WHERE 1=0");
$sth->execute;
printf "%s\n",join',',@{$sth->{NAME}};
$sth->finish;

$sth = $dbh->prepare("SELECT LOWER FROM $tbl WHERE 1=0");
$sth->execute;
printf "%s\n",join',',@{$sth->{NAME}};
$sth->finish;
$sth = $dbh->prepare("SELECT LoweR FROM $tbl WHERE 1=0");
$sth->execute;
printf "%s\n",join',',@{$sth->{NAME}};
$sth->finish;
$dbh->do($_) for <DATA>;

$sth=$dbh->prepare("
    SELECT SUM(sales), MAX(sales) FROM biz
");
ok('2700~1000^' eq query2str($sth),'AGGREGATE FUNCTIONS WITHOUT GROUP BY');

$sth=$dbh->prepare("
    SELECT region,SUM(sales), MAX(sales) FROM biz GROUP BY region
");
ok('West~2000~1000^East~700~700^' eq query2str($sth),'GROUP BY one column');

$sth=$dbh->prepare("
    SELECT region,store,SUM(sales), MAX(sales) FROM biz GROUP BY region,store
");
ok('West~Los Angeles~1500~1000^West~San Diego~500~500^East~Boston~700~700^'
 eq query2str($sth),'GROUP BY several columns');


sub query2str {
    my($sth)=@_;
    $sth->execute;
    my $str='';
    while (my $r=$sth->fetch) {
        $str .= sprintf "%s^",join('~',map { defined $_ ? $_ : 'undef' } @$r);
    }
    return $str unless $DEBUG;
    printf "%s\n",join',',@{$sth->{NAME}};
    print "<$str>\n";
    return $str;
}
__END__
CREATE TEMP TABLE biz (region TEXT, store TEXT, sales INTEGER)
INSERT INTO biz VALUES ('West','Los Angeles',1000 )
INSERT INTO biz VALUES ('West','San Diego'  ,500  )
INSERT INTO biz VALUES ('West','Los Angeles',500  )
INSERT INTO biz VALUES ('East','Boston'     ,700  )
