#!/usr/bin/perl
use warnings;
use strict;

eval { require DBI; require DBD::CSV; };
if ($@) {
    die 'No DBD::CSV available';
}

my $dbh  = DBI->connect('dbi:CSV:',,,{RaiseError=>1,PrintError=>1});
foreach my $stmt ( split(';',join('',<DATA>)) )
{
    $dbh->do($stmt);
}
foreach my $num ( 1..26 )
{
    my $t2num = ( ( $num - 1 ) * 2 + 1 );
    my $t2val = chr( ord('z') - $num + 1 ) x 3;
    foreach my $stmt (
        sprintf( q{INSERT INTO t1 VALUES (%d, '%s')}, $num, chr($num + ord('a') - 1) ),
        sprintf( q{INSERT INTO t1 VALUES (%d, '%s')}, $num+26, chr($num + ord('A') - 1) ),
        sprintf( q{INSERT INTO t2 VALUES (%d, '%s')}, $t2num, $t2val ),
        sprintf( q{INSERT INTO t2 VALUES (%d, '%s')}, $t2num+52, uc($t2val) ),
        sprintf( q{INSERT INTO t3 VALUES (%d, %d)}, $num, $t2num ),
        sprintf( q{INSERT INTO t3 VALUES (%d, %d)}, $num+26, $t2num+52 ),
    )
    {
        $dbh->do( $stmt );
    }
}

my $max = 100;
for my $i (1..$max)
{
    my $sth = $dbh->prepare("SELECT * FROM t1 CROSS JOIN t2");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 INNER JOIN t2 ON t1.num = t2.num" );
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 INNER JOIN t2 USING (num)");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 NATURAL INNER JOIN t2 ");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 USING (num)");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 RIGHT JOIN t2 ON t1.num = t2.num");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t2 LEFT JOIN t1 ON t1.num = t2.num");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 FULL JOIN t2 ON t1.num = t2.num");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num AND t2.wert = 'xxx'");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num WHERE (t2.wert = 'xxx' OR t2.wert IS NULL)");
    $sth->execute();

    $sth = $dbh->prepare( "SELECT * FROM t1, t2, t3 WHERE t1.num = t3.t1num AND t2.num = t3.t2num" );
    $sth->execute();
}

__DATA__
CREATE TEMP TABLE t1    (num INT, name TEXT);
CREATE TEMP TABLE t2    (num INT, wert TEXT);
CREATE TEMP TABLE t3    (t1num INT, t2num INT)
