#!/usr/bin/perl
use warnings;
use strict;
use Test::More;

eval { require DBI; require DBD::CSV; };
if ($@) {
    plan skip_all => "No DBD::CSV available";
}
else {
    plan tests => 48;
}

my $dbh  = DBI->connect('dbi:CSV:',,,{RaiseError=>1,PrintError=>1});
$dbh->do($_) for split(';',join('',<DATA>));

my $sth = $dbh->prepare("SELECT * FROM t1 CROSS JOIN t2");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    |   1 | xxx
   1 | a    |   3 | yyy
   1 | a    |   5 | zzz
   2 | b    |   1 | xxx
   2 | b    |   3 | yyy
   2 | b    |   5 | zzz
   3 | c    |   1 | xxx
   3 | c    |   3 | yyy
   3 | c    |   5 | zzz

=cut

my $names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,num,wert}, 'Cross Joins - columns ok' );
for my $res (
    q{1,a,1,xxx},
    q{1,a,3,yyy},
    q{1,a,5,zzz},
    q{2,b,1,xxx},
    q{2,b,3,yyy},
    q{2,b,5,zzz},
    q{3,c,1,xxx},
    q{3,c,3,yyy},
    q{3,c,5,zzz},
)
{
    my $values = sprintf( q{%s}, join( q{,}, $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Cross Joins - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 INNER JOIN t2 ON t1.num = t2.num" );
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    |   1 | xxx
   1 | a    |   3 | yyy

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,num,wert}, 'Inner Joins - columns ok' );
for my $res (
    q{1,a,1,xxx},
    q{3,c,3,yyy},
)
{
    my $values = sprintf( q{%s}, join( q{,}, $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Inner Joins - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 INNER JOIN t2 USING (num)");
$sth->execute();

=pod

Results:

 num | name | wert
-----+------+------
   1 | a    | xxx
   3 | c    | yyy

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,wert}, 'Inner Joins (USING) - columns ok' );
for my $res (
    q{1,a,xxx},
    q{3,c,yyy},
)
{
    my $values = sprintf( q{%s}, join( q{,}, $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Inner Joins (USING) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 NATURAL INNER JOIN t2 ");
$sth->execute();

=pod

Results:

 num | name | wert
-----+------+------
   1 | a    | xxx
   3 | c    | yyy

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,wert}, 'Inner Joins (NATURAL) - columns ok' );
for my $res (
    q{1,a,xxx},
    q{3,c,yyy},
)
{
    my $values = sprintf( q{%s}, join( q{,}, $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Inner Joins (NATURAL) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    | 1   | xxx
   2 | b    |     |
   3 | c    | 3   | yyy

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,num,wert}, 'Left Joins (using ON condition) - columns ok' );
for my $res (
    q{'1','a','1','xxx'},
    q('2','b',,),
    q{'3','c','3','yyy'},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Left Joins (using ON condition) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 USING (num)");
$sth->execute();

=pod

Results:

 num | name | wert
-----+------+------
   1 | a    | xxx
   2 | b    | 
   3 | c    | yyy

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,wert}, 'Left Joins (USING (num) condition) - columns ok' );
for my $res (
    q{'1','a','xxx'},
    q('2','b',),
    q{'3','c','yyy'},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Left Joins (USING (num) condition) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 RIGHT JOIN t2 ON t1.num = t2.num");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    | 1   | xxx
   3 | c    | 3   | yyy
     |      | 5   | zzz

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,wert,num,name}, 'Right Joins (using ON condition) - columns ok' );
for my $res (
    q{'1','xxx','1','a'},
    q{'3','yyy','3','c'},
    q{'5','zzz',,},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Right Joins (using ON condition) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t2 LEFT JOIN t1 ON t1.num = t2.num");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    | 1   | xxx
   3 | c    | 3   | yyy
     |      | 5   | zzz

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,wert,num,name}, 'Left Joins (reverse former Right Join) - columns ok' );
for my $res (
    q{'1','xxx','1','a'},
    q{'3','yyy','3','c'},
    q{'5','zzz',,},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Left Joins (reverse former Right Join) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 FULL JOIN t2 ON t1.num = t2.num");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    | 1   | xxx
   2 | b    |     | 
   3 | c    | 3   | yyy
     |      | 5   | zzz

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,num,wert}, 'Full Joins (using ON condition) - columns ok' );
for my $res (
    q{'1','a','1','xxx'},
    q{'2','b',,},
    q{'3','c','3','yyy'},
    q{,,'5','zzz'},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Full Joins (using ON condition) - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num AND t2.wert = 'xxx'");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    | 1   | xxx
   2 | b    |     | 
   3 | c    |     | 

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,num,wert}, 'Left Joins (using ON t1.num = t2.num AND t2.wert = "xxx") - columns ok' );
for my $res (
    q{'1','a','1','xxx'},
    q{'2','b',,},
    q{'3','c',,},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Left Joins (using ON t1.num = t2.num AND t2.wert = "xxx") - values ok' );
}

$sth = $dbh->prepare( "SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num WHERE (t2.wert = 'xxx' OR t2.wert IS NULL)");
$sth->execute();

=pod

Results:

 num | name | num | wert
-----+------+-----+------
   1 | a    | 1   | xxx
   2 | b    |     | 
   3 | c    |     | 

=cut

$names = join(',',@{$sth->{NAME}});
cmp_ok( $names, 'eq', q{num,name,num,wert}, 'Left Joins (using ON t1.num = t2.num WHERE (t2.wert = "xxx" OR t2.wert IS NULL)) - columns ok' );
for my $res (
    q{'1','a','1','xxx'},
    q{'2','b',,},
    q{'3','c',,},
)
{
    my $values = sprintf( q{%s}, join( q{,}, map { defined($_) ? "'" . $_ . "'" : '' } $sth->fetchrow_array() ) );
    cmp_ok( $values, 'eq', $res, 'Left Joins (using ON t1.num = t2.num WHERE (t2.wert = "xxx" OR t2.wert IS NULL)) - values ok' );
}

__DATA__
CREATE TEMP TABLE t1    (num INT, name TEXT);
CREATE TEMP TABLE t2    (num INT, wert TEXT);
INSERT INTO t1 VALUES   (1,'a');
INSERT INTO t1 VALUES   (2,'b');
INSERT INTO t1 VALUES   (3,'c');
INSERT INTO t2 VALUES   (1,'xxx');
INSERT INTO t2 VALUES   (3,'yyy');
INSERT INTO t2 VALUES   (5,'zzz')
