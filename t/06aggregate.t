#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
use vars qw($DEBUG);
eval {
    require DBI;
    require DBI::DBD::SqlEngine;
    require DBD::File;
};

if ($@ or $DBI::DBD::SqlEngine::VERSION lt '0.01') {
        plan skip_all => "Requires DBI > 1.611, DBD::File >= 0.39 and DBI::DBD::SqlEngine >= 0.01";
}
else {
    plan tests => 15;
}

use SQL::Statement;
diag( sprintf( "SQL::Statement v%s\n", $SQL::Statement::VERSION ) );
diag( sprintf( "DBI v%s\n", $DBI::VERSION ) );
diag( sprintf( "DBD::File v%s\n", $DBD::File::VERSION ) );

my ($dbh, $sth);

$dbh=DBI->connect('dbi:File(RaiseError=1,PrintError=0):');
$dbh->do($_) for <DATA>;

$sth=$dbh->prepare("SELECT class,SUM(sales) as foo, MAX(sales) FROM biz GROUP BY class");
cmp_ok(query2str($sth), 'eq', 'Car~2000~1000^Truck~700~400','GROUP BY one column');

$sth=$dbh->prepare("SELECT color,class,SUM(sales), MAX(sales) FROM biz GROUP BY color,class");
cmp_ok(query2str($sth), 'eq', 'Blue~Car~500~500^Red~Car~500~500^White~Car~1000~1000^White~Truck~700~400',
       'GROUP BY several columns');

$sth=$dbh->prepare("SELECT SUM(sales), MAX(sales) FROM biz");
cmp_ok(query2str($sth), 'eq', '2700~1000','AGGREGATE FUNCTIONS WITHOUT GROUP BY');

$sth = $dbh->prepare( "SELECT distinct class, COUNT(distinct color) FROM biz GROUP BY class" );
cmp_ok( query2str($sth), 'eq', 'Car~3^Truck~1', 'COUNT(distinct column) WITH GROUP BY' );

$sth = $dbh->prepare( "SELECT class, COUNT(*) FROM biz GROUP BY class" );
cmp_ok( query2str($sth), 'eq', 'Car~3^Truck~2', 'COUNT(*) with GROUP BY' );

eval { $sth = $dbh->prepare( "SELECT class, COUNT(distinct *) FROM biz GROUP BY class" ); };
like( $@, qr/Keyword DISTINCT is not allowed for COUNT/m, 'COUNT(DISTINCT *) fails' );

eval {
    $sth = $dbh->prepare( "SELECT class, COUNT(color) FROM biz" );
    $sth->execute();
};
like( $@, qr/Column 'biz\.class' must appear in the GROUP BY clause or be used in an aggregate function/, 'GROUP BY required' );

$sth = $dbh->prepare("SELECT SUM(bar) FROM numbers");
cmp_ok( query2str($sth), 'eq', 'undef', 'SUM(bar) of empty table' );

$sth = $dbh->prepare("SELECT COUNT(bar),c_foo FROM numbers GROUP BY c_foo");
cmp_ok( query2str($sth), 'eq', '0~undef', 'COUNT(bar) of empty table with GROUP BY' );

$sth = $dbh->prepare("SELECT COUNT(*) FROM numbers");
cmp_ok( query2str($sth), 'eq', '0', 'COUNT(*) of empty table' );

my $sql_stmt = "INSERT INTO numbers VALUES (?, ?, ?)";
my $stmt = $dbh->prepare($sql_stmt);
for my $num ( 1 .. 3999 )
{
    my @params = ( $num, ( "a" .. "f" )[ int rand 6 ], int rand 10 );
    $stmt->execute(@params);
}

$sth = $dbh->prepare( "SELECT foo AS boo, COUNT (*) AS counted FROM numbers GROUP BY boo" );
$sth->execute();
cmp_ok( join( '^', @{$sth->{NAME_lc}} ), 'eq', 'boo^counted', 'Names in aggregated Table' );
my $res = $sth->fetchall_arrayref();
cmp_ok( scalar( @{$res} ), '==', '6', 'Number of rows in aggregated Table' );
my $all_counted = 0;
foreach my $row (@{$res})
{
    $all_counted += $row->[1];
}
cmp_ok( $all_counted, '==', 3999, 'SUM(COUNTED)' );

$sth = $dbh->prepare( "SELECT MIN(c_foo), MAX(c_foo), AVG(c_foo) FROM numbers" );
cmp_ok( query2str($sth), 'eq', '1~3999~2000', 'Aggregate functions');

$sth=$dbh->prepare("SELECT COUNT(*) FROM trick");
cmp_ok(query2str($sth), 'eq', '2','Nasty COUNT(*)');

sub query2str {
    my($sth)=@_;
    $sth->execute();
    my @rows;
    while (my $r=$sth->fetch()) {
        push( @rows, join( '~', map { defined $_ ? $_ : 'undef' } @{$r} ) );
    }
    my $str = join( "^", sort @rows );
    return $str unless $DEBUG;
    printf "%s\n",join',',@{$sth->{NAME}};
    print "<$str>\n";
    return $str;
}
__END__
CREATE TEMP TABLE biz (class TEXT, color TEXT, sales INTEGER, BUGNULL TEXT)
INSERT INTO biz VALUES ('Car',   'White', 1000, NULL)
INSERT INTO biz VALUES ('Car',   'Blue',   500, NULL )
INSERT INTO biz VALUES ('Truck', 'White',  400, NULL )
INSERT INTO biz VALUES ('Car',   'Red',    500, NULL )
INSERT INTO biz VALUES ('Truck', 'White',  300, NULL )
CREATE TEMP TABLE numbers (c_foo INTEGER, foo TEXT, bar INTEGER)
CREATE TEMP TABLE trick   (id INTEGER, foo TEXT)
INSERT INTO trick VALUES (1, '1foo')
INSERT INTO trick VALUES (11, 'foo')
