#!/usr/bin/perl -w
$|=1;
use strict;
use lib qw' ./ ./t ';
use SQLtest;
use Test::More;
eval {
    require DBI;
    require DBI::DBD::SqlEngine;
    require DBD::File;
};
if ($@ or $DBI::DBD::SqlEngine::VERSION lt '0.01') {
        plan skip_all => "Requires DBI > 1.611, DBD::File >= 0.39 and DBI::DBD::SqlEngine >= 0.01";
}
else {
    plan tests => 26;
}
my($sth,$str);
my $dbh = DBI->connect('dbi:File(RaiseError=1):');

$dbh->do(q{ CREATE TEMP TABLE Tmp (id INT,phrase VARCHAR(30)) } );
ok($dbh->do(q{ INSERT INTO Tmp (id,phrase) VALUES (?,?) },{},9,'yyy'),'placeholder insert with named cols');
ok($dbh->do(q{ INSERT INTO Tmp VALUES(?,?) },{},2,'zzz'),'placeholder insert without named cols');
$dbh->do(q{ INSERT INTO Tmp (id,phrase) VALUES (?,?) },{},3,'baz');
ok($dbh->do(q{ DELETE FROM Tmp WHERE id=? or phrase=? },{},3,'baz'),'placeholder delete');
ok($dbh->do(q{ UPDATE Tmp SET phrase=? WHERE id=?},{},'bar',2),'placeholder update');
ok($dbh->do(q{ UPDATE Tmp SET phrase=?,id=? WHERE id=? and phrase=?},{},'foo',1,9,'yyy'),'placeholder update');
ok( $dbh->do( q{INSERT INTO Tmp VALUES (3, 'baz'), (4, 'fob'),
(5, 'zab')} ), 'multiline insert' );
$sth = $dbh->prepare('SELECT id,phrase FROM Tmp ORDER BY id');
$sth->execute();
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
cmp_ok($str, 'eq', '1 foo^2 bar^3 baz^4 fob^5 zab^','verify table contents');
ok( $dbh->do(q{ DROP TABLE IF EXISTS Tmp } ), 'DROP TABLE' );


########################################
# CREATE, INSERT, UPDATE, DELETE, SELECT
########################################
for (split /\n/,
  q{  CREATE TEMP TABLE phrase (id INT,phrase VARCHAR(30))
      INSERT INTO phrase VALUES(1,UPPER(TRIM(' foo ')))
      INSERT INTO phrase VALUES(2,'baz')
      INSERT INTO phrase VALUES(3,'qux')
      UPDATE phrase SET phrase=UPPER(TRIM(LEADING 'z' FROM 'zbar')) WHERE id=3
      DELETE FROM phrase WHERE id = 2                   }
){
    $sth = $dbh->prepare($_);
    ok($sth->execute(),$sth->{sql_stmt}->command);
}

$sth = $dbh->prepare("SELECT UPPER('a') AS A,phrase FROM phrase");
$sth->execute;
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq 'A FOO^A BAR^','SELECT');
cmp_ok(scalar $dbh->selectrow_array("SELECT COUNT(*) FROM phrase"),'==', 2, 'COUNT *');

#################################
# COMPUTED COLUMNS IN SELECT LIST
#################################
ok('B' eq $dbh->selectrow_array("SELECT UPPER('b')"),'COMPUTED COLUMNS IN SELECT LIST');

###########################
# CREATE function in script
###########################
$dbh->do("CREATE FUNCTION froog");
sub froog { 99 }
ok('99'eq $dbh->selectrow_array("SELECT froog"),'CREATE FUNCTION from script');


###########################
# CREATE function in module
###########################
BEGIN {
   eval "package Foo; sub foo { 88 } 1;"
}
$dbh->do(q{CREATE FUNCTION foo NAME "Foo::foo"});
ok(88 == $dbh->selectrow_array("SELECT foo"), 'CREATE FUNCTION from module');

################
# LOAD functions
################
unlink 'Bar.pm' if -e 'Bar.pm';
open(O,'>Bar.pm') or die $!;
print O "package Bar; sub SQL_FUNCTION_BAR{77};1;";
close O;
$dbh->do("LOAD Bar");
ok(77 == $dbh->selectrow_array("SELECT bar"), 'LOAD FUNCTIONS');
unlink 'Bar.pm' if -e 'Bar.pm';

####################
# IMPORT($AoA)
####################
$sth = $dbh->prepare("SELECT word FROM IMPORT(?) ORDER BY id DESC");
my $AoA=  [ [qw( id word    )],
    [qw( 4  Just    )],
    [qw( 3  Another )],
    [qw( 2  Perl    )],
    [qw( 1  Hacker  )],
];

$sth->execute($AoA);
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq 'Just^Another^Perl^Hacker^','IMPORT($AoA)');

#######################
# IMPORT($internal_sth)
#######################
$dbh->do($_) for split /\n/,<<"";
        CREATE TEMP TABLE tmp (id INTEGER, xphrase VARCHAR(30))
        INSERT INTO tmp VALUES(1,'foo')

my $internal_sth = $dbh->prepare('SELECT * FROM tmp');
$internal_sth->execute;
$sth=$dbh->prepare('SELECT * FROM IMPORT(?)');
$sth->execute($internal_sth);
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq '1 foo^','IMPORT($internal_sth)');

#######################
# IMPORT($external_sth)
#######################
eval { require DBD::XBase };
SKIP: {
   skip('No XBase installed',1) if $@;
   ok(external_sth(),'IMPORT($external_sth)');
};

sub external_sth {
    my $xb_dbh = DBI->connect('dbi:XBase:./');
    unlink 'xb' if -e 'xb';
    $xb_dbh->do($_) for split /\n/,<<"";
        CREATE TABLE xb (id INTEGER, xphrase VARCHAR(30))
        INSERT INTO xb VALUES(1,'foo')

    my $xb_sth = $xb_dbh->prepare('SELECT * FROM xb');
    $xb_sth->execute;
    $sth=$dbh->prepare('SELECT * FROM IMPORT(?)');
    $sth->execute($xb_sth);
    $str = '';
    while (my $r=$sth->fetch) { $str.="@$r^"; }
    $xb_dbh->do("DROP TABLE xb");
    return ($str eq '1 foo^');
}

#my $foo=0;
#sub test2 {$foo = 6;}
#open(O,'>','tmpss.sql') or die $!;
#print O "SELECT test2";
#close O;
#$dbh->do("CREATE FUNCTION test2");
#ok($dbh->do(q{CALL RUN('tmpss.sql')}),'run');
#ok(6==$foo,'call run');
#unlink 'tmpss.sql' if -e 'tmpss.sql';

ok( $dbh->do("DROP TABLE phrase"), 'DROP TEMP TABLE');

my $pauli = [
    [ 'H', 19 ],
    [ 'H', 21 ],
    [ 'KK', 1 ],
    [ 'KK', 2 ],
    [ 'KK', 13 ],
    [ 'MMM', 25 ],
];
$dbh->do( q{CREATE TEMP TABLE pauli (column1 TEXT, column2 INTEGER)} );
foreach my $line (@{$pauli})
{
    $dbh->do( sprintf( "INSERT INTO pauli VALUES ('%s', %d)", @{$line} ) );
}
$sth = $dbh->prepare ("UPDATE pauli SET column1 = ? WHERE column1 = ?");
my $cnt = $sth->execute ("XXXX", "KK");
cmp_ok( $cnt, '==', 3, 'UPDATE with placeholders' );
$sth->finish();

$sth = $dbh->prepare( "SELECT column1, COUNT(column1) FROM pauli GROUP BY column1" );
$sth->execute();
my $hres = $sth->fetchall_hashref('column1');
cmp_ok( $hres->{XXXX}->{'COUNT'}, '==', 3, 'UPDATE with placeholder updates correct' );
__END__
