#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More tests => 18;
use lib  qw( ../lib );
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use DBI;
my $sth;
my $dbh = DBI->connect('dbi:File(RaiseError=1):');

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
    ok($sth->execute($_),$sth->{f_stmt}->command);
}
$sth = $dbh->prepare("SELECT UPPER('a') AS A,phrase FROM phrase");
$sth->execute;
my $str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq 'A FOO^A BAR^','SELECT');
ok(2==$dbh->selectrow_array("SELECT COUNT(*) FROM phrase"),'COUNT *');

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
    [qw( 1  Hacker  )],
    [qw( 2  Perl    )],
    [qw( 3  Another )],
    [qw( 4  Just    )] ];

$sth->execute($AoA);
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq 'Just^Another^Perl^Hacker^','IMPORT($AoA)');

#######################
# IMPORT($internal_sth)
#######################
$dbh->do($_) for split /\n/,<<"";
        CREATE TEMP TABLE xb (id INTEGER, xphrase VARCHAR(30))
        INSERT INTO xb VALUES(1,'foo')

my $xb_sth = $dbh->prepare('SELECT * FROM xb');
$xb_sth->execute;
$sth=$dbh->prepare('SELECT * FROM IMPORT(?)');
$sth->execute($xb_sth);
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq '1 foo^','IMPORT($internal_sth)');

#######################
# CREATE ... AS SELECT
#######################
$dbh->do("CREATE TEMP TABLE tbl_copy AS SELECT * FROM phrase");
$sth = $dbh->prepare("SELECT * FROM tbl_copy");
$sth->execute;
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq '1 FOO^3 BAR^','CREATE ... AS SELECT');

#######################
# CREATE ... AS IMPORT
#######################
$dbh->do("CREATE TEMP TABLE tbl_copy2 AS IMPORT(?)",{},$AoA);
$sth = $dbh->prepare("SELECT * FROM tbl_copy");
$sth->execute;
$str = '';
while (my $r=$sth->fetch) { $str.="@$r^"; }
ok($str eq '1 FOO^3 BAR^','CREATE ... AS IMPORT');

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
ok( $dbh->do("DROP TABLE tbl_copy"), 'DROP TEMP TABLE');
__END__
