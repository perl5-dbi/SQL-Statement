#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
#use lib  qw( ../lib );
eval {require DBI; require DBD::File; require IO::File;};
if ($@) {
    plan skip_all => "DBI or DBD::File not available";
}
elsif ($DBD::File::VERSION < '0.033' ) {
    plan skip_all => "Tests require DBD::File => 0.33";
}
else {
    plan tests => 5;
}
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use vars qw($dbh $sth $DEBUG);
$DEBUG = 0;
$dbh = DBI->connect('dbi:File(RaiseError=1):');
$dbh->do($_) for <DATA>;

$sth = $dbh->prepare("SELECT * FROM Prof");
is( (join' ',cols($sth)),'PID PNAME','Column Names: select list = *');

$sth = $dbh->prepare("SELECT pname,pID FROM Prof");
is( (join' ',cols($sth)), 'pname pID' ,'Column Names: select list = named');

$sth = $dbh->prepare('SELECT pname AS "ProfName", pId AS "Magic#" from prof');
is( (join' ',cols($sth)), '"ProfName" "Magic#"' ,'Column Names: select list = aliased');

$sth = $dbh->prepare(q{SELECT pid, concat(pname, ' is #', pId ) from prof});
is( (join' ',cols($sth)), 'pid CONCAT' ,'Column Names: select list with function');

$sth = $dbh->prepare(q{SELECT pid AS "ID", concat(pname, ' is #', pId ) AS "explanation"  from prof});
is( (join' ',cols($sth)), '"ID" "explanation"' ,'Column Names: select list with function = aliased');

sub cols {
    my($sth)=@_;
    $sth->execute;
    my $str='';
    while (my $r=$sth->fetch) {
        $str .= sprintf "%s^",join('~',map { defined $_ ? $_ : 'undef' } @$r);
    }
    return @{$sth->{NAME}};
}
__END__
CREATE TEMP TABLE Prof (pid INT, pname VARCHAR(30))
INSERT INTO Prof VALUES (1,'Sue')
INSERT INTO Prof VALUES (2,'Bob')
INSERT INTO Prof VALUES (3,'Tom')
