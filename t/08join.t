#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
use lib  qw( ../lib );
eval { require DBI; require DBD::File; require IO::File };
if ($@) {
        plan skip_all => "No DBD::File available";
}
else {
    plan tests => 8;
}
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use vars qw($dbh $sth $DEBUG);
$DEBUG = 0;
$dbh = DBI->connect('dbi:File(RaiseError=1):');
$dbh->do($_) for <DATA>;

$sth = $dbh->prepare("SELECT pname,sname FROM Prof NATURAL JOIN Subject");
ok( 'Sue~Chem^Bob~Bio^Bob~Math^'
 eq query2str($sth),'NATURAL JOIN - with named columns in select list');


$sth = $dbh->prepare("SELECT * FROM Prof NATURAL JOIN Subject");
ok( '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^'
 eq query2str($sth),'NATURAL JOIN - with select list = *');

$sth = $dbh->prepare("
    SELECT UPPER(pname)AS P,Prof.pid,pname,sname FROM Prof NATURAL JOIN Subject
");
ok( 'SUE~1~Sue~Chem^BOB~2~Bob~Bio^BOB~2~Bob~Math^'
 eq query2str($sth),'NATURAL JOIN - with computed columns');

$sth = $dbh->prepare("SELECT * FROM Prof LEFT JOIN Subject USING(pid)");
ok( '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^3~Tom~undef^'
 eq query2str($sth),'LEFT JOIN');

$sth = $dbh->prepare("SELECT * FROM Prof RIGHT JOIN Subject USING(pid)");
ok( '1~Chem~Sue^2~Bio~Bob^2~Math~Bob^4~English~undef^'
 eq query2str($sth),'RIGHT JOIN');

$sth = $dbh->prepare("SELECT * FROM Prof FULL JOIN Subject USING(pid)");
ok( '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^3~Tom~undef^4~undef~English^'
 eq query2str($sth),'FULL JOIN');

$sth = $dbh->prepare("
    SELECT * FROM Prof AS P,Subject AS S WHERE P.pid=S.pid
");
ok( '1~Sue~1~Chem^2~Bob~2~Bio^2~Bob~2~Math^'
 eq query2str($sth),'IMPLICIT JOIN - two tables');

$sth = $dbh->prepare("
    SELECT *
      FROM Prof AS P,Subject AS S,Room AS R
     WHERE P.pid=S.pid
       AND P.pid=R.pid
");
ok( '1~Sue~1~Chem~1~1C^2~Bob~2~Bio~2~2B^2~Bob~2~Math~2~2B^'
 eq query2str($sth),'IMPLICIT JOIN - three tables');

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
CREATE TEMP TABLE Prof (pid INT, pname VARCHAR(30))
INSERT INTO Prof VALUES (1,'Sue')
INSERT INTO Prof VALUES (2,'Bob')
INSERT INTO Prof VALUES (3,'Tom')
CREATE TEMP TABLE Subject (pid INT, sname VARCHAR(30))
INSERT INTO Subject VALUES (1,'Chem')
INSERT INTO Subject VALUES (2,'Bio')
INSERT INTO Subject VALUES (2,'Math')
INSERT INTO Subject VALUES (4,'English')
CREATE TEMP TABLE Room (pid INT, rname VARCHAR(30))
INSERT INTO Room VALUES (1,'1C')
INSERT INTO Room VALUES (2,'2B')
