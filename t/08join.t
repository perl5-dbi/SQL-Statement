#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
#use lib  qw( ../lib );
eval {
    require DBI;
    require DBI::DBD::SqlEngine;
    require DBD::File;
};
if ($@ or $DBI::DBD::SqlEngine::VERSION lt '0.01') {
        plan skip_all => "Requires DBI > 1.611, DBD::File >= 0.39 and DBI::DBD::SqlEngine >= 0.01";
}
else {
    plan tests => 14;
}
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use vars qw($dbh $sth $DEBUG);
$DEBUG = 0;
$dbh = DBI->connect('dbi:File(PrintError=1):');
$dbh->do($_) for <DATA>;

queryresult_is(
  "SELECT pname,sname FROM Prof NATURAL JOIN Subject",
  'Sue~Chem^Bob~Bio^Bob~Math^',
  'NATURAL JOIN - with named columns in select list'
);

queryresult_is("SELECT * FROM Prof NATURAL JOIN Subject",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^',
 'NATURAL JOIN - with select list = *'
);

queryresult_is("SELECT UPPER(pname) AS P,Prof.pid,pname,sname FROM Prof NATURAL JOIN Subject",
 'SUE~1~Sue~Chem^BOB~2~Bob~Bio^BOB~2~Bob~Math^',
    'NATURAL JOIN - with computed columns'
);

queryresult_is(
 "SELECT UPPER(pname) AS P,pid,pname,sname FROM Prof NATURAL JOIN Subject",
 'SUE~1~Sue~Chem^BOB~2~Bob~Bio^BOB~2~Bob~Math^',
 'NATURAL JOIN - with no specifier on join column'
);

queryresult_is(
 "SELECT UPPER(pname) AS P,pid,pname,sname FROM Prof JOIN Subject using (pid)",
 'SUE~1~Sue~Chem^BOB~2~Bob~Bio^BOB~2~Bob~Math^',
 'INNER JOIN - with no specifier on join column'
);

queryresult_is("SELECT * FROM Prof LEFT JOIN Subject USING(pid)",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^3~Tom~undef^',
 'LEFT JOIN'
);

queryresult_is("SELECT pid,pname,sname FROM Prof LEFT JOIN Subject USING(pid)",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^3~Tom~undef^',
 'LEFT JOIN - enumerated columns'
);

queryresult_is("SELECT subject.pid,pname,sname FROM Prof LEFT JOIN Subject USING(pid)",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^undef~Tom~undef^',
 'LEFT JOIN - perversely intentionally mis-enumerated columns'
);

queryresult_is("select subject.pid,pname,sname from prof left join subject using(pid)",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^undef~Tom~undef^',
 'LEFT JOIN - lower case keywords'
);

queryresult_is("SELECT * FROM Prof RIGHT JOIN Subject USING(pid)",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^undef~undef~English^',
 'RIGHT JOIN'
);

queryresult_is("SELECT pid,sname,pname FROM Prof RIGHT JOIN Subject USING(pid)",
 '1~Chem~Sue^2~Bio~Bob^2~Math~Bob^undef~English~undef^',
 'RIGHT JOIN - enumerated columns'
);

queryresult_is("SELECT * FROM Prof FULL JOIN Subject USING(pid)",
 '1~Sue~Chem^2~Bob~Bio^2~Bob~Math^3~Tom~undef^4~undef~English^',
 'FULL JOIN'
);

queryresult_is("
    SELECT * FROM Prof AS P,Subject AS S WHERE P.pid=S.pid
",
 '1~Sue~1~Chem^2~Bob~2~Bio^2~Bob~2~Math^',
    'IMPLICIT JOIN - two tables'
);

queryresult_is("
    SELECT *
      FROM Prof AS P,Subject AS S,Room AS R
     WHERE P.pid=S.pid
       AND P.pid=R.pid
",
  '1~Sue~1~Chem~1~1C^2~Bob~2~Bio~2~2B^2~Bob~2~Math~2~2B^',
   'IMPLICIT JOIN - three tables'
);

sub queryresult_is {
    my ($query,$expected,$desc) = @_;
    my $sth = $dbh->prepare($query);
    my $result = query2str($sth);
    is($result,$expected,$desc);
}

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
