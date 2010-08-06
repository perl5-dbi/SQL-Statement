#!/usr/bin/perl

use strict;
use warnings;
use Test::More;

use DBI;
use SQL::Statement;

my $dbh = DBI->connect ("dbi:File:", undef, undef, { PrintError => 1 });

ok ($dbh->do ("create temp table duck (id integer, name varchar (10))"), "create");

{   ok (my $sth = $dbh->prepare ("select count (*) from duck"), "prepare count");

    sub count
    {
	my $expected_count = shift;
	ok ($sth->execute, "execute count");
	ok (my $r = $sth->fetch, "fetch count");
	is ($r->[0], $expected_count, "count = $expected_count");
	} # count
    }

count (0);
ok ($dbh->do ("insert into duck (id, name) values (1, 'donald')"), "insert donald");
count (1);
ok ($dbh->do ("insert into duck (id, name) values (2, 'kwik')"), "insert kwik");
count (2);
ok ($dbh->do ("insert into duck (id, name) values (3, 'kwek')"), "insert kwek");
count (3);
ok ($dbh->do ("insert into duck (id, name) values (4, 'kwak')"), "insert kwak");
count (4);

ok ($dbh->do ("delete from duck where id = 2"), "delete kwik");
# is ($dbh->rows, 1, "deleted rows"); # Not supported by base class DBD::File
count (3);

ok ($dbh->do ("delete from duck"), "delete all ducks");
# is ($dbh->rows, 3, "deleted rows"); # Not supported by base class DBD::File
count (0);

done_testing ();
