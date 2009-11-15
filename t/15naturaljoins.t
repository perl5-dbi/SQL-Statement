#!/usr/bin/perl
use warnings;
use strict;
use Test::More tests => 2;

use DBI;

my $dbh  = DBI->connect('dbi:CSV:',,,{RaiseError=>1,PrintError=>0});
$dbh->do($_) for split(';',join('',<DATA>));
my $sth = $dbh->prepare("SELECT * FROM author NATURAL JOIN book");
$sth->execute();

=pod

Results:

author_name,author_id,book_title
'Neal Stephenson', '1', 'Cryptonomicon'
1 rows

=cut

my $names = join(',',@{$sth->{NAME}});
cmp_ok( q{author_name,author_id,book_title}, 'eq', $names, 'Natural Join - columns ok' );
my $values = sprintf( q{'%s'}, join( q{', '}, $sth->fetchrow_array() ) );
cmp_ok( q{'Neal Stephenson', '1', 'Cryptonomicon'}, 'eq', $values, 'Natural Join - values ok' );

__DATA__
CREATE TEMP TABLE book   (book_title TEXT, author_id INT);
CREATE TEMP TABLE author (author_name TEXT, author_id INT);
INSERT INTO author VALUES ('Neal Stephenson',1);
INSERT INTO author VALUES ('Vernor Vinge',2);
INSERT INTO book VALUES ('Cryptonomicon',1);
INSERT INTO book VALUES ('Dahlgren',3)
