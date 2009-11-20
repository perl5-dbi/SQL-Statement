#!/usr/bin/perl -w

use strict;
use SQL::Statement; # apt-get install libsql-statement-perl

$_="SELECT m.word_id FROM phpbb_search_wordmatch m, phpbb_search_wordlist w WHERE w.word_text
 IN ( 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S')
 AND m.word_id = w.word_id";

#create a parser object
my $parser = SQL::Parser->new('AnyData' , {RaiseError=>0} );
my $stmt = eval { SQL::Statement->new($_, $parser) };
