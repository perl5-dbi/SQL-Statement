#!/usr/bin/perl

use warnings;
use strict;

use Test::More tests => 11;

use Data::Dumper qw(Dumper);
use Scalar::Util qw(blessed);

BEGIN
{
    use_ok('SQL::Statement');    # Test 1
    use_ok('SQL::Parser');       # Test 2
}

diag("SQL::Statement version $SQL::Statement::VERSION");

sub columns_sig
{
    my (@columns) = @_;

    if ( blessed( $columns[0] ) && $columns[0]->isa('SQL::Statement') )
    {
        unshift( @columns, @{shift(@columns)->{column_names}} ); # columns() doesn't work before open_tables succeeds
    }

    @columns = sort( @columns );

    local $Data::Dumper::Useqq  = 1;
    local $Data::Dumper::Terse  = 1;
    local $Data::Dumper::Indent = 0;
    return Dumper( \@columns );
}

sub tables_sig
{
    my (@tables) = @_;

    if ( blessed( $tables[0] ) && $tables[0]->isa('SQL::Statement') )
    {
        unshift( @tables, map { $_->name() } shift(@tables)->tables() );
    }

    @tables = sort( @tables );

    local $Data::Dumper::Useqq  = 1;
    local $Data::Dumper::Terse  = 1;
    local $Data::Dumper::Indent = 0;
    return Dumper( \@tables );
}

my $parser = SQL::Parser->new(
                               'ANSI',
                               {
                                  RaiseError => 0,
                                  PrintError => 0
                               }
                             );

my $sql;
my $stmt;
my $got;
my $expect;

#
# [rt.cpan.org #34121]
# Raw SQL was leaking into column and table names and to users such as DBD::CSV.
#

{

    # Test 3
    $sql = q{SELECT "foo" FROM "SomeTable"};
    $stmt = SQL::Statement->new( $sql, $parser );
    ok( !defined( $parser->structure()->{errstr} ), "Parsing $sql" )
      or diag( "errstr: " . $parser->structure()->{errstr} );

  SKIP:
    {
        skip( "Parsing select statement fails", 2 )
          if( defined( $parser->structure()->{errstr} ) );

        # Test 4
        $got    = columns_sig($stmt);
        $expect = columns_sig('"foo"');
        cmp_ok( $got, 'eq', $expect, 'Raw SQL hidden absent from column name [rt.cpan.org #34121]' );

        # Test 5
        $got    = tables_sig($stmt);
        $expect = tables_sig('"SomeTable"');
             cmp_ok( $got, 'eq', $expect, 'Raw SQL hidden absent from table name [rt.cpan.org #34121]' )
    }
}

{

    # Test 6
    $sql = q{SELECT "text" FROM "Table"};
    $stmt = SQL::Statement->new( $sql, $parser );
    ok( !defined( $parser->structure()->{errstr} ), "Parsing $sql" )
      or diag( "errstr: " . $parser->structure()->{errstr} );

  SKIP:
    {
        skip( "Parsing select statement fails", 2 )
          if defined( $parser->structure()->{errstr} );

        # Test 7
        $got    = columns_sig($stmt);
        $expect = columns_sig('"text"');
             cmp_ok( $got, 'eq', $expect, 'Raw SQL hidden absent from column name [rt.cpan.org #34121]' );

        # Test 8
        $got    = tables_sig($stmt);
        $expect = tables_sig('"Table"');
             cmp_ok( $got, 'eq', $expect, 'Raw SQL hidden absent from table name [rt.cpan.org #34121]' );
    }
}

#
# According to jZed,
#
#     Verbatim from Martin Gruber and Joe Celko (who is on the standards committee
#     and whom I have talked to in person about this), _SQL Instant Reference_, Sybex
#
#         "A regular and a delimited identifier are equal if they contain the same
#         characters, taking case into account, but first converting the regular
#         (but not the delimited) identifier to all uppercase letters.  In effect
#         a delimited identifier that contains lowercase letters can never equal a
#         regular identifier although it may equal another delimited one."
# 

{

    # Test 9
    $sql = q{SELECT foo FROM SomeTable};
    $stmt = SQL::Statement->new( $sql, $parser );
    ok( !defined( $parser->structure()->{errstr} ), "Parsing $sql" )
      or diag( "errstr: " . $parser->structure()->{errstr} );

  SKIP:
    {
        skip( "Parsing select statement fails", 1 )
          if defined( $parser->structure()->{errstr} );

        # Test 10
        $got    = columns_sig($stmt);
        $expect = columns_sig('foo');
        cmp_ok( $got, 'eq', $expect, 'Lowercased unquoted column name' );

        # Test 11
        $got    = tables_sig($stmt);
        $expect = tables_sig('sometable');
        cmp_ok( $got, 'eq', $expect, 'Lowercased unquoted table name' );
    }
}
