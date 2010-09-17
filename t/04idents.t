#!/usr/bin/perl -w
use strict;
use warnings;
use lib qw(t);

use Test::More;
use TestLib qw(connect prove_reqs show_reqs test_dir);

my ( $required, $recommended ) = prove_reqs();
show_reqs( $required, $recommended );
my @test_dbds = ( 'SQL::Statement', grep { /^dbd:/i } keys %{$recommended} );
my $testdir = test_dir();

foreach my $test_dbd (@test_dbds)
{
    my ( $dbh, $sth );
    diag("Running tests for $test_dbd");
    my $temp = "";
    # XXX
    # my $test_dbd_tbl = "${test_dbd}::Table";
    # $test_dbd_tbl->can("fetch") or $temp = "$temp";
    $test_dbd eq "DBD::File"      and $temp = "TEMP";
    $test_dbd eq "SQL::Statement" and $temp = "TEMP";

    $dbh = connect(
                    $test_dbd,
                    {
                       PrintError => 0,
                       RaiseError => 0,
                       f_dir      => $testdir,
                    }
                  );

    #######################
    # identifier names
    #######################
    $dbh->do($_) for split /\n/, <<"";
	CREATE TEMP TABLE Prof (pid INT, pname VARCHAR(30))
	INSERT INTO Prof VALUES (1,'Sue')
	INSERT INTO Prof VALUES (2,'Bob')
	INSERT INTO Prof VALUES (3,'Tom')

    $sth = $dbh->prepare("SELECT * FROM Prof");
    $sth->execute();
    is_deeply( $sth->col_names(), [qw(pid pname)], "Column Names: select list = *" );

    $sth = $dbh->prepare("SELECT pname,pID FROM Prof");
    $sth->execute();
    is_deeply( $sth->col_names(), [qw(pname pID)], 'Column Names: select list = named' );

    $sth = $dbh->prepare('SELECT pname AS "ProfName", pId AS "Magic#" from prof');
    $sth->execute();
    no warnings;
    is_deeply( $sth->col_names(), [qw("ProfName" "Magic#")],
               "Column Names: select list = aliased" );
    use warnings;

    $sth = $dbh->prepare(q{SELECT pid, concat(pname, ' is #', pId ) from prof});
    $sth->execute();
    is_deeply( $sth->col_names(), [qw(pid concat)], "Column Names: select list with function" );

    $sth = $dbh->prepare(
                   q{SELECT pid AS "ID", concat(pname, ' is #', pId ) AS "explanation"  from prof});
    $sth->execute();
    is_deeply( $sth->col_names(), [qw("ID" "explanation")],
               "Column Names: select list with function = aliased" );

    my @rt34121_checks = (
        {
           descr => 'camelcased',
           cols  => [qw("fOo")],
           tbls  => [qw("SomeTable")]
        },
        {
           descr => 'reserved names',
           cols  => [qw("text")],
           tbls  => [qw("Table")]
        },
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
          descr => 'not quoted',
          cols  => [qw(Foo)],
          tbls  => [qw(SomeTable)],
          icols => [qw(foo)],
          itbls => [qw(sometable)],    # none quoted identifiers are lowercased internally
        },
    );
    for my $check (@rt34121_checks)
    {
        $sth = $dbh->prepare(
                              sprintf(
                                       q{SELECT %s FROM %s},
                                       join( ", ", @{ $check->{cols} } ),
                                       join( ", ", @{ $check->{tbls} } )
                                     )
                            );
        is_deeply( $sth->col_names(),
                  $check->{icols} || $check->{cols},
                  "Raw SQL hidden absent from column name [rt.cpan.org #34121] ($check->{descr})" );
        is_deeply( $sth->tbl_names(),
                   $check->{itbls} || $check->{tbls},
                   "Raw SQL hidden absent from table name [rt.cpan.org #34121] ($check->{descr})" );
    }

    $dbh->do("CREATE $temp TABLE allcols ( f1 char(10), f2 char(10) )");
    $sth = $dbh->prepare("INSERT INTO allcols (f1,f2) VALUES (?,?)") or diag("Can't prepare insert sth: " . $dbh->errstr());
    $sth->execute('abc', 'def');
    my $allcols_before = $sth->all_cols();
    $sth->execute('abc', 'def') for 1 .. 100;
    my $allcols_after = $sth->all_cols();
    is_deeply( $allcols_before, $allcols_after, '->{all_cols} structure does not grow beyond control');
}

done_testing();
