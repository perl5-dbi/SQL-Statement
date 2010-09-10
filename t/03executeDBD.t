#!/usr/bin/perl -w
use strict;
use warnings;
use lib qw(t);

use Test::More;
use TestLib qw(connect prove_reqs show_reqs test_dir default_recommended);

my ( $required, $recommended ) = prove_reqs( { default_recommended(), ( MLDBM => 0 ) } );
my @test_dbds = ( grep { /^dbd:/i } keys %{$recommended} );
my $testdir = test_dir();

sub external_sth
{
    my $dbh    = $_[0];
    my $xb_dbh = DBI->connect('dbi:XBase:./');
    unlink 'xb' if -e 'xb';
    $xb_dbh->do($_) for split /\n/, <<"";
    CREATE TABLE xb (id INTEGER, xphrase VARCHAR(30))
    INSERT INTO xb VALUES(1,'foo')

    my $xb_sth = $xb_dbh->prepare('SELECT * FROM xb');
    $xb_sth->execute();
    my $sth = $dbh->prepare('SELECT * FROM IMPORT(?)');
    $sth->execute($xb_sth);
    my $str = '';
    while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
    $xb_dbh->do("DROP TABLE xb");
    return ( $str eq '1 foo^' );
}

foreach my $test_dbd (@test_dbds)
{
    my $dbh;
    diag("Running tests for $test_dbd");
    my $temp = "";
    # XXX
    # my $test_dbd_tbl = "${test_dbd}::Table";
    # $test_dbd_tbl->can("fetch") or $temp = "$temp";
    $test_dbd eq "DBD::File" and $temp = "TEMP";

    my %extra_args;
    if ( $test_dbd eq "DBD::DBM" and $recommended->{MLDBM} )
    {
        $extra_args{dbm_mldbm} = "Storable";
    }
    $dbh = connect(
                    $test_dbd,
                    {
                       PrintError => 0,
                       RaiseError => 0,
                       f_dir      => $testdir,
                       %extra_args,
                    }
                  );

    my ( $sth, $str );

    $dbh->do(qq{ CREATE $temp TABLE Tmp (id INT,phrase VARCHAR(30)) });
    ok( $dbh->do( qq{ INSERT INTO Tmp (id,phrase) VALUES (?,?) }, {}, 9, 'yyy' ),
        'placeholder insert with named cols' );
    ok( $dbh->do( qq{ INSERT INTO Tmp VALUES(?,?) }, {}, 2, 'zzz' ),
        'placeholder insert without named cols' );
    $dbh->do( qq{ INSERT INTO Tmp (id,phrase) VALUES (?,?) }, {}, 3, 'baz' );
    ok( $dbh->do( qq{ DELETE FROM Tmp WHERE id=? or phrase=? }, {}, 3, 'baz' ),
        'placeholder delete' );
    ok( $dbh->do( qq{ UPDATE Tmp SET phrase=? WHERE id=?}, {}, 'bar', 2 ), 'placeholder update' );
    ok(
        $dbh->do(
                  qq{ UPDATE Tmp SET phrase=?,id=? WHERE id=? and phrase=?},
                  {}, 'foo', 1, 9, 'yyy'
                ),
        'placeholder update'
      );
    ok(
        $dbh->do(
            qq{INSERT INTO Tmp VALUES (3, 'baz'), (4, 'fob'),
(5, 'zab')}
                ),
        'multiline insert'
      );
    $sth = $dbh->prepare('SELECT id,phrase FROM Tmp ORDER BY id');
    $sth->execute();
    $str = '';
    while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
    cmp_ok( $str, 'eq', '1 foo^2 bar^3 baz^4 fob^5 zab^', 'verify table contents' );
    ok( $dbh->do(qq{ DROP TABLE IF EXISTS Tmp }), 'DROP TABLE' );

########################################
    # CREATE, INSERT, UPDATE, DELETE, SELECT
########################################
    for (
        split /\n/,
        qq{  CREATE $temp TABLE phrase (id INT,phrase VARCHAR(30))
      INSERT INTO phrase VALUES(1,UPPER(TRIM(' foo ')))
      INSERT INTO phrase VALUES(2,'baz')
      INSERT INTO phrase VALUES(3,'qux')
      UPDATE phrase SET phrase=UPPER(TRIM(LEADING 'z' FROM 'zbar')) WHERE id=3
      DELETE FROM phrase WHERE id = 2                   }
        )
    {
        $sth = $dbh->prepare($_);
        ok( $sth->execute(), $sth->{sth}->{sql_stmt}->command );
    }

    $sth = $dbh->prepare("SELECT UPPER('a') AS A,phrase FROM phrase");
    $sth->execute;
    $str = '';
    while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
    ok( $str eq 'A FOO^A BAR^', 'SELECT' );
    cmp_ok( scalar $dbh->selectrow_array("SELECT COUNT(*) FROM phrase"), '==', 2, 'COUNT *' );

#################################
    # COMPUTED COLUMNS IN SELECT LIST
#################################
    cmp_ok( $dbh->selectrow_array("SELECT UPPER('b')"),
            'eq', 'B', 'COMPUTED COLUMNS IN SELECT LIST' );

###########################
    # CREATE function in script
###########################
    $dbh->do("CREATE FUNCTION froog");
    sub froog { 99 }
    ok( '99' eq $dbh->selectrow_array("SELECT froog"), 'CREATE FUNCTION from script' );

###########################
    # CREATE function in module
###########################
    BEGIN
    {
        eval "package Foo; sub foo { 88 } 1;";
    }
    $dbh->do(qq{CREATE FUNCTION foo NAME "Foo::foo"});
    ok( 88 == $dbh->selectrow_array("SELECT foo"), 'CREATE FUNCTION from module' );

################
    # LOAD functions
################
    unlink 'Bar.pm' if -e 'Bar.pm';
    open( O, '>Bar.pm' ) or die $!;
    print O "package Bar; sub SQL_FUNCTION_BAR{77};1;";
    close O;
    $dbh->do("LOAD Bar");
    ok( 77 == $dbh->selectrow_array("SELECT bar"), 'LOAD FUNCTIONS' );
    unlink 'Bar.pm' if -e 'Bar.pm';

####################
    # IMPORT($AoA)
####################
    $sth = $dbh->prepare("SELECT word FROM IMPORT(?) ORDER BY id DESC");
    my $AoA = [
                [qw( id word    )], [qw( 4  Just    )], [qw( 3  Another )], [qw( 2  Perl    )],
                [qw( 1  Hacker  )],
              ];

    $sth->execute($AoA);
    $str = '';
    while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
    ok( $str eq 'Just^Another^Perl^Hacker^', 'IMPORT($AoA)' );

#######################
    # IMPORT($internal_sth)
#######################
    $dbh->do($_) for split /\n/, <<"";
        CREATE $temp TABLE tmp (id INTEGER, xphrase VARCHAR(30))
        INSERT INTO tmp VALUES(1,'foo')

    my $internal_sth = $dbh->prepare('SELECT * FROM tmp')->{sth};    # XXX breaks abstraction
    $internal_sth->execute();
    $sth = $dbh->prepare('SELECT * FROM IMPORT(?)');
    $sth->execute($internal_sth);
    $str = '';
    while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
    cmp_ok( $str, 'eq', '1 foo^', 'IMPORT($internal_sth)' );

#######################
    # IMPORT($external_sth)
#######################
    eval { require DBD::XBase };
  SKIP:
    {
        skip( 'No XBase installed', 1 ) if $@;
        skip( "No DBH",             1 ) if $@;
        ok( external_sth($dbh), 'IMPORT($external_sth)' );
    }

    #my $foo=0;
    #sub test2 {$foo = 6;}
    #open(O,'>','tmpss.sql') or die $!;
    #print O "SELECT test2";
    #close O;
    #$dbh->do("CREATE FUNCTION test2");
    #ok($dbh->do(qq{CALL RUN('tmpss.sql')}),'run');
    #ok(6==$foo,'call run');
    #unlink 'tmpss.sql' if -e 'tmpss.sql';

    ok( $dbh->do("DROP TABLE phrase"), "DROP $temp TABLE" );

  SKIP:
    {
        if ( $test_dbd eq "DBD::DBM" and !$recommended->{MLDBM} )
        {
            skip( "DBD::DBM Update test wont run without MLDBM", 2 );
        }
        my $pauli = [
                      [ 1, 'H',   19 ],
                      [ 2, 'H',   21 ],
                      [ 3, 'KK',  1 ],
                      [ 4, 'KK',  2 ],
                      [ 5, 'KK',  13 ],
                      [ 6, 'MMM', 25 ],
                    ];
        $dbh->do(qq{CREATE $temp TABLE pauli (id INT, column1 TEXT, column2 INTEGER)});
        $sth = $dbh->prepare("INSERT INTO pauli VALUES (?, ?, ?)");
        foreach my $line ( @{$pauli} )
        {
            $sth->execute( @{$line} );
        }
        $sth = $dbh->prepare("UPDATE pauli SET column1 = ? WHERE column1 = ?");
        my $cnt = $sth->execute( "XXXX", "KK" );
        cmp_ok( $cnt, '==', 3, 'UPDATE with placeholders' );
        $sth->finish();

        $sth = $dbh->prepare("SELECT column1, COUNT(column1) FROM pauli GROUP BY column1");
        $sth->execute();
        my $hres = $sth->fetchall_hashref('column1');
        cmp_ok( $hres->{XXXX}->{'COUNT'}, '==', 3, 'UPDATE with placeholder updates correct' );
    }
}

done_testing();
