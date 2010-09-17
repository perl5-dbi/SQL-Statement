#!/usr/bin/perl -w
use strict;
use warnings;
use lib qw(t);

use Test::More;
use TestLib qw(connect prove_reqs show_reqs test_dir default_recommended);

my ( $required, $recommended ) = prove_reqs( { default_recommended(), ( MLDBM => 0 ) } );
my ( undef, $extra_recommended ) = prove_reqs( { 'DBD::SQLite' => 0, } );
show_reqs( $required, { %$recommended, %$extra_recommended } );
my @test_dbds = ( 'SQL::Statement', grep { /^dbd:/i } keys %{$recommended} );
my $testdir = test_dir();

my @external_dbds =
  ( keys %$extra_recommended, grep { /^dbd::(?:dbm|csv)/i } keys %{$recommended} );

foreach my $test_dbd (@test_dbds)
{
    my $dbh;
    diag("Running tests for $test_dbd");
    my $temp = "";
    # XXX
    # my $test_dbd_tbl = "${test_dbd}::Table";
    # $test_dbd_tbl->can("fetch") or $temp = "$temp";
    $test_dbd eq "DBD::File"      and $temp = "TEMP";
    $test_dbd eq "SQL::Statement" and $temp = "TEMP";

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

    my $external_dsn;
    if (%$extra_recommended)
    {
        if ( $extra_recommended->{'DBD::SQLite'} )
        {
            $external_dsn = "DBI:SQLite:dbname=" . File::Spec->catfile( $testdir, 'sqlite.db' );
        }
    }
    elsif (@external_dbds)
    {
        if ( $test_dbd eq $external_dbds[0] and @external_dbds > 1 )
        {
            $external_dsn = $external_dbds[1];
        }
        else
        {
            $external_dsn = $external_dbds[0];
        }
        $external_dsn =~ s/^dbd::(\w+)$/dbi:$1:/i;
        my @valid_dsns = DBI->data_sources( $external_dsn, { f_dir => $testdir } );
        $external_dsn = $valid_dsns[0];
    }

    my ( $sth, $str );

    ok( $dbh->do(qq{ CREATE $temp TABLE Tmp (id INT,phrase VARCHAR(30)) }), 'CREATE Tmp' )
      or diag( $dbh->errstr() );
    ok( $dbh->do( qq{ INSERT INTO Tmp (id,phrase) VALUES (?,?) }, {}, 9, 'yyy' ),
        'placeholder insert with named cols' )
      or diag( $dbh->errstr() );
    ok( $dbh->do( qq{ INSERT INTO Tmp VALUES(?,?) }, {}, 2, 'zzz' ),
        'placeholder insert without named cols' )
      or diag( $dbh->errstr() );
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
    ok( $dbh->do($_), $dbh->command() ) for split /\n/, <<"";
        CREATE $temp TABLE phrase (id INT,phrase VARCHAR(30))
	INSERT INTO phrase VALUES(1,UPPER(TRIM(' foo ')))
	INSERT INTO phrase VALUES(2,'baz')
	INSERT INTO phrase VALUES(3,'qux')
	UPDATE phrase SET phrase=UPPER(TRIM(LEADING 'z' FROM 'zbar')) WHERE id=3
	DELETE FROM phrase WHERE id = 2

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
    cmp_ok( $str, 'eq', 'Just^Another^Perl^Hacker^', 'IMPORT($AoA)' );

    #######################
    # IMPORT($AoH)
    #######################
    my $aoh = [
                {
                   c1 => 1,
                   c2 => 9
                },
                {
                   c1 => 2,
                   c2 => 8
                }
              ];
    $sth = $dbh->prepare("SELECT C1,c2 FROM IMPORT(?)");
    $sth->execute($aoh);
    $str = '';
    while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
    cmp_ok( $str, 'eq', '1 9^2 8^', 'IMPORT($AoH)' );

    #######################
    # IMPORT($internal_sth)
    #######################
  SKIP:
    {
        skip( "Need DBI statement handle - can't use when executing direct", 7 )
          if ( $dbh->isa('TestLib::Direct') );

        ok( $dbh->do( "CREATE $temp TABLE aoh AS IMPORT(?)", {}, $aoh ), 'CREATE AS IMPORT($aoh)' )
          or diag( $dbh->errstr() );
        $sth = $dbh->prepare("SELECT C1,c2 FROM aoh");
        $sth->execute();
        $str = '';
        while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
        cmp_ok( $str, 'eq', '1 9^2 8^', 'SELECT FROM IMPORTED($AoH)' );

        ok( $dbh->do( "CREATE $temp TABLE aoa AS IMPORT(?)", {}, $AoA ), 'CREATE AS IMPORT($aoa)' )
          or diag( $dbh->errstr() );
        $sth = $dbh->prepare("SELECT word FROM aoa ORDER BY id DESC");
        $sth->execute();
        $str = '';
        while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
        cmp_ok( $str, 'eq', 'Just^Another^Perl^Hacker^', 'SELECT FROM IMPORTED($AoA)' );

        ok( $dbh->do("CREATE $temp TABLE tbl_copy AS SELECT * FROM aoa"), 'CREATE AS SELECT *' )
          or diag( $dbh->errstr() );
        $sth = $dbh->prepare("SELECT * FROM tbl_copy ORDER BY id ASC");
        $sth->execute();
        $str = '';
        while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
        cmp_ok( $str, 'eq', '1 Hacker^2 Perl^3 Another^4 Just^', 'SELECT FROM "SELECTED(*)"' );

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
    }

    #######################
    # IMPORT($external_sth)
    #######################
  SKIP:
    {
        skip( 'No external usable data source installed', 2 ) unless ($external_dsn);

        my $xb_dbh = DBI->connect($external_dsn);
        $xb_dbh->do($_) for split /\n/, <<"";
    CREATE TABLE xb (id INTEGER, xphrase VARCHAR(30))
    INSERT INTO xb VALUES(1,'foo')

        my $xb_sth = $xb_dbh->prepare('SELECT * FROM xb');
        $xb_sth->execute();

        $sth = $dbh->prepare('SELECT * FROM IMPORT(?)');
        $sth->execute($xb_sth);
        $str = '';
        while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
        cmp_ok( $str, 'eq', '1 foo^', 'SELECT IMPORT($external_sth)' );

      SKIP:
        {
            skip( "Need DBI statement handle - can't use when executing direct", 2 )
              if ( $dbh->isa('TestLib::Direct') );

            $xb_sth = $xb_dbh->prepare('SELECT * FROM xb');
            $xb_sth->execute();

            ok( $dbh->do( "CREATE $temp TABLE xbi AS IMPORT(?)", {}, $xb_sth ),
                'CREATE AS IMPORT($sth)' )
              or diag( $dbh->errstr() );
            $sth = $dbh->prepare('SELECT * FROM xbi');
            $sth->execute();
            $str = '';
            while ( my $r = $sth->fetch_row() ) { $str .= "@$r^"; }
            cmp_ok( $str, 'eq', '1 foo^', 'SELECT FROM IMPORTED ($external_sth)' );
        }

        $xb_dbh->do("DROP TABLE xb");
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
            skip( "DBD::DBM Update test won't run without MLDBM", 3 );
        }
        my $pauli = [
                      [ 1, 'H',   19 ],
                      [ 2, 'H',   21 ],
                      [ 3, 'KK',  1 ],
                      [ 4, 'KK',  2 ],
                      [ 5, 'KK',  13 ],
                      [ 6, 'MMM', 25 ],
                    ];
        ok( $dbh->do(qq{CREATE $temp TABLE pauli (id INT, column1 VARCHAR, column2 INTEGER)}),
            'CREATE pauli test table' )
          or diag( $dbh->errstr() );
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
