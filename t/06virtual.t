#!/usr/bin/perl -w
use strict;
use warnings;
use lib qw(t);

use Test::More;
use TestLib qw(connect prove_reqs show_reqs test_dir default_recommended);

use Params::Util qw(_CODE _ARRAY);

my ( $required, $recommended ) = prove_reqs( { default_recommended(), ( MLDBM => 0 ) } );
show_reqs( $required, $recommended );
my @test_dbds = ( 'SQL::Statement', grep { /^dbd:/i } keys %{$recommended} );
my $testdir = test_dir();

my @massValues = map { [ $_, ( "a" .. "f" )[ int rand 6 ], int rand 10 ] } ( 1 .. 3999 );

SKIP:
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
    if ( $test_dbd eq "DBD::DBM" )
    {
        if ( $recommended->{MLDBM} )
        {
            $extra_args{dbm_mldbm} = "Storable";
        }
        else
        {
            skip( 'DBD::DBM test runs without MLDBM', 1 );
        }
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

    for my $sql (
        split /\n/, <<""
	CREATE $temp TABLE biz (sales INTEGER, class CHAR, color CHAR, BUGNULL CHAR)
	INSERT INTO biz VALUES (1000, 'Car',   'White', NULL)
	INSERT INTO biz VALUES ( 500, 'Car',   'Blue',  NULL )
	INSERT INTO biz VALUES ( 400, 'Truck', 'White', NULL )
	INSERT INTO biz VALUES ( 700, 'Car',   'Red',   NULL )
	INSERT INTO biz VALUES ( 300, 'Truck', 'White', NULL )
	CREATE $temp TABLE numbers (c_foo INTEGER, foo CHAR, bar INTEGER)
	CREATE $temp TABLE trick   (id INTEGER, foo CHAR)
	INSERT INTO trick VALUES (1, '1foo')
	INSERT INTO trick VALUES (11, 'foo')

                )
    {
        ok( $sth = $dbh->prepare($sql), "prepare $sql on $test_dbd" ) or diag( $dbh->errstr() );
        ok( $sth->execute(), "execute $sql on $test_dbd" ) or diag( $sth->errstr() );
    }

    my @tests = (
        {
           test     => 'GROUP BY one column',
           sql      => "SELECT class,SUM(sales) as foo, MAX(sales) FROM biz GROUP BY class",
           fetch_by => 'class',
           result   => {
                       Car => {
                                MAX   => '1000',
                                foo   => 2200,
                                class => 'Car'
                              },
                       Truck => {
                                  MAX   => '400',
                                  foo   => 700,
                                  class => 'Truck'
                                }
                     },
        },
        {
           test     => "GROUP BY several columns",
           sql      => "SELECT color,class,SUM(sales), MAX(sales) FROM biz GROUP BY color,class",
           fetch_by => [ 'color', 'class' ],
           result   => {
                       Blue => {
                                 Car => {
                                          color => 'Blue',
                                          class => 'Car',
                                          SUM   => 500,
                                          MAX   => 500,
                                        },
                               },
                       Red => {
                                Car => {
                                         color => 'Red',
                                         class => 'Car',
                                         SUM   => 700,
                                         MAX   => 700,
                                       },
                              },
                       White => {
                                  Car => {
                                           color => 'White',
                                           class => 'Car',
                                           SUM   => 1000,
                                           MAX   => 1000,
                                         },
                                  Truck => {
                                             color => 'White',
                                             class => 'Truck',
                                             SUM   => 700,
                                             MAX   => 400,
                                           },
                                }
                     },
        },
        {
           test   => 'AGGREGATE FUNCTIONS WITHOUT GROUP BY',
           sql    => "SELECT SUM(sales), MAX(sales) FROM biz",
           result => [ [ 2900, 1000 ], ]
        },
        {
           test     => 'COUNT(distinct column) WITH GROUP BY',
           sql      => "SELECT distinct class, COUNT(distinct color) FROM biz GROUP BY class",
           fetch_by => 'class',
           result   => {
                       Car => {
                                class => 'Car',
                                COUNT => 3,
                              },
                       Truck => {
                                  class => 'Truck',
                                  COUNT => 1,
                                },
                     },
        },
        {
           test     => 'COUNT(*) with GROUP BY',
           sql      => "SELECT class, COUNT(*) FROM biz GROUP BY class",
           fetch_by => 'class',
           result   => {
                       Car => {
                                class => 'Car',
                                COUNT => 3,
                              },
                       Truck => {
                                  class => 'Truck',
                                  COUNT => 2,
                                },
                     },
        },
        {
           test        => 'COUNT(DISTINCT *) fails',
           sql         => "SELECT class, COUNT(distinct *) FROM biz GROUP BY class",
           prepare_err => qr/Keyword DISTINCT is not allowed for COUNT/m,
        },
        {
           test => 'GROUP BY required',
           sql  => "SELECT class, COUNT(color) FROM biz",
           execute_err =>
             qr/Column 'biz\.class' must appear in the GROUP BY clause or be used in an aggregate function/,
        },
        {
           test   => 'SUM(bar) of empty table',
           sql    => "SELECT SUM(bar) FROM numbers",
           result => [ [undef] ],
        },
        {
           test   => 'COUNT(bar) of empty table with GROUP BY',
           sql    => "SELECT COUNT(bar),c_foo FROM numbers GROUP BY c_foo",
           result => [ [ 0, undef ] ],
        },
        {
           test   => 'COUNT(*) of empty table',
           sql    => "SELECT COUNT(*) FROM numbers",
           result => [ [0] ],
        },
        {
           test   => 'Mass insert of random numbers',
           sql    => "INSERT INTO numbers VALUES (?, ?, ?)",
           params => \@massValues,
        },
        {
           test        => 'Number of rows in aggregated Table',
           sql         => "SELECT foo AS boo, COUNT (*) AS counted FROM numbers GROUP BY boo",
           result_cols => [qw(boo counted)],
           result_code => sub {
               my $sth = $_[0];
               my $res = $sth->fetch_rows();
               cmp_ok( scalar( @{$res} ), '==', '6', 'Number of rows in aggregated Table' );
               my $all_counted = 0;
               foreach my $row ( @{$res} )
               {
                   $all_counted += $row->[1];
               }
               cmp_ok( $all_counted, '==', 3999, 'SUM(COUNTED)' );
           },
        },
        {
           test   => 'Aggregate functions MIN, MAX, AVG',
           sql    => "SELECT MIN(c_foo), MAX(c_foo), AVG(c_foo) FROM numbers",
           result => [ [ 1, 3999, 2000 ], ],
        },
        {
           test   => 'COUNT(*) internal for nasty table',
           sql    => "SELECT COUNT(*) FROM trick",
           result => [ [2] ],
        },
    );

    foreach my $test (@tests)
    {
        if ( defined( $test->{prepare_err} ) )
        {
            $sth = $dbh->prepare( $test->{sql} );
            ok( !$sth, "prepare $test->{sql} using $test_dbd fails" );
            like( $dbh->errstr(), $test->{prepare_err}, $test->{test} );
            next;
        }
        $sth = $dbh->prepare( $test->{sql} );
        ok( $sth, "prepare $test->{sql} using $test_dbd" ) or diag( $dbh->errstr() );
        if ( defined( $test->{params} ) )
        {
	    my $params;
            if ( defined( _CODE( $test->{params} ) ) )
            {
                $params = [ &{ $test->{params} } ];
            }
            elsif ( !defined( _ARRAY( $test->{params}->[0] ) ) )
            {
                $params = [ $test->{params} ];
            }
	    else
	    {
		$params = $test->{params};
	    }

            my $i = 0;
            my @failed;
            foreach my $bp ( @{ $test->{params} } )
            {
                ++$i;
                my $n = $sth->execute(@$bp);
                $n
                  or
                  ok( $n, "$i: execute $test->{sql} using $test_dbd (" . DBI::neat_list($bp) . ")" )
                  or diag( $dbh->errstr() )
                  or push( @failed, $bp );

                # 'SELECT' eq $sth->command() or next;
                # could become funny ...
            }

            @failed or ok( 1, "1 .. $i: execute $test->{sql} using $test_dbd" );
        }
        else
        {
            my $n = $sth->execute();
            if ( defined( $test->{execute_err} ) )
            {
                ok( !$n, "execute $test->{sql} using $test_dbd fails" );
                like( $dbh->errstr(), $test->{execute_err}, $test->{test} );
                next;
            }

            ok( $n, "execute $test->{sql} using $test_dbd" ) or diag( $dbh->errstr() );
            'SELECT' eq $sth->command() or next;

            if ( $test->{result_cols} )
            {
                is_deeply( $sth->col_names(), $test->{result_cols}, "Columns in $test->{test}" );
            }

            if ( $test->{fetch_by} )
            {
                is_deeply( $sth->fetchall_hashref( $test->{fetch_by} ),
                           $test->{result}, $test->{test} );
            }
            elsif ( $test->{result_code} )
            {
                &{ $test->{result_code} }($sth);
            }
            else
            {
                is_deeply( $sth->fetch_rows(), $test->{result}, $test->{test} );
            }
        }
    }
}

done_testing();
