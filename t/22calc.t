#!/usr/bin/perl
use strict;
use warnings;

use Test::More tests => 40;
use Data::Dumper;

# test 1
BEGIN
{
    use_ok('SQL::Statement');
    use_ok('SQL::Parser');
}
my $loaded = 1;
END { print "not ok 1\n" unless $loaded; }

my $stmt;
my $cache = {};
my $parser = SQL::Parser->new(
                               'ANSI',
                               {
                                  RaiseError => 0,
                                  PrintError => 1
                               }
                             );

sub do_sql(@)
{
    my @stmts = @_;
    foreach my $sql (@stmts)
    {
        chomp $sql;
        $sql =~ s/^\s+//;
        $sql =~ s/--.*$//;
        $sql =~ s/\s+$//;
        next if ( '' eq $sql );
        $stmt = SQL::Statement->new( $sql, $parser );
        ok( $stmt->execute($cache), $sql );
        if ( $stmt->{errstr} )
        {
            warn("'$sql' -> '$stmt->{errstr}'");
        }
    }
}

my $now = time();

do_sql(q{CREATE TEMP TABLE log (id INT, host CHAR, signature CHAR, message CHAR, time_stamp TIMESTAMP)});

do_sql( split( "\n", join( '', sprintf( <<'EOD', ($now) x 7 ) ) ) );
INSERT INTO log VALUES (1, 'bert', '/netbsd', 'Copyright (c) 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005,', %d)
INSERT INTO log VALUES (2, 'bert', '/netbsd', '2006, 2007, 2008, 2009', %d)
INSERT INTO log VALUES (3, 'bert', '/netbsd', 'The NetBSD Foundation, Inc.  All rights reserved.', %d)
INSERT INTO log VALUES (4, 'bert', '/netbsd', 'Copyright (c) 1982, 1986, 1989, 1991, 1993', %d)
INSERT INTO log VALUES (5, 'bert', '/netbsd', 'The Regents of the University of California.  All rights reserved.', %d)
INSERT INTO log VALUES (6, 'bert', '/netbsd', '', %d)
INSERT INTO log VALUES (7, 'bert', '/netbsd', 'NetBSD 5.99.21 (BERT) #0: Mon Nov 30 08:16:07 CET 2009', %d)
EOD

my @timelist;
for my $hour ( 1 .. 10 )
{
    push( @timelist, $now - ( $hour * 3600 ) );
}

do_sql( split( "\n", join( '', sprintf( <<'EOD', @timelist ) ) ) );
INSERT INTO log VALUES (8, 'ernie', 'rpc.statd', 'starting', %d)
INSERT INTO log VALUES (9, 'ernie', 'savecore', 'no core dump', %d)
INSERT INTO log VALUES (10, 'ernie', 'postfix/postfix-script', 'starting the Postfix mail system', %d)
INSERT INTO log VALUES (11, 'ernie', 'rpcbind', 'connect from 127.0.0.1 to dump()', %d)
INSERT INTO log VALUES (12, 'ernie', 'sshd', 'last message repeated 2 times', %d)
INSERT INTO log VALUES (13, 'ernie', 'shutdown', 'poweroff by root:', %d)
INSERT INTO log VALUES (14, 'ernie', 'shutdown', 'rebooted by root', %d)
INSERT INTO log VALUES (15, 'ernie', 'sshd', 'Server listening on :: port 22.', %d)
INSERT INTO log VALUES (16, 'ernie', 'sshd', 'Server listening on 0.0.0.0 port 22.', %d)
INSERT INTO log VALUES (17, 'ernie', 'sshd', 'Received SIGHUP; restarting.', %d)
EOD

my %calcs = (
    q{SELECT id,host,signature,message FROM log WHERE time_stamp < (%d - ( 4 * 60 )) ORDER BY id} =>
      '8^ernie^rpc.statd^starting^9^ernie^savecore^no core dump^10^ernie^postfix/postfix-script^starting the Postfix mail system^11^ernie^rpcbind^connect from 127.0.0.1 to dump()^12^ernie^sshd^last message repeated 2 times^13^ernie^shutdown^poweroff by root:^14^ernie^shutdown^rebooted by root^15^ernie^sshd^Server listening on :: port 22.^16^ernie^sshd^Server listening on 0.0.0.0 port 22.^17^ernie^sshd^Received SIGHUP; restarting.',
    q{SELECT id,host,signature,message FROM log WHERE (time_stamp > (%d - 5)) AND (time_stamp < (%d + 5)) ORDER BY id}
      => '1^bert^/netbsd^Copyright (c) 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005,^2^bert^/netbsd^2006, 2007, 2008, 2009^3^bert^/netbsd^The NetBSD Foundation, Inc.  All rights reserved.^4^bert^/netbsd^Copyright (c) 1982, 1986, 1989, 1991, 1993^5^bert^/netbsd^The Regents of the University of California.  All rights reserved.^6^bert^/netbsd^^7^bert^/netbsd^NetBSD 5.99.21 (BERT) #0: Mon Nov 30 08:16:07 CET 2009',
    q{SELECT id,host,signature,message FROM log WHERE time_stamp BETWEEN ( %d - 5, %d + 5) ORDER BY id} =>
      '1^bert^/netbsd^Copyright (c) 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005,^2^bert^/netbsd^2006, 2007, 2008, 2009^3^bert^/netbsd^The NetBSD Foundation, Inc.  All rights reserved.^4^bert^/netbsd^Copyright (c) 1982, 1986, 1989, 1991, 1993^5^bert^/netbsd^The Regents of the University of California.  All rights reserved.^6^bert^/netbsd^^7^bert^/netbsd^NetBSD 5.99.21 (BERT) #0: Mon Nov 30 08:16:07 CET 2009',
);

$calcs{q{SELECT MAX(time_stamp) FROM log WHERE time_stamp IN ( %d - (2*3600), %d - (4*3600))}} = $now - ( 2 * 3600 );
$calcs{q{SELECT MAX(time_stamp - 3*3600) FROM log}}                                            = $now - ( 3 * 3600 );
$calcs{q{SELECT MAX( CHAR_LENGTH(message) ) FROM log}}                                         = '73';
$calcs{q{SELECT 1+0 from log}} = '1^1^1^1^1^1^1^1^1^1^1^1^1^1^1^1^1';
$calcs{q{SELECT 1+1*2}}        = 3;
$calcs{q{SELECT 1}}            = 1;

while ( my ( $sql_t, $result ) = each(%calcs) )
{
    my $sql = sprintf( $sql_t, $now, $now, $now, $now );
    $stmt = SQL::Statement->new( $sql, $parser );
    eval { $stmt->execute($cache) };
    warn $@ if $@;
    ok( !$@, '$stmt->execute "' . $sql . '" (' . $stmt->command . ')' );
    my @res;
    while ( my $row = $stmt->fetch )
    {
        push( @res, @{$row} );
    }
    is( join( '^', @res ), $result, $sql );
}

$parser->{PrintError} = 0;
my %todo = ( q{SELECT MAX(time_stamp) - 3*3600 FROM log} => $now - ( 3 * 3600 ), );

while ( my ( $sql_t, $result ) = each(%todo) )
{
  TODO:
    {
        local $TODO = "Known limitation. Parser/Engine can not handle properly";
        my $sql = sprintf( $sql_t, $now, $now, $now, $now );
        $stmt = SQL::Statement->new( $sql, $parser );
        eval { $stmt->execute($cache) };
        warn $@ if($@);
        ok( !$@, '$stmt->execute "' . $sql . '" (' . ($stmt->command() || 'n/a') . ')' );
        my @res;
        while ( my $row = $stmt->fetch )
        {
            push( @res, @{$row} );
        }
        is( join( '^', @res ), $result, $sql );
    }
}
