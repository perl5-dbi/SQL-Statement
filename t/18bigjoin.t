#!/usr/bin/perl
use warnings;
use strict;

use Test::More tests => 89;
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
                                  PrintError => 0
                               }
                             );
for my $sql ( split( "\n", join( '', <<'EOD' ) ) )
CREATE TEMP TABLE APPL (id INT, applname CHAR, appluniq CHAR, version CHAR, appl_type CHAR)
CREATE TEMP TABLE PREC (id INT, appl_id INT, node_id INT, precedence INT)
CREATE TEMP TABLE NODE (id INT, nodename CHAR, os CHAR, version CHAR)
CREATE TEMP TABLE LANDSCAPE (id INT, landscapename CHAR)
CREATE TEMP TABLE CONTACT (id INT, surname CHAR, familyname CHAR, phone CHAR, userid CHAR, mailaddr CHAR)
CREATE TEMP TABLE NM_LANDSCAPE (id INT, ls_id INT, obj_id INT, obj_type INT)
CREATE TEMP TABLE APPL_CONTACT (id INT, contact_id INT, appl_id INT, contact_type CHAR)

INSERT INTO APPL VALUES ( 1, 'ZQF', 'ZFQLIN', '10.2.0.4', 'Oracle DB')
INSERT INTO APPL VALUES ( 2, 'YRA', 'YRA-UX', '10.2.0.2', 'Oracle DB')
INSERT INTO APPL VALUES ( 3, 'PRN1', 'PRN1-4.B2', '1.1.22', 'CUPS' )
INSERT INTO APPL VALUES ( 4, 'PRN2', 'PRN2-4.B2', '1.1.22', 'CUPS' )
INSERT INTO APPL VALUES ( 5, 'PRN1', 'PRN1-4.B1', '1.1.22', 'CUPS' )
INSERT INTO APPL VALUES ( 7, 'PRN2', 'PRN2-4.B1', '1.1.22', 'CUPS' )
INSERT INTO APPL VALUES ( 8, 'sql-stmt', 'SQL::Statement', '1.21', 'Project Web-Site')
INSERT INTO APPL VALUES ( 9, 'cpan.org', 'http://www.cpan.org/', '1.0', 'Web-Site')
INSERT INTO APPL VALUES (10, 'httpd', 'cpan-apache', '2.2.13', 'Web-Server')
INSERT INTO APPL VALUES (11, 'cpan-mods', 'cpan-mods', '8.4.1', 'PostgreSQL DB')
INSERT INTO APPL VALUES (12, 'cpan-authors', 'cpan-authors', '8.4.1', 'PostgreSQL DB')

INSERT INTO NODE VALUES ( 1, 'ernie', 'RHEL', '5.2')
INSERT INTO NODE VALUES ( 2, 'bert', 'RHEL', '5.2')
INSERT INTO NODE VALUES ( 3, 'statler', 'FreeBSD', '7.2')
INSERT INTO NODE VALUES ( 4, 'waldorf', 'FreeBSD', '7.2')
INSERT INTO NODE VALUES ( 5, 'piggy', 'NetBSD', '5.0.2')
INSERT INTO NODE VALUES ( 6, 'kermit', 'NetBSD', '5.0.2')
INSERT INTO NODE VALUES ( 7, 'samson', 'NetBSD', '5.0.2')
INSERT INTO NODE VALUES ( 8, 'tiffy', 'NetBSD', '5.0.2')
INSERT INTO NODE VALUES ( 9, 'rowlf', 'Debian Lenny', '5.0')
INSERT INTO NODE VALUES (10, 'fozzy', 'Debian Lenny', '5.0')

INSERT INTO PREC VALUES ( 1,  1,  1, 1)
INSERT INTO PREC VALUES ( 2,  1,  2, 2)
INSERT INTO PREC VALUES ( 3,  2,  2, 1)
INSERT INTO PREC VALUES ( 4,  2,  1, 2)
INSERT INTO PREC VALUES ( 5,  3,  5, 1)
INSERT INTO PREC VALUES ( 6,  3,  7, 2)
INSERT INTO PREC VALUES ( 7,  4,  6, 1)
INSERT INTO PREC VALUES ( 8,  4,  8, 2)
INSERT INTO PREC VALUES ( 9,  5,  7, 1)
INSERT INTO PREC VALUES (10,  5,  5, 2)
INSERT INTO PREC VALUES (11,  6,  8, 1)
INSERT INTO PREC VALUES (12,  7,  6, 2)
INSERT INTO PREC VALUES (13, 10,  9, 1)
INSERT INTO PREC VALUES (14, 10, 10, 1)
INSERT INTO PREC VALUES (15,  8,  9, 1)
INSERT INTO PREC VALUES (16,  8, 10, 1)
INSERT INTO PREC VALUES (17,  9,  9, 1)
INSERT INTO PREC VALUES (17,  9, 10, 1)
INSERT INTO PREC VALUES (18, 11,  3, 1)
INSERT INTO PREC VALUES (19, 11,  4, 2)
INSERT INTO PREC VALUES (20, 12,  4, 1)
INSERT INTO PREC VALUES (21, 12,  3, 2)

INSERT INTO LANDSCAPE VALUES (1, 'Logistic')
INSERT INTO LANDSCAPE VALUES (2, 'Infrastructure')
INSERT INTO LANDSCAPE VALUES (3, 'CPAN')

INSERT INTO CONTACT VALUES ( 1, 'Hans Peter', 'Mueller', '12345', 'HPMUE', 'hp-mueller@here.com')
INSERT INTO CONTACT VALUES ( 2, 'Knut', 'Inge', '54321', 'KINGE', 'k-inge@here.com')
INSERT INTO CONTACT VALUES ( 3, 'Lola', 'Nguyen', '+1-123-45678-90', 'LNYUG', 'lola.ngyuen@customer.com')
INSERT INTO CONTACT VALUES ( 4, 'Helge', 'Brunft', '+41-123-45678-09', 'HBRUN', 'helge.brunft@external-dc.at')

-- TYPE: 1: APPL 2: NODE 3: CONTACT
INSERT INTO NM_LANDSCAPE VALUES ( 1, 1, 1, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 2, 1, 2, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 3, 3, 3, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 4, 3, 4, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 5, 2, 5, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 6, 2, 6, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 7, 2, 7, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 8, 2, 8, 2)
INSERT INTO NM_LANDSCAPE VALUES ( 9, 3, 9, 2)
INSERT INTO NM_LANDSCAPE VALUES (10, 3,10, 2)
INSERT INTO NM_LANDSCAPE VALUES (11, 1, 1, 1)
INSERT INTO NM_LANDSCAPE VALUES (12, 2, 2, 1)
INSERT INTO NM_LANDSCAPE VALUES (13, 2, 2, 3)
INSERT INTO NM_LANDSCAPE VALUES (14, 3, 1, 3)

INSERT INTO APPL_CONTACT VALUES (1, 3, 1, 'OWNER')
INSERT INTO APPL_CONTACT VALUES (2, 3, 2, 'OWNER')
INSERT INTO APPL_CONTACT VALUES (3, 4, 3, 'ADMIN')
INSERT INTO APPL_CONTACT VALUES (4, 4, 4, 'ADMIN')
INSERT INTO APPL_CONTACT VALUES (5, 4, 5, 'ADMIN')
INSERT INTO APPL_CONTACT VALUES (6, 4, 6, 'ADMIN')
EOD
{
    chomp $sql;
    $sql =~ s/^\s+//;
    $sql =~ s/--.*$//;
    $sql =~ s/\s+$//;
    next if( '' eq $sql );
    $stmt = SQL::Statement->new( $sql, $parser );
    ok( $stmt->execute($cache), $sql );
}

# CREATE TEMP TABLE APPL (id INT, applname CHAR, appluniq CHAR, version CHAR, appl_type CHAR)
# CREATE TEMP TABLE PREC (id INT, appl_id INT, node_id INT, precedence INT)
# CREATE TEMP TABLE NODE (id INT, nodename CHAR, os CHAR, version CHAR)
# CREATE TEMP TABLE LANDSCAPE (id INT, landscapename CHAR)
# CREATE TEMP TABLE CONTACT (id INT, surname CHAR, familyname CHAR, phone CHAR, userid CHAR, mailaddr CHAR, contact_type CHAR)
# CREATE TEMP TABLE NM_LANDSCAPE (id INT, ls_id INT, obj_id INT, obj_type INT)
# CREATE TEMP TABLE APPL_CONTACT (id INT, contact_id INT, appl_id INT, contact_type CHAR)

my %joins = (
  q{SELECT applname, appluniq, version, nodename FROM APPL, PREC, NODE WHERE appl_type LIKE '%DB' AND APPL.id=PREC.appl_id AND PREC.node_id=NODE.id} => 'ZQF^ZFQLIN^10.2.0.4^ernie^ZQF^ZFQLIN^10.2.0.4^bert^YRA^YRA-UX^10.2.0.2^bert^YRA^YRA-UX^10.2.0.2^ernie^cpan-mods^cpan-mods^8.4.1^statler^cpan-mods^cpan-mods^8.4.1^waldorf^cpan-authors^cpan-authors^8.4.1^waldorf^cpan-authors^cpan-authors^8.4.1^statler',
  q{SELECT applname, appluniq, version, landscapename, nodename FROM APPL, PREC, NODE, LANDSCAPE, NM_LANDSCAPE WHERE appl_type LIKE '%DB' AND APPL.id=PREC.appl_id AND PREC.node_id=NODE.id AND NM_LANDSCAPE.obj_id=APPL.id AND NM_LANDSCAPE.obj_type=1 AND NM_LANDSCAPE.ls_id=LANDSCAPE.id} => 'ZQF^ZFQLIN^10.2.0.4^Logistic^ernie^ZQF^ZFQLIN^10.2.0.4^Logistic^bert^YRA^YRA-UX^10.2.0.2^Infrastructure^bert^YRA^YRA-UX^10.2.0.2^Infrastructure^ernie',
  q{SELECT applname, appluniq, version, surname, familyname, phone, nodename FROM APPL, PREC, NODE, CONTACT, APPL_CONTACT WHERE appl_type='CUPS' AND APPL.id=PREC.appl_id AND PREC.node_id=NODE.id AND APPL_CONTACT.appl_id=APPL.id AND APPL_CONTACT.contact_id=CONTACT.id AND PREC.PRECEDENCE=1} => 'PRN1^PRN1-4.B2^1.1.22^Helge^Brunft^+41-123-45678-09^piggy^PRN2^PRN2-4.B2^1.1.22^Helge^Brunft^+41-123-45678-09^kermit^PRN1^PRN1-4.B1^1.1.22^Helge^Brunft^+41-123-45678-09^samson',
  q{SELECT DISTINCT applname, appluniq, version, surname, familyname, phone, nodename FROM APPL, PREC, NODE, CONTACT, APPL_CONTACT WHERE appl_type='CUPS' AND APPL.id=PREC.appl_id AND PREC.node_id=NODE.id AND APPL_CONTACT.appl_id=APPL.id AND APPL_CONTACT.contact_id=CONTACT.id} => 'PRN1^PRN1-4.B1^1.1.22^Helge^Brunft^+41-123-45678-09^piggy^PRN1^PRN1-4.B1^1.1.22^Helge^Brunft^+41-123-45678-09^samson^PRN1^PRN1-4.B2^1.1.22^Helge^Brunft^+41-123-45678-09^piggy^PRN1^PRN1-4.B2^1.1.22^Helge^Brunft^+41-123-45678-09^samson^PRN2^PRN2-4.B2^1.1.22^Helge^Brunft^+41-123-45678-09^kermit^PRN2^PRN2-4.B2^1.1.22^Helge^Brunft^+41-123-45678-09^tiffy',
  q{SELECT CONCAT('[% NOW %]') AS timestamp, applname, appluniq, version, nodename FROM APPL, PREC, NODE WHERE appl_type LIKE '%DB' AND APPL.id=PREC.appl_id AND PREC.node_id=NODE.id} => '[% NOW %]^ZQF^ZFQLIN^10.2.0.4^ernie^[% NOW %]^ZQF^ZFQLIN^10.2.0.4^bert^[% NOW %]^YRA^YRA-UX^10.2.0.2^bert^[% NOW %]^YRA^YRA-UX^10.2.0.2^ernie^[% NOW %]^cpan-mods^cpan-mods^8.4.1^statler^[% NOW %]^cpan-mods^cpan-mods^8.4.1^waldorf^[% NOW %]^cpan-authors^cpan-authors^8.4.1^waldorf^[% NOW %]^cpan-authors^cpan-authors^8.4.1^statler',
);

while( my ( $sql, $result ) = each(%joins) )
{
    $stmt = SQL::Statement->new($sql,$parser);
    eval { $stmt->execute($cache) };
    warn $@ if $@;
    ok(!$@,'$stmt->execute "'.$sql.'" ('.$stmt->command.')');
    my @res;
    while (my $row=$stmt->fetch) {
        push( @res, @{$row} );
    }
    is( join( '^', @res ), $result, $sql );
}
