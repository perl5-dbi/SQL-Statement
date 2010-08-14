#!/usr/bin/perl -w
use strict;
use DBI;
use Text::CSV_XS;
use Getopt::Std;

use vars qw/ %option /;

getopts( 's:d:r:vnh', \%option );

if ( $option{ h } ) {
    &usage;
    exit;
}

foreach ( qw/ s r d / ) {
    die &usage if ! $option{ $_ };
}

my $schema_file = $option{ s };
my $raw_data    = $option{ r };
my $data_dir    = format_data_dir( $option{ d } );
my $no_header   = $option{ n } || 0;

# errors that are not immediately fatal will be written to the following log:
my $error_log  = 'err.log';

if ( ! -e $data_dir ) {
    if ( $option{ v } ) {
        # we're validating an exisisting database, so there had
        # darned well better be one there!
        die "Attempt to validate database '$data_dir' failed.  Folder does not exist.\n";
    }
    mkdir $data_dir or die "Could not mkdir $data_dir: $!";
}

my $database = schema( $schema_file );

if ( $option{ v } ) {
    print "\nWarning: you are about to validate an existing database.\n" .
          "This program will drop the tables prior to validation.\n" .
          "Did you back up the database? ";
    exit if 'y' ne lc substr <STDIN>, 0, 1;

    create_raw_data( $data_dir, $raw_data, $no_header, $database );
}

my $dbh = DBI->connect("DBI:CSV:f_dir=$data_dir", { RaiseError => 1 } );
create_database( $dbh, $data_dir, $database );

print "All tables successfully created.\n";

add_data_to_database( $dbh, $raw_data, $database );

print "All data successfully added to database.\n";

open ERROR_LOG, "> $error_log" or die "Cannot open $error_log for writing: $!";
print "Now validating unique fields.\n";
my $error_count = validate_unique_fields( \*ERROR_LOG, $dbh, $database );

print "Now validating foreign key constraints.\n";
$error_count += validate_foreign_keys( \*ERROR_LOG, $dbh, $database );

print "Now validating data against regular expressions.\n";
$error_count += validate_with_regexes( \*ERROR_LOG, $dbh, $database );

close ERROR_LOG;

if ( $error_count ) {
    my $text = $error_count == 1 ? " was" : "s were";
    print "\nWARNING\n$error_count error$text found while validating the data.  Errors were written to '$error_log'\n";
} else {
    unlink $error_log;
    print "\nNo errors were found while validating the data.\n";
}

###                ###
#  main subs follow  #
###                ###

sub schema {
    # The following datatypes are used for documentation purposes, but
    # DBD::CSV does not support these properties.  They are mainly here so
    # that programmers can customize these attributes for their own database.
    #
    # They will also be used in this program to ensure that data added to database
    # will pass validation (this will minimize database corruption )
    #
    # currently, each key in %data_type points to a hashref.  This is done in case
    # we need to add extra functionality in the future.
    #
    # note that DBD::CSV relies on SQL::Statement which does not support most
    # data types, such as DateTime or Boolean.

    my $schema_file = shift;

    my %data_type = (
        INT => {
            validate => \&is_int
        },
        VARCHAR => {
            validate => \&is_varchar
        },
        CHAR => {
            validate => \&is_char
        }
    );

    open SCHEMA, "< $schema_file" or die "Cannot open $schema_file for reading:$!\n";

    my $table = '';
    my %schema;
    while ( <SCHEMA> ) {
        next if /^#/;    # skip comments;
        next if /^\s*$/; # skip blank lines
        $table = $1, next if ( /^\s*\[([^\]]+)\]\s*$/ );

        if ( substr( $_, 0, 1 ) ne '_' ) {
            if ( ! $table ) {
                error_in_data( 'unknown', $_, "Could not determine table for line $. in schema '$schema_file'");
            }
            my ( $field, $type, @extra ) = split;
            $type = uc $type;
            $type = 'INT' if $type eq 'INTEGER';
            if ( $type !~ /^(INT|(?:VAR)?CHAR)/ ) {
                error_in_data( $table, $_, "Unknown type '$1' in schema '$schema_file'" );
            }

            if ( ! exists $schema{ $table }{ $field } ) {
                $schema{ $table }{ $field } = $type;
                push @{ $schema{ $table }{ _field_order } }, $field; # holds the order of fields

                if ( defined $extra[0] and uc $extra[0] eq 'UNIQUE' ) {
                    push @{ $schema{ $table }{ _unique } }, $field; # field must not have duplicates
                }
            if ( defined $extra[-1] and $extra[-1] =~ m!^/(.*)/$! ) {
            $schema{ $table }{ _regex }{ $field } = $1;
        }
            } else {
                error_in_data( $table, $_, "Redefined field '$field' in schema '$schema_file'" );
            }
        } elsif ( substr( $_, 0, 12 ) eq '_foreign_key' ) {
            my ( undef, @data ) = split;
            if ( @data != 3 ) {
                error_in_data( $table, $_, "Foreign key definitions are in form 'thistable.field, foreign.table, foreign.table.field");
            } else {
                push @{ $schema{ $table }{ _foreign_key } }, \@data;
            }
        }
    }
    close SCHEMA;

    foreach my $table ( keys %schema ) {
        $schema{ $table }{ _rec_count } = 0; # stop uninitialized warnings later
    }
    $schema{ _data_type } = \%data_type;
    return \%schema;
}

sub create_raw_data {
    # This will only be called if validating an existing database
    # or CSV file
    my ( $data_dir, $raw_data, $no_header, $database ) = @_;
    if ( -e $raw_data ) {
        print "Warning: raw data file '$raw_data' exists.  Overwrite? [y/n] ";
        exit if 'y' ne lc substr <STDIN>, 0, 1;
    }

    open RAW_DATA, "> $raw_data" or die "Cannot open $raw_data for writing: $!";
    foreach my $table ( keys %$database ) {
        next if '_' eq substr $table, 0, 1; # these are data keys
        my $fields = join ', ', @{ $database->{ $table }{ _field_order } };
        $fields = "# $fields\n";
        open TABLE, "< $data_dir$table" or die "Cannot open $data_dir$table for reading: $!";
        my $header = <TABLE>;

        if ( ! $no_header ) {
            $header = '';
        }
        print RAW_DATA "table=$table\n", $fields, $header, <TABLE>, "\n.\n\n";
        close TABLE;
    }
    close RAW_DATA;
}

sub create_database {
    # drop tables if they exist and then recreate them.
    my ( $dbh, $data_dir, $database ) = @_;
    foreach my $table ( keys %$database ) {
        next if substr( $table, 0, 1 ) eq '_';
        if ( -e $data_dir.$table ) {
            $dbh->do( "DROP TABLE $table" );
        }
        my @fields;
        if ( ! exists $database->{$table}{_field_order} ) {
            die "Table: '$table' does not have a _field_order specification.\n";
        }
        my @field_order = @{ $database->{$table}{_field_order} };
        my $order_count = unique( @field_order );
        if ( @field_order != $order_count ) {
            die "Table: '$table' has duplicate fields in _field_order.\n";
        }
        my $field_count = unique( grep { /^[^_]/ } keys %{$database->{ $table }} );
        if ( $field_count != $order_count ) {
            my $comp = $field_count > $order_count ? "more" : "fewer";
            die "Table '$table' has $comp fields than _field_order specifies.";
        }

        foreach my $field ( @field_order ) {
            if ( ! exists $database->{ $table }{ $field } ) {
                die "Table: '$table' does not have field '$field' listed in _field_order";
            }
            push @fields, "$field $database->{$table}{$field}";
        }
        my $fields = join ',', @fields;
        $dbh->do("CREATE TABLE $table ( $fields )");
    }
}

sub add_data_to_database {
    # This could use some work.  Just a roungh hack to get the data.
    # for right now it's fine, but in the future, this could be
    # problematic if someone creates a huge amount of data.
    #
    # Records are separated by putting a period on a line by itself: "\n.\n"
    # First line of each record is table name.  Subsequent records are fields
    # to be entered.  Empty lines and lines beginning with a # are skipped

    my ( $dbh, $raw_data, $database ) = @_;

    my $csv = Text::CSV_XS->new;
    local $/ = "\n.\n";
    open DATA_FILE, "< $raw_data" or die "Cannot open $raw_data for reading: $!\n";
    while ( <DATA_FILE> ) {
        chomp;
        next if /^\s*$/;
        my @records = grep { /^[^#]/ } split /\n/;
        my $table = trim( shift @records );
        if ( $table =~ /=/ ) {
            $table = trim( ( split /=/, $table, 2 )[-1] );
        }

        if ( ! exists $database->{$table} ) {
            die "Table '$table' found in input data but is not in schema.";
        }

        my $place_holders = '?,' x @{ $database->{$table}{_field_order} };
        chop $place_holders; # remove trailing comma
        my $sql = "INSERT INTO $table VALUES ( $place_holders )";
        my $sth = $dbh->prepare( $sql );
        foreach my $record ( @records ) {
            next if $record !~ /\S/ or $record =~ /^\.$/; # skip empty records
            my $status;
            if( ! ( $status = $csv->parse($record) ) ) { # parse a CSV string into fields
                error_in_data( $table, $record, "Bad argument in CSV record" );
            }
            my @columns = $csv->fields();      # get the parsed fields
            if ( @columns != @{ $database->{$table}{_field_order} } ) {
                my $in_length = @columns;
                my $expected_length = @{ $database->{$table}{_field_order} };
                error_in_data( $table, $record, "Expected $expected_length columns.  Found $in_length columns." );
            } else {
                validate_record( $database, $table, \@columns, $csv );
                $sth->execute( @columns );
                $database->{ $table }{ _rec_count }++;
            }
        } # next record
    } # end while
    close DATA_FILE;
    print "\n";
    foreach my $table ( sort keys %$database ) {
        next if substr( $table, 0, 1 ) eq '_';
        my $count = $database->{ $table }{ _rec_count };
        if ( $count == 0 ) {
            print "WARNING: table '$table' created but no records added.\n";
        } else {
            print "$count records added to table '$table'.\n";
        }
    }
    print "\n";
}

sub validate_unique_fields {
    # for each defined unique field, grab all of them from the
    # database and add them to an autoincrementing hash.  If any
    # value is greater than one, then we know we have more than
    # one unique key
    my ( $err, $dbh, $database ) = @_;
    my $error_count = 0;
    foreach my $table ( %$database ) {
        if ( exists $database->{ $table }{ _unique } ) {
            my @unique_fields = @{ $database->{ $table }{ _unique } };
            foreach my $field ( @unique_fields ) {
                my %field_count;
                my $sql = "SELECT $field FROM $table";
                my $fields = $dbh->selectall_arrayref( $sql );
                foreach ( @$fields ) {
                    $field_count{ $_->[0] }++;
                }
                foreach ( keys %field_count ) {
                    if ( $field_count{$_} > 1 ) {
                        print $err "Table: '$table' Unique field: '$_' was found $field_count{$_} times\n";
                        $error_count++;
                    }
                }
            } # next $field
        }
    } # next $table
    return $error_count;
}

sub validate_foreign_keys {
    # the foreign keys for a table are an array ref of array refs.
    # each inner array ref is a table key, foreign table, and foreign key.
    # this routine grabs the table keys and ensures that each one matches
    # a foreign key in the foreign table.
    my ( $err, $dbh, $database ) = @_;
    my $error_count = 0;
    foreach my $table ( %$database ) {
        my $last_error_count = $error_count;
        if ( exists $database->{ $table }{ _foreign_key } ) {

            my @fkey_array = @{ $database->{ $table }{ _foreign_key } };
            my $base_err = 'Foreign Key constraint violated.';

            foreach my $fkey_data ( @fkey_array ) {
                my ( $this_key, $ftable, $ftable_key ) = @$fkey_data;
                if ( ! exists $database->{ $table }{ $this_key } ) {
                    my $err_message = "$base_err  Table '$table' does not have field '$this_key'\n";
                    print $err $err_message;
                    $error_count++;
                }
                if ( ! exists $database->{ $ftable } ) {
                    my $err_message = "$base_err  Foreign table '$ftable' does not exist.\n";
                    print $err $err_message;
                    $error_count++;
                }
                if ( exists $database->{ $ftable } and ! exists $database->{ $ftable }{ $ftable_key } ) {
                    my $err_message = "$base_err  Foreign table '$ftable' does not have field '$ftable_key\n";
                    print $err $err_message;
                    $error_count++;
                }
                if ( exists $database->{ $ftable }
                     and
                     exists $database->{ $ftable }{ $ftable_key }
                     and
                     ( ! exists $database->{ $ftable }{ _unique }
                       or
                       ! grep { /$ftable_key/ } @{$database->{ $ftable }{ _unique }}
                     )
                   )
                {
                    my $err_message = "$base_err  Table '$table', field '$this_key'. ".
                                      "'$ftable_key' not defined as UNIQUE in '$ftable'\n";
                    print $err $err_message;
                    $error_count++;
                }
                next if $error_count > $last_error_count;
                my $sql = "SELECT $this_key FROM $table";
                my $sth = $dbh->prepare( $sql );
                $sth->execute;
                my $tbl_array_ref = $sth->fetchall_arrayref;
                foreach ( @$tbl_array_ref ) {
                    my $data = $dbh->quote( $_->[ 0 ] );
                    my $sql  = "SELECT $ftable_key FROM $ftable WHERE $ftable_key = $data";
                    my $sth  = $dbh->prepare( $sql );
                    my $rv   = $sth->execute;
                    if ( $rv eq '0E0' ) {
                        my $err_message = "$base_err  Table '$table', field '$this_key', ".
                                          "value $data Not found in table '$ftable', field '$ftable_key'\n";
                        print $err $err_message;
                        $error_count++;
                    }
                }
            } # next $fkey_data
        }
    } # next $table
    return $error_count;
}

sub validate_record {
    # this function loops through each field in the record, determines
    # the type of field and then attempts to verify that the data in the
    # field does, in fact, match the data type supplied.
    # The program will die if the data type does not match
    my ( $database, $table, $columns, $csv ) = @_;
    my $data_type = $database->{ _data_type };
    my @fields = @{ $database->{$table}{_field_order} };
    foreach my $index ( 0 .. $#fields ) {
        my ( $type ) = ( $database->{ $table}{ $fields[ $index] } =~ /^(\w+)/ );
        my $function = $data_type->{ $type }{ 'validate' };
        if ( ref $function ne 'CODE' ) {
            # this should not happen
            die "No validation routine found for type: '$type'.";
        }
        my $type_match = $function->( $columns->[$index], $database->{ $table }{ $fields[$index] } );
        if ( ! $type_match ) {
            my $status = $csv->combine( @$columns );
            my $data   = $csv->string;
            my $err_message = "Field: '$columns->[ $index ]' does not match data type: '$database->{ $table }{ $fields[$index] }'.";
            error_in_data( $table, $data, $err_message );
        }
    }
}

sub validate_with_regexes {
    # every field in the database may have an optional regex added to
    # describe valid data types.  This sub will test whether or not the
    # data for those fields matches the regex.
    my ( $err, $dbh, $database ) = @_;
    my $error_count = 0;
    foreach my $table ( %$database ) {
        if ( exists $database->{ $table }{ _regex } ) {
            my %fields = %{$database->{ $table }{ _regex }};
            my @bad_regexes;
            while ( my ( $field, $regex ) = each %fields ) {
                if ( ! is_valid_pattern( $regex ) ) {
                    print $err "Table: '$table', field '$field' had invalid pattern '$regex'. Discarded.\n";
                    push @bad_regexes, $field;
                    $error_count++;
                }
            }
            delete @fields{ @bad_regexes } if @bad_regexes;
            my $fieldnames = join ',', keys %fields;
            my $sql = "SELECT $fieldnames FROM $table";

            my $sth = $dbh->prepare( $sql );
            $sth->execute;
            while ( my $data = $sth->fetchrow_hashref ) {
                foreach ( keys %$data ) {
                    my $value = $data->{ $_ };
                    my $regex = $fields{ $_ };
                    if ( $value !~ /$regex/ ) {
                        print $err "Table: '$table' field: '$_' value: '$value' did not match /$fields{$_}/.\n";
                        $error_count++;
                    }
                }
            }
        }
    } # next $table
    return $error_count;
}

###                   ###
#  utility subs follow  #
###                   ###

sub usage {
    print <<"    END_USAGE";

usage: db_validate.pl [-h] [-s schema_file] [-d database_dir] [-r raw_data]
                      [-v] [-n]
Validate and/or create a CSV database.
-h              help
-?              help
-s schema_file  File containing schema.  See POD for details
-d database_dir The directory where the database will be created.  If -v is
                supplied, this is location where data is to be read from, and
                then written back to.
-r raw_data     The file containing raw database information.  If -v is
                supplied, this is where the raw information will be written to.
-v              Validate existing database.
-n              If -v is used, this says that the data in -d does *not* have a
                header line labeling the fields.
    END_USAGE
}

sub error_in_data {
    my ( $table, $record, $message ) = @_;
    die "\nERROR in table: '$table'\n$message\nRecord: $record\n";
}

sub trim {
    my $data = shift;
    $data =~ s/^\s+//;
    $data =~ s/\s+$//;
    $data;
}

sub unique {
    my @array = @_;
    my %count;
    @array = grep { ! $count{$_}++ } @array;
    @array;
}

# each routine returns a true/false value for whether or not the data passes validation
sub is_int {
    my $data = shift;
    return $data =~ /^-?\d+$/ ? 1 : 0;
}

sub is_varchar {
    my ( $data, $desc ) = @_;
    # $desc will typically be something like VARCHAR(64)
    my ( $length ) = ( $desc =~ /\(\s*(\d+)/ );
    return ( length $data <= $length ) ? 1 : 0;
}

sub is_char {
    my ( $data, $desc ) = @_;
    # $desc will typically be something like CHAR(8)
    my ( $length ) = ( $desc =~ /\(\s*(\d+)/ );
    return ( length $data == $length ) ? 1 : 0;
}

sub is_valid_pattern {
    my $pattern = shift;
    return eval { '' =~ /$pattern/; 1 } || 0;
}

sub format_data_dir {
    my $dir = shift;
    # the following is stolen from CGI.pm
    my $os = $^O;
    unless ( defined $os and $os ) {
        require Config;
        $os = $Config::Config{'osname'};
    }
    my $sep;
    if ($os=~/Win/i) {
        $sep = '\\';
    } elsif ($os=~/vms/i) {
        $sep = '/';
    } elsif ($os=~/bsdos/i) {
        $sep = '/';
    } elsif ($os=~/dos/i) {
        $sep = '\\';
    } elsif ($os=~/^MacOS$/i) {
        $sep = ':';
    } elsif ($os=~/os2/i) {
        $sep = '\\';
    } else {
        $sep = '/';
    }
    $dir .= $sep if $dir !~ /$sep$/; # add the separator if it's not there
    return $dir;
}

__END__

=head1 NAME

db_validate.pl - Simple validation for DBD::CSV database creation

=head1 SYNOPSIS

 perl db_validate.pl -s schema.txt -r raw_data.txt -d data_dir

=head1 DESCRIPTION

DBD::CSV is a useful module if you are forced to work with CSV files as a
database.  Unfortunately, the module does no data validation.  This
program allows you to define a schema, along with UNIQUE fields and foreign
key constraints.  Further, basic data type validation for INT, CHAR, and
VARCHAR is supported.  Oddly enough, DBD::CSV, while only allowing those
datatypes to be used in a C<CREATE table> statement, does not actually check
to see whether or not the data you are inserting matches those datatypes.

If one supplies the -v option on the command line, then this program validates
and existing database.  The -r option is then used to specify where the
raw_data will be written to.  The -n ("no header") option may be used when
validating if you are validating a CSV file while does not have field names
listed as the first line of the file.

Much of the information provided in here is for those who wish to maintain and
extend this program.  These sections are marked with B<MAINTENANCE BEGIN> and
B<MAINTENANCE END> and may be skipped if you do not care about this.

=head1 Schema

Each database to be created and validated must have an associated schema
created by the programmer.

The schema location is passed to the program via the command line -s switch.
The schema is then read into the C<$schema> variable in C<&schema>. C<$schema>
is a hash ref.  Every key in the hash that does not begin with an underscore
(_) is assumed to be a table name.  Each table is also a hashref.  Every key
in a table hash that does not begin with an underscore is assumed to be a
field name.  Keys beginning with underscores are metadata describing their
respectives tables.  These keys are C<_field_order>, C<_foreign_key>,
C<_unique>, and C<_rec_count>.  These are used to validate the data in the
database.

The format of the schema file must be as follows (items enclosed in curly
braces are optional):

 [tablename]
 field DATATYPE {UNIQUE} {/regex/}
 _foreign_key tablename.field foreigntable foreigntable.key

Currently, the only datatypes supported are INT, CHAR, and VARCHAR.  CHAR and
VARCHAR must have a number in parenthese following them which specifies the
maximum number of characters allowed.  For example:

 [users]
 user_id   INT UNIQUE
 user_name VARCHAR(10)

In the schema, blank lines and lines starting with a sharp (#) are skipped.
Fields in the database will be in the order listed.  As soon as a new table
name is encountered, the previous table definition is assumed to be complete.

A regex may be supplied as the last item on the field definition line.  If a
field value does not match the regex, a warning will be written to the error
log.  For example to ensure that user ids are positive integers:

 [users]
 user_id INT UNIQUE /^\d+$/

The regex B<must> begin and end with a forward slash.  You can use a regex such
as C</.+/> to enforce a C<NOT NULL> requirement.

Also note that you may write C<INTEGER> instead of C<INT>, if you prefer.

=head2 Table Fields

Each field in a table may be of type INT, VARCHAR, or CHAR.  These are the only
datatypes supported by DBD::CSV when creating tables.  These datatypes are
stored in C<$int>, C<$varchar>, and C<$char>, respectively.  Let's examine the
following SQL C<CREATE> statement and see how it's translated:

 CREATE TABLE users (
    user_id INTEGER UNIQUE,
    name    VARCHAR(64),
    area    CHAR(8)
 )

B<MAINTENANCE BEGIN>

The following is the minimum legal specification for this table, as defined in
the C<$schema> variable.

 users => {
     user_id      => $int,
     name         => "$varchar(64)",
     area         => "$char(8)",
     _field_order => [qw/ user_id name area /]
 }

Upon validating the data, and exception will be thrown if C<user_id> does not
match the regular expression C</^-?\d+$/>.

The C<name> would throw an exception if it exceeded the length of 64 and
C<area> would throw an exception if the length did not match 8.  When the table
is created, the order of fields in the table would match the order specified in
C<_field_order>.

B<MAINTENANCE END>

This definition for this table would be created in the schema file as follows:

 [users]
 user_id INT
 name    VARCHAR(64)
 area    CHAR(8)

The

=head2 Metadata

As mentioned previously, there are four metadata keys used for tables.  These
are C<_field_order>, C<_foreign_key>, C<_unique>, and C<_rec_count>.

=over 4

=item _field_order

B<MAINTENANCE BEGIN>

Since hashes have an effectively arbitrary order for their keys, the
C<_field_order> key is used to specify the order of the fields when the tables
are created in the database.  This is an array reference in the C<$schema>
variable:

 _field_order => [qw/ user_id name area /]

If the tables fields and the field order do not match, an exception will be
thrown when the program attempts to create the table.

B<MAINTENANCE END>

=item _unique

C<DBD::CSV> does not actually allow for PRIMARY KEYS.  Rather than try to write
a bunch of arcane and effectively useless code to allow for this, we have a
C<UNIQUE> specifier.  Internally, this is merely an array reference specifying
the keys in the table for which we do not allow duplicate values.

B<MAINTENANCE BEGIN>

In the example of the C<users> table above, the table specification is as
follows:

 users => {
     user_id      => $int,
     name         => "$varchar(64)",
     area         => "$char(8)",
     _field_order => [qw/ user_id name area /],
     _unique      => [qw/ user_id /]
 }

B<MAINTENANCE END>

To make a user_id UNIQUE, we do the following:

 [users]
 user_id INT UNIQUE
 name    VARCHAR(64)
 area    CHAR(8)

After all data has been added to the database, the program will validate all
unique fields and an error will be written to an error log if any duplicate
values are found in these fields.

=item _foreign_key

Despite arguments by some diehard MySQL supporters :), any real database
supports foreign key constraints.  Naturally, DBD::CSV does not.  The
C<_foreign_key> value for a table is an array ref of array refs.  Each inner
array has the following format:

 field foreign_table foreign_field

After data is added to the database, foreign key validation will occur.  For
each table that has a C<_foreign_field> key, the program will look up the value
in C<field> and ensure that C<foreign_table> has a corresponding valud in
C<foreign_field>.  Further, the C<foreign_field> must be designated as a
C<_unique> field.  If these conditions are not met, an entry will be added to
the error log.

Consider a lookup table for a music CD database.  Each CD could potentially
have several artists and each artist could be present on several CDs.  Assuming
we have two tables named C<artists> and C<CDs>, with C<artist_id> and C<CD_id>
respectively, we could define the lookup table as follows:

B<MAINTENANCE BEGIN>

Internal representation

 cd_artist => {
     artist       => $int,
     cd           => $int,
     _field_order => [qw/ artist_id cd_id /],
     _foreign_key =>    [
         [ qw/ artist artists artist_id / ],
         [ qw/ cd CDs CD_id / ]
     ]
 }

B<MAINTENANCE END>

 [cd_artist]
 artist INT
 cd     INT
 _foreign_key artist artists artist_id
 _foreign_key cd CDs CD_id

Note that the tables will still be created if a foreign key constraint is
violated and there is nothing in the C<DBD::CSV> module to prevent this.  This
is for advisory purposes only.  Your database will function even if the foreign
key is not defined as C<UNIQUE>, but you should probably take a look at your
schema to look for problems.

=item _record_count

This is an internal field used to track how many records were written for each
table.  After data is added to the database, a list of the number of records
written to each table will be displayed to the screen.  A B<WARNING> message
will occur for all tables that have no records written.

=back

=head1 DATABASE CREATION

=head2 Creating a schema

Let's take a look at a simple database and see how it's all put together. We
have three tables: CDs, artist, and cd_artist.  The last is a lookup table.
What follows is not intended to be an complete database.

 [CDs]
 CD_id         INT /^\d++$/
 CD_name       VARCHAR(30) /.+/
 # date will be in YYYY
 year_released CHAR(4)

 [artist]
 artist_id     INT UNIQUE /^\d+$/
 artist_name   VARCHAR(30) /.+/

 [cd_artist]
 artist        INT
 cd            INT
 _foreign_key artist artist artist_id
 _foreign_key cd CDs CD_id

Note that when this schema is read that the lines are split on whitespace.
C<DBD::CSV> does not allow whitespace in a table name.

Further, I would not recommend the naming convention that I have used.  Foreign
key field names should (IMHO) match the field names of the tables that they
should match.  I have made them distinct so that it's easier to distinguish
them in this example.

=head2 Creating the raw data for the database

Now that we have our schema, we need to create the raw data.  The data file
contains all of the tables and data that will be added to the database.  Each
table is separated by "\n.\n".  The data for a table will be read in chunks
that is split on the separator.  I suppose that it's possible for someone to
have such a large amount of data that this could be a problem, but if you do,
you should be using a real database.  As with the schema file, lines composed
of all white space or beginning with a sharp will be discarded.  Fields must be
separated with a comma and if a field contains a comma, it must be quoted.

As each chunk is read, the program looks for the first line which contains data
and assumes this to be a table name.  You may use either of the two following
statements to declare a tablename:

 table=artist

or simply:

 artist

Here is a small raw data file:

 table=artist
 #artist_id, artist_name
 1,Ovid
 2,Yello
 .

 CDs
 # CD_id, CD_name,date_released
 1,One Second,1987
 2,"Baby",1991
 3,"Ovid's Greatest Hits, Volume Zero",0000
 .

 cd_artist
 # artist, cd
 1,3
 2,1
 4,2

Let's say we save the schema in a file called C<schema.txt> and the data in
a file called C<raw.txt> and we want to write this to a database called
C<stuff>.  We'd use the following command:

 perl db_validate.pl -s schema.txt -r raw.txt -d stuff

If a directory called C<stuff> does not exist, it will be created for us.
Issuing that command, we get the following:

 All tables successfully created.

 ERROR in table: 'CDs'
 Field: 'Ovid's Greatest Hits, Volume Zero' does not match data type: 'VARCHAR(30)'.
 Record: 3,"Ovid's Greatest Hits, Volume Zero",0000

Be examing our schema, we see that we have defined C<CD_name> as C<VARCHAR(30)>.
The field in question is over 30 characters long, so we change C<CD_name> in
the schema to C<VARCHAR(40)>.  Then we rerun the command.  We get the following
output:

 All tables successfully created.

 3 records added to table 'CDs'.
 2 records added to table 'artist'.
 3 records added to table 'cd_artist'.

 All data successfully added to database.
 Now validating unique fields.
 Now validating foreign key constraints.
 Now validating data against regular expressions.

 WARNING
 3 errors were found while validating the data.  Errors were written to 'err.log'

Hmm... what errors?  Opening the error.log, we see:

 Foreign Key constraint violated.  Table 'cd_artist', field 'artist', value '4' Not found in table 'artist', field 'artist_id'
 Foreign Key constraint violated.  Table 'cd_artist', field 'cd'. 'CD_id' not defined as UNIQUE in 'CDs'
 Table: 'CDs', field 'CD_id' had invalid pattern '^\d++$'. Discarded.

Oops.  Let's change those in our schema.

 CD_id         INT UNIQUE /^\d+$/

The regular expression, in this case, may seem superfluous, but it does two things for us:

 1.  It effectively ensures that we will have at least one digit in the id.
 2.  It ensures that all ids are positive.

Also, we see that there is no artist with an id of 4.  That should be changed to 2.
Rerunning the program this time creates all of the tables and generates absolutely
no errors.

=head1 Validating an existing database

If you have an existing CSV database, define a schema for the database.  Then,
run the program as normal using the C<-v> switch.  This tells the program that
we will be validating an existing database rather than creating a new one from
scratch.  If you wish to validate a CSV file which does not have the field
names as the first line (i.e., the first line of the file is also data), then
use the C<-n> option to tell the program that the CSV files has "no headers".

B<WARNING>: You want to backup the database or CSV file before doing this!
This program will copy the database or CSV file to the raw_data file and, if
all goes well, it will drop your tables (or CSV file) before proceeding.  Then
it will attempt to re-add the data to the files.  If it does not successfully
add the data, your data may very well be corrupted.

Also, the files or files created by validating B<will> have a header, as
C<DBD::CSV> automatically adds them, due to the way the module works.  If this
is a problem, you should remove them yourself.

When you have completed validating a database, the raw data fill will not be
unlinked.  I have left the file there if it's necessary to debug your
application.

I strongly recommend using this feature if you have created an application that
uses DBD::CSV.  Use your application, put it through its paces (with all the
tests you wrote, right?) and then use this application to test for data
corruption.

=head1 COPYRIGHT

Copyright (c) 2001 Curtis "Ovid" Poe.  All rights reserved.
This program is free software; you may redistribute it and/or modify it under
the same terms as Perl itself

=head1 AUTHOR

Curtis "Ovid" Poe <poec@yahoo.com>
Address bug reports and comments to: poec@yahoo.com.

=head1 BUGS

2001/11/10 There are no known bugs at this time.  However, I modified this pretty
heavily to get it "production ready".  Please let me know if there are any issues
with it.

=head1 SEE ALSO

L<DBD::CSV>, L<DBI>, L<Text::CSV_XS> and L<SQL::Statement>.

=cut

