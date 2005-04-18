##################################
package SQL::Statement::Functions;
##################################
=pod

=head1 NAME

SQL::Statement::Functions - built-in & user-defined SQL functions

=head1 SYNOPSIS

 SELECT Func(args);
 SELECT * FROM Func(args);
 SELECT * FROM x WHERE Funcs(args);
 SELECT * FROM x WHERE y < Funcs(args);

=head1 DESCRIPTION

This module contains the built-in functions for SQL::Parser and SQL::Statement.  All of the functions are also available in any DBDs that subclass those modules (e.g. DBD::CSV, DBD::DBM, DBD::File, DBD::AnyData, DBD::Excel, etc.).

This documentation covers built-in functions and also explains how to create your own functions to supplement the built-in ones.  It's easy!  If you create one that is generally useful, see below for how to submit it to become a built-in function.

=head1 Function syntax

When using SQL::Statement/SQL::Parser directly to parse SQL, functions (either built-in or user-defined) may occur anywhere in a SQL statement that values, column names, table names, or predicates may occur.  When using the modules through a DBD or in any other context in which the SQL is both parsed and executed, functions can occur in the same places except that they can not occur in the column selection clause of a SELECT statement that contains a FROM clause.

 # valid for both parsing and executing

     SELECT MyFunc(args);
     SELECT * FROM MyFunc(args);
     SELECT * FROM x WHERE MyFuncs(args);
     SELECT * FROM x WHERE y < MyFuncs(args);

 # valid only for parsing (won't work from a DBD)

     SELECT MyFunc(args) FROM x WHERE y;

=head1 User-Defined Functions

=head2 Loading User-Defined Functions

In addition to the built-in functions, you can create any number of your own user-defined functions (UDFs).  In order to use a UDF in a script, you first have to create a perl subroutine (see below), then you need to make the function available to your database handle with the CREATE FUNCTION or LOAD commands:

 # load a single function "foo"from a subroutine 
 # named "foo" in the current package

      $dbh->do(" CREATE FUNCTION foo EXTERNAL ");

 # load a single function "foo" from a subroutine
 # named "bar" in the current package

      $dbh->do(" CREATE FUNCTION foo EXTERNAL NAME bar");


 # load a single function "foo" from a subroutine named "foo"
 # in another package

      $dbh->do(' CREATE FUNCTION foo EXTERNAL NAME "Bar::Baz::foo" ');

 # load all the functions in another package

      $dbh->do(' LOAD "Bar::Baz" ');

Functions themselves should follow SQL identifier naming rules.  Subroutines loaded with CREATE FUNCTION can have any valied perl subrourinte name.  Subroutines loaded with LOAD must start with SQL_FUNCTION_ and then the actual function name.  For example:

 package Qux::Quimble;
 sub SQL_FUNCTION_FOO { ... }
 sub SQL_FUNCTION_BAR { ... }
 sub some_other_perl_subroutine_not_a_function { ... }
 1;

 # in another package
 $dbh->do("LOAD Qux::Quimble");

 # This loads FOO and BAR as SQL functions.

=head2 Creating User-Defined Functions

User-defined functions (UDFs) are perl subroutines that return values appropriate to the context of the function in a SQL statement.  For example the built-in CURRENT_TIME returns a string value and therefore may beused anywhere in a SQL statement that a string value can.  Here' the entire perl code for the function:

 # CURRENT_TIME
 #
 # arguments : none
 # returns   : string containing current time as hh::mm::ss
 #
 sub SQL_FUNCTION_CURRENT_TIME {
     sprintf "%02s::%02s::%02s",(localtime)[2,1,0]
 }

More complex functions can make use of a number of arguments always passed to functions automatically.  Functions always recieve these values in @_:

 sub FOO {
     my($self,$sth,$rowhash,@params);
 }

The first argument, $self, is whatever class the function is defined in, not generally useful unless you have an entire module to support the function.

The second argument, $sth is the active statement handle of the current statement.  Like all active statement handles it contains the current database handle in the {Database} attribute so you can have access to the database handle in any function:

 sub FOO {
     my($self,$sth,$rowhash,@params);
     my $dbh = $sth->{Database};
     # $dbh->do( ...), etc.
 }

In actual practice you probably want to use $sth-{Database} directly rather than making a local copy, so $sth->{Database}->do(...).

The third argument, $rowhash, is a reference to a hash containing the key/value pairs for the current database row the SQL is searching.  This isn't relevant for something like CURRENT_TIME which isn't based on a SQL search, but here's an example of a (rather useless) UDF using $rowhash that just joins the values for the entire row with a colon:

 sub COLON_JOIN {
     my($self,$sth,$rowhash,@params);
     my $str = join ':', values %$rowhash;
 }

The remaining arguments, @params, are aguements passed by users to the function, either directly or with placeholders; another silly example which just returns the results of multiplying the arguments passed to it:

 sub MULTIPLY {
     my($self,$sth,$rowhash,@params);
     return $params[0] * $params[1];
 }

 # first make the function available
 #
 $dbh->do("CREATE FUNCTION MULTIPLY");

 # then multiply col3 in each row times seven
 #
 my $sth=$dbh->prepare("SELECT col1 FROM tbl1 WHERE col2 = MULTIPLY(col3,7)");
 $sth->execute;
 #
 # or
 #
 my $sth=$dbh->prepare("SELECT col1 FROM tbl1 WHERE col2 = MULTIPLY(col3,?)");
 $sth->execute(7);

=head2 Creating In-Memory Tables with functions

A function can return almost anything, as long is it is an appropriate return for the context the function will be used in.  In the special case of table-returning functions, the function should return a reference to an array of array references with the first row being the column names and the remaining rows the data.  For example:

B<1. create a function that returns an AoA>,

  sub Japh {[
      [qw( id word   )],
      [qw( 1 Hacker  )],
      [qw( 2 Perl    )],
      [qw( 3 Another )],
      [qw( 4 Just    )],
  ]}

B<2. make your database handle aware of the function>

  $dbh->do("CREATE FUNCTION 'Japh');

B<3. Access the data in the AoA from SQL>

  $sth = $dbh->prepare("SELECT word FROM Japh ORDER BY id DESC");

Or here's an example that does a join on two in-memory tables:

  sub Prof  {[ [qw(pid pname)],[qw(1 Sue )],[qw(2 Bob)],[qw(3 Tom )] ]}
  sub Class {[ [qw(pid cname)],[qw(1 Chem)],[qw(2 Bio)],[qw(2 Math)] ]}
  $dbh->do("CREATE FUNCTION $_) for qw(Prof Class);
  $sth = $dbh->prepare("SELECT * FROM Prof NATURAL JOIN Class");

The "Prof" and "Class" functions return tables which can be used like any SQL table.

More complex functions might do something like scrape an RSS feed, or search a file system and put the results in AoA.  For example, to search a directory with SQL:

 sub Dir {
     my($self,$sth,$rowhash,$dir)=@_;
     opendir D, $dir or die "'$dir':$!";
     my @files = readdir D;
     my $data = [[qw(fileName fileExt)]];
     for (@files) {
         my($fn,$ext) = /^(.*)(\.[^\.]+)$/;
         push @$data, [$fn,$ext];
     }
     return $data;
 }
 $dbh->do("CREATE FUNCTION Dir");
 printf "%s\n", join'   ',@{ $dbh->selectcol_arrayref("
     SELECT fileName FROM Dir('./') WHERE fileExt = '.pl'
 ")};

Obviously, that function could be expanded with File::Find and/or stat to provide more information and it could be made to accept a list of directories rather than a single directory.

Table-Returning functions are a way to turn *anything* that can be modeled as an AoA into a DBI data source.

=head1 Built-in Functions

=cut

use vars qw($VERSION);
$VERSION = '0.1';

=pod

=head2 Aggregate Functions

=head3 min, max, avg, sum, count

Aggregate functions are handled elsewhere, see L<SQL::Parser> for documentation.

=pod

=head2 Date and Time Functions

=head3 current_date, current_time, current_timestamp


B<CURRENT_DATE>

 # purpose   : find current date
 # arguments : none
 # returns   : string containing current date as yyyy-mm-dd

=cut
sub SQL_FUNCTION_CURRENT_DATE {
    my($sec,$min,$hour,$day,$mon,$year) = localtime;
    sprintf "%4s-%02s-%02s", $year+1900,$mon+1,$day;
}

=pod

B<CURRENT_TIME>

 # purpose   : find current time
 # arguments : none
 # returns   : string containing current time as hh::mm::ss

=cut
sub SQL_FUNCTION_CURRENT_TIME {
    sprintf "%02s::%02s::%02s",(localtime)[2,1,0]
}

=pod

B<CURRENT_TIMESTAMP>

 # purpose   : find current date and time
 # arguments : none
 # returns   : string containing current timestamp as yyyy-mm-dd hh::mm::ss

=cut
sub SQL_FUNCTION_CURRENT_TIMESTAMP {
    my($sec,$min,$hour,$day,$mon,$year) = localtime;
    sprintf "%4s-%02s-%02s %02s::%02s::%02s",
            $year+1900,$mon+1,$day,$hour,$min,$sec;
}

=pod

=head2 String Functions

=head3 char_length, lower, position, regex, soundex, substring, trim, upper

B<CHAR_LENGTH>

 # purpose   : find length in characters of a string
 # arguments : a string
 # returns   : a number - the length of the string in characters

=cut
sub SQL_FUNCTION_CHAR_LENGTH {
    my($self,$sth,$rowhash,@params) = @_;
    return length $params[0];
}

=pod

B<LOWER & UPPER>

 # purpose   : lower-case or upper-case a string
 # arguments : a string
 # returns   : the sting lower or upper cased

=cut
sub SQL_FUNCTION_LOWER {
    my($self,$sth,$rowhash,$str) = @_;
    return "\L$str";
}
sub SQL_FUNCTION_UPPER {
    my($self,$sth,$rowhash,$str) = @_;
    return "\U$str";
}

=pod

B<POSITION>

 # purpose   : find first position of a substring in a string
 # arguments : a substring and  a string possibly containing the substring
 # returns   : a number - the index of the substring in the string
 #             or 0 if the substring doesn't occur in the sring

=cut
sub SQL_FUNCTION_POSITION {
    my($self,$sth,$rowhash,@params) = @_;
    return index($params[1],$params[0]) +1;
}

=pod

B<REGEX>

 # purpose   : test if a string matches a perl regular expression
 # arguments : a string and a regex to match the string against
 # returns   : boolean value of the regex match
 #
 # example   : ... WHERE REGEX(col3,'/^fun/i') ... matches rows
 #             in which col3 starts with "fun", ignoring case

=cut
sub SQL_FUNCTION_REGEX {
    my($self,$sth,$rowhash,@params)=@_;
    return 0 unless defined $params[0] and defined $params[1];
    my($pattern,$modifier) = $params[1] =~ m~^/(.+)/([a-z]*)$~;
    $pattern = "(?$modifier:$pattern)" if $modifier;
    return ($params[0] =~ qr($pattern)) ? 1 : 0;
}

=pod

B<SOUNDEX>

 # purpose   : test if two strings have matching soundex codes
 # arguments : two strings
 # returns   : true if the strings share the same soundex code
 #
 # example   : ... WHERE SOUNDEX(col3,'fun') ... matches rows
 #             in which col3 is a soundex match for "fun"

=cut
sub SQL_FUNCTION_SOUNDEX {
    my($self,$sth,$rowhash,@params)=@_;
    require Text::Soundex;
    my $s1 = Text::Soundex::soundex($params[0]) or return 0;
    my $s2 = Text::Soundex::soundex($params[1]) or return 0;
    return ($s1 eq $s2) ? 1 : 0;
}

=pod

B<CONCAT>

 # purpose   : concatenate 1 or more strings into a single string;
 #			an alternative to the '||' operator
 # arguments : 1 or more strings
 # returns   : the concatenated string
 #
 # example   : SELECT CONCAT(first_string, 'this string', ' that string')
 #              returns "<value-of-first-string>this string that string"
 # note      : if any argument evaluates to NULL, the returned value is NULL

=cut
sub SQL_FUNCTION_CONCAT {
    my($self,$sth,$rowhash,@params)=@_;

	my $str = '';
	foreach (@params) {
		return undef unless defined($_);
		$str .= $_;
	}
    return $str;
}

=pod

B<COALESCE> I<aka> B<NVL>

 # purpose   : return the first non-NULL value from a list
 # arguments : 1 or more expressions
 # returns   : the first expression (reading left to right)
 #             which is not NULL; returns NULL if all are NULL
 #
 # example   : SELECT COALESCE(NULL, some_null_column, 'not null')
 #              returns 'not null'

=cut
sub SQL_FUNCTION_COALESCE {
	my ($obj, $sth, $rowhash, @params) = @_;
#
#	eval each expr in list until a non-null
#	is encountered, then return it
#
	foreach (@params) {
		return $_
			if defined($_);
	}
	return undef;
}

sub SQL_FUNCTION_NVL { return SQL_FUNCTION_COALESCE(@_); }

=pod

B<DECODE>

 # purpose   : compare the first argument against 
 #             succeding arguments at position 1 + 2N
 #             (N = 0 to (# of arguments - 2)/2), and if equal,
 #				return the value of the argument at 1 + 2N + 1; if no
 #             arguments are equal, the last argument value is returned
 # arguments : 4 or more expressions, must be even # of arguments
 # returns   : the value of the argument at 1 + 2N + 1 if argument 1 + 2N
 #             is equal to argument1; else the last argument value
 #
 # example   : SELECT DECODE(some_column, 
 #                    'first value', 'first value matched'
 #                    '2nd value', '2nd value matched'
 #                    'no value matched'
 #                    )

=cut
#
#	emulate Oracle DECODE; behaves same as
#	CASE expr WHEN <expr2> THEN expr3
#	WHEN expr4 THEN expr5
#	...
#	ELSE exprN END
#
sub SQL_FUNCTION_DECODE {
	my ($obj, $sth, $rowhash, @params) = @_;
#
#	check param list size, must be at least 4,
#	and even in length
#
	return $obj->do_err('Invalid DECODE argument list!')
		unless ((scalar @params > 3) && ($#params & 1 == 1));
#
#	eval first argument, and last argument,
#	then eval and compare each succeeding pair of args
#	be careful about NULLs!
#
	my $lhs = shift @params;
	my $default = pop @params;
	return $default unless defined($lhs);
	my $lhs_isnum = is_number($lhs);

	while (@params) {
		my $rhs = shift @params;
		shift @params,
		next
			unless defined($rhs);
		return shift @params
			if ((is_number($rhs) && $lhs_isnum && ($lhs == $rhs)) ||
				($lhs eq $rhs));
		shift @params;
	}
	return $default;
}

sub is_number {
	my $v = shift;
    return ($v=~/^([+-]?|\s+)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/);
}

=pod

B<REPLACE>, B<SUBSTITUTE>

 # purpose   : perform perl subsitution on input string
 # arguments : a string and a substitute pattern string
 # returns   : the result of the substitute operation
 #
 # example   : ... WHERE REPLACE(col3,'s/fun(\w+)nier/$1/ig') ... replaces
 #			all instances of /fun(\w+)nier/ in col3 with the string
 #			between 'fun' and 'nier'

=cut
sub SQL_FUNCTION_REPLACE {
    my($self,$sth,$rowhash,@params)=@_;
    return undef unless defined $params[0] and defined $params[1];

	eval "\$params[0]=~$params[1]";
    return $@ ? undef : $params[0];
}

sub SQL_FUNCTION_SUBSTITUTE { return SQL_FUNCTION_REPLACE(@_); }


sub SQL_FUNCTION_SUBSTR {
    my($self,$sth,$rowhash,@params)=@_;
    my $string = $params[0] || '';
    my $start  = $params[1] || 0;
    my $offset = $params[2] || length $string;
    my $value = '';
       $value = substr($string,$start-1,$offset)
       if length $string >= $start-2+$offset;
}

=pod

B<SUBSTRING>

  SUBSTRING( string FROM start_pos [FOR length] )

Returns the substring starting at start_pos and extending for
"length" character or until the end of the string, if no
"length" is supplied.  Examples:

  SUBSTRING( 'foobar' FROM 4 )       # returns "bar"

  SUBSTRING( 'foobar' FROM 4 FOR 2)  # returns "ba"

Note: The SUBSTRING function is implemented in SQL::Parser and SQL::Statement and, at the current time, can not be over-ridden.

B<TRIM>

  TRIM ( [ [LEADING|TRAILING|BOTH] ['trim_char'] FROM ] string )

Removes all occurrences of <trim_char> from the front, back, or
both sides of a string.

 BOTH is the default if neither LEADING nor TRAILING is specified.

 Space is the default if no trim_char is specified.

 Examples:

 TRIM( string )
   trims leading and trailing spaces from string

 TRIM( LEADING FROM str )
   trims leading spaces from string

 TRIM( 'x' FROM str )
   trims leading and trailing x's from string

Note: The TRIM function is implemented in SQL::Parser and SQL::Statement and, at the current time, can not be over-ridden.

=head1 Special Utility Functions

=head2 IMPORT()

 CREATE TABLE foo AS IMPORT(?)    ,{},$external_executed_sth
 CREATE TABLE foo AS IMPORT(?)    ,{},$AoA

=cut

sub SQL_FUNCTION_IMPORT {
    my($self,$sth,$rowhash,@params)=@_;
    if (ref $params[0] eq 'ARRAY') {
        my $type =  ref $params[0]->[0];
        return $params[0] unless $type and $type eq 'HASH';
        my @tbl=();
        for my $row(@{$params[0]}) {
            my @cols = sort keys %$row;
            push @tbl, \@cols unless @tbl;
            push @tbl, [@$row{@cols}];
        }
        return \@tbl;
    }
    my $tmp_sth = $params[0];
#   my @cols = map{$_->name} $tmp_sth->{f_stmt}->columns if $tmp_sth->{f_stmt};
   my @cols;
    @cols = @{ $tmp_sth->{NAME} } unless @cols;
#    push @{$sth->{org_names}},$_ for @cols;
    my $tbl  = [ \@cols ];
    while (my @row=$tmp_sth->fetchrow_array) {
        push @$tbl, \@row;
    }
    return $tbl;
}

# RUN()
#
# takes the name of a file containing SQL statements, runs the statements
# see SQL::Parser for details
#
sub SQL_FUNCTION_RUN {
    my($self,$sth,$rowhash,$file)=@_;
    my @params = $sth->{f_stmt}->params;
       @params = () unless @params;
    local *IN;
    open(IN,'<',$file) or die "Couldn't open SQL File '$file': $!\n";
    my @stmts = split /;\s*\n+/,join'',<IN>;
    $stmts[-1] =~ s/;\s*$//;
    close IN;
    my @results = ();
    for my $sql(@stmts) {
        my $tmp_sth = $sth->{Database}->prepare($sql);
        $tmp_sth->execute(@params);
        next unless $tmp_sth->{NUM_OF_FIELDS};
        push @results, $tmp_sth->{NAME} unless @results;
        while (my @r=$tmp_sth->fetchrow_array) { push @results, \@r }
    }
    #use Data::Dumper; print Dumper \@results and exit if @results;
    return \@results;
}

=pod

=head1 Submitting built-in functions

There are a few built-in functions in the SQL::Statement::Functions.  If you make a generally useful UDF, why not submit it to me and have it (and your name) included with the built-in functions?  Please follow the format shown in the module including a description of the arguments and return values for the function as well as an example.  Send them to me at jzucker AT cpan.org with a subject line containing "built-in UDF".

Thanks in advance :-).

=head1 ACKNOWLEDGEMENTS

Dean Arnold supplied DECODE, COALESCE, REPLACE, many thanks!

=head1 AUTHOR & COPYRIGHT

This module is copyright (c) 2005 by Jeff Zucker. All rights reserved.

The module may be freely distributed under the same terms as
Perl itself using either the "GPL License" or the "Artistic
License" as specified in the Perl README file.

Jeff can be reached at: jzucker AT cpan.org

=cut
1;
