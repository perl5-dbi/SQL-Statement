######################################################################
package SQL::Parser;
######################################################################
#
# This module is copyright (c), 2001,2005 by Jeff Zucker.
# All rights resered.
#
# It may be freely distributed under the same terms as Perl itself.
# See below for help and copyright information (search for SYNOPSIS).
#
######################################################################

use strict;
use warnings;
use vars qw($VERSION);
use constant FUNCTION_NAMES => join '|', qw( TRIM SUBSTRING );

$VERSION = '1.13';

BEGIN { if( $ENV{SQL_USER_DEFS} ) { require SQL::UserDefs; } }
eval { require 'Data/Dumper.pm'; $Data::Dumper::Indent=1};
*bug = ($@) ? sub {warn @_} : sub { print Data::Dumper::Dumper(\@_) };

#############################
# PUBLIC METHODS
#############################

sub new {
    my $class   = shift;
    my $dialect = shift || 'ANSI';
    $dialect = 'ANSI'    if uc $dialect eq 'ANSI';
    $dialect = 'AnyData' if uc $dialect eq 'ANYDATA' or uc $dialect eq 'CSV';
#    $dialect = 'CSV'     if uc $dialect eq 'CSV';
    if ($dialect eq 'SQL::Eval') {
       $dialect = 'AnyData';
    }
    my $flags  = shift || {};
    $flags->{"dialect"}      = $dialect;
    $flags->{"PrintError"}   = 1 unless defined $flags->{"PrintError"};
    my $self = bless_me($class,$flags);
    $self->dialect( $self->{"dialect"} );
    $self->set_feature_flags($self->{"select"},$self->{"create"});
    bless $self,$class;
    $self->LOAD("LOAD SQL::Statement::Functions");
    return $self;
}

sub parse {
    my $self = shift;
    my $sql = shift;
    $self->dialect( $self->{"dialect"} )  unless $self->{"dialect_set"};
    $sql =~ s/^\s+//;
    $sql =~ s/\s+$//;
    $self->{"struct"} = {};
    $self->{"tmp"} = {};
    $self->{"original_string"} = $sql;
    $self->{struct}->{"original_string"} = $sql;

    ################################################################
    #
    # COMMENTS

    # C-STYLE
    #
    my $comment_re = $self->{"comment_re"} || '(\/\*.*?\*\/)';
    $self->{"comment_re"} = $comment_re;
    my $starts_with_comment;
    if ($sql =~ /^\s*$comment_re(.*)$/s) {
       $self->{"comment"} = $1;
       $sql = $2;
       $starts_with_comment=1;
    }
    # SQL STYLE
    #
    if ($sql =~ /^\s*--(.*)(\n|$)/) {
       $self->{"comment"} = $1;
       return 1;
    }
    ################################################################

    $sql = $self->clean_sql($sql);
    my($com) = $sql =~ /^\s*(\S+)\s+/s ;
    if (!$com) {
        return 1 if $starts_with_comment;
        return $self->do_err("Incomplete statement!");
    }
    $com = uc $com;
    $self->{"opts"}->{"valid_commands"}->{CALL}=1;
    $self->{"opts"}->{"valid_commands"}->{LOAD}=1;
    if ($self->{"opts"}->{"valid_commands"}->{$com}) {
        my $rv = $self->$com($sql);
        delete $self->{"struct"}->{"literals"};
#        return $self->do_err("No table names found!")
#               unless $self->{"struct"}->{"table_names"};
        return $self->do_err("No command found!")
               unless $self->{"struct"}->{"command"};
        if ( $self->{"struct"}->{join}
         and scalar keys %{$self->{"struct"}->{join}}==0
         ) {
            delete $self->{"struct"}->{join};
	}
        $self->replace_quoted_ids();
	for (@{$self->{struct}->{table_names}}) {
            push @{$self->{struct}->{org_table_names}},$_;
	}
#
# UPPER CASE TABLE NAMES
#
my @uTables = map {uc $_ } @{$self->{struct}->{table_names}};
#
# REMOVE schema.table infor if present
#
   @uTables = map { s/^.*\.([^\.]+)$/$1/;$_} @uTables;
$self->{struct}->{table_names} = \@uTables unless $com eq 'CREATE';
	if ($self->{struct}->{column_names}) {
	for (@{$self->{struct}->{column_names}}) {
                 my $cn = $_;
                 $cn = uc $cn unless $cn =~ /^"/;
            push @{$self->{struct}->{org_col_names}},
                 $self->{struct}->{ORG_NAME}->{$cn};
	}
	}
$self->{struct}->{join}->{table_order}
    = $self->{struct}->{table_names}
   if $self->{struct}->{join}->{table_order}
  and scalar(@{$self->{struct}->{join}->{table_order}}) == 0;
@{$self->{struct}->{join}->{keycols}}
     = map {uc $_ } @{$self->{struct}->{join}->{keycols}}
    if $self->{struct}->{join}->{keycols};
@{$self->{struct}->{join}->{shared_cols}}
    = map {uc $_ } @{$self->{struct}->{join}->{shared_cols}}
    if $self->{struct}->{join}->{shared_cols};
##
#  For RR aliases, added quoted id protection from upper casing
my @uCols = map { ($_=~/^"/)?$_:uc $_} @{$self->{struct}->{column_names}};
##
$self->{struct}->{column_names} = \@uCols unless $com eq 'CREATE';
	if ($self->{original_string} =~ /Y\.\*/) {
#use mylibs; zwarn $self; exit;
	}
        delete $self->{struct}->{join}
               if $self->{struct}->{join}
              and scalar keys %{$self->{struct}->{join}}==0;

        undef $self->{struct}->{set_function}
        unless $self->{struct}->{has_set_functions};
        return $rv;
    } 
    else {
       $self->{struct}={};
       if ($ENV{SQL_USER_DEFS}) {
           return SQL::UserDefs::user_parse($self,$sql);
       }
       return $self->do_err("Command '$com' not recognized or not supported!");
    }
}

sub replace_quoted_ids {
    my $self = shift;
    my $id = shift;
    return $id unless $self->{struct}->{quoted_ids};
    if ($id) {
      if ($id =~ /^\?QI(\d+)\?$/) {
        return '"'.$self->{struct}->{quoted_ids}->[$1].'"';
      } 
      else {
	return $id;
      }
    }
    return unless defined $self->{struct}->{table_names};
    my @tables = @{$self->{struct}->{table_names}};
    for my $t(@tables) {
        if ($t =~ /^\?QI(.+)\?$/ ) {
            $t = '"'.$self->{struct}->{quoted_ids}->[$1].'"';
#            $t = $self->{struct}->{quoted_ids}->[$1];
        }
    }
    $self->{struct}->{table_names} = \@tables;
    delete $self->{struct}->{quoted_ids};
}


sub structure { shift->{"struct"} }
sub command { my $x = shift->{"struct"}->{command} || '' }

sub feature {
    my($self,$opt_class,$opt_name,$opt_value) = @_;
    if (defined $opt_value) {
        if ( $opt_class eq 'select' ) {
            $self->set_feature_flags( {"join"=>$opt_value} );
        }
        elsif ( $opt_class eq 'create' ) {
            $self->set_feature_flags( undef, {$opt_name=>$opt_value} );
        }
        else {
          # patch from chromatic
          $self->{"opts"}->{$opt_class}->{$opt_name} = $opt_value;
	  # $self->{$opt_class}->{$opt_name} = $opt_value;
	} 
    }
    else {
        return $self->{"opts"}->{$opt_class}->{$opt_name};
    }
}

sub errstr  { shift->{"struct"}->{"errstr"} }

sub list {
    my $self = shift;
    my $com  = uc shift;
    return () if $com !~ /COMMANDS|RESERVED|TYPES|OPS|OPTIONS|DIALECTS/i;
    $com = 'valid_commands' if $com eq 'COMMANDS';
    $com = 'valid_comparison_operators' if $com eq 'OPS';
    $com = 'valid_data_types' if $com eq 'TYPES';
    $com = 'valid_options' if $com eq 'OPTIONS';
    $com = 'reserved_words' if $com eq 'RESERVED';
    $self->dialect( $self->{"dialect"} ) unless $self->{"dialect_set"};

    return sort keys %{ $self->{"opts"}->{$com} } unless $com eq 'DIALECTS';
    my $dDir = "SQL/Dialects";
    my @dialects;
    for my $dir(@INC) {
      local *D;

      if ( opendir(D,"$dir/$dDir")  ) {
          @dialects = grep /.*\.pm$/, readdir(D);
          last;
      } 
    }
    @dialects = map { s/\.pm$//; $_} @dialects;
    return @dialects;
}

sub dialect {
    my($self,$dialect) = @_;
    return $self->{"dialect"} unless $dialect;
    return $self->{"dialect"} if $self->{dialect_set};
    $self->{"opts"} = {};
    my $mod = "SQL/Dialects/$dialect.pm";
    undef $@;
    eval {
        require "$mod";
    };
    return $self->do_err($@) if $@;
    $mod =~ s/\.pm//;
    $mod =~ s"/"::"g;
    my @data = split /\n/, $mod->get_config;
    my $feature;
    for (@data) {
        chomp;
        s/^\s+//;
        s/\s+$//;
        next unless $_;
        if (/^\[(.*)\]$/i) {
            $feature = lc $1;
            $feature =~ s/\s+/_/g;
            next;
        }
        my $newopt = uc $_;
        $newopt =~ s/\s+/ /g;
        $self->{"opts"}->{$feature}->{$newopt} = 1;
    }
    $self->create_op_regexen();
    $self->{"dialect"} = $dialect;
    $self->{"dialect_set"}++;
}

sub create_op_regexen {
    my($self)=@_;
#
#	DAA precompute the predicate operator regex's
#
#       JZ moved this into a sub so it can be called from both
#       dialect() and from CREATE_OPERATOR and DROP_OPERATOR
#       since those also modify the available operators
#
    my @allops = keys %{ $self->{"opts"}->{"valid_comparison_operators"} };
#
#	complement operators
#
    my @notops;
    for (@allops) { 
    	push (@notops, $_) 
    		if /NOT/i;
    }
    $self->{"opts"}->{"valid_comparison_NOT_ops_regex"} = 
    	'^\s*(.+)\s+('. join('|', @notops) . ')\s+(.*)\s*$'
    	if scalar @notops;
#
#	<>, <=, >= operators
#
	my @compops;
	for (@allops) { 
		push (@compops, $_) 
			if /<=|>=|<>/;
	}
	$self->{"opts"}->{"valid_comparison_twochar_ops_regex"} = 
		'^\s*(.+)\s+(' . join('|', @compops) . ')\s+(.*)\s*$'
		if scalar @compops;
#
#	everything
#
	$self->{"opts"}->{"valid_comparison_ops_regex"} = 
		'^\s*(.+)\s+(' . join('|', @allops) . ')\s+(.*)\s*$'
		if scalar @allops;
#
#	end DAA
#
}

##################################################################
# SQL COMMANDS
##################################################################

####################################################
# DROP TABLE <table_name>
####################################################
sub DROP {
    my $self = shift;
    my $stmt = shift;
    my $features = 'TYPE|KEYWORD|FUNCTION|OPERATOR|PREDICATE';
    if ($stmt =~ /^\s*DROP\s+($features)\s+(.+)$/si ) {
        my($sub,$arg) = ($1,$2);
        $sub = 'DROP_' . $sub;
        return $self->$sub($arg);
    }
    my $table_name;
    $self->{"struct"}->{"command"}     = 'DROP';
    if ($stmt =~ /^\s*DROP\s+TABLE\s+IF\s+EXISTS\s+(.*)$/si ) {
        $stmt = "DROP TABLE $1";
        $self->{"struct"}->{ignore_missing_table}=1;
    }
    if ($stmt =~ /^\s*DROP\s+(\S+)\s+(.+)$/si ) {
       my $com2    = $1 || '';
       $table_name = $2;
       if ($com2 !~ /^TABLE$/i) {
          return $self->do_err(
              "The command 'DROP $com2' is not recognized or not supported!"
          );
      }
      $table_name =~ s/^\s+//;
      $table_name =~ s/\s+$//;
      if ( $table_name =~ /(\S+) (RESTRICT|CASCADE)/i) {
          $table_name = $1;
          $self->{"struct"}->{"drop_behavior"} = uc $2;
      }
    }
    else {
        return $self->do_err( "Incomplete DROP statement!" );

    }
    return undef unless $self->TABLE_NAME($table_name);
    $table_name = $self->replace_quoted_ids($table_name);
    $self->{"tmp"}->{"is_table_name"}  = {$table_name => 1};
    $self->{"struct"}->{"table_names"} = [$table_name];
    return 1;
}

####################################################
# DELETE FROM <table_name> WHERE <search_condition>
####################################################
sub DELETE {
    my($self,$str) = @_;
    $self->{"struct"}->{"command"}     = 'DELETE';
    $str =~ s/^DELETE\s+FROM\s+/DELETE /i; # Make FROM optional
    my($table_name,$where_clause) = $str =~
        /^DELETE (\S+)(.*)$/i;
    return $self->do_err(
        'Incomplete DELETE statement!'
    ) if !$table_name;
    return undef unless $self->TABLE_NAME($table_name);
    $self->{"tmp"}->{"is_table_name"}  = {$table_name => 1};
    $self->{"struct"}->{"table_names"} = [$table_name];
    $self->{"struct"}->{"column_names"} = ['*'];
    $where_clause =~ s/^\s+//;
    $where_clause =~ s/\s+$//;
    if ($where_clause) {
        $where_clause =~ s/^WHERE\s*(.*)$/$1/i;
        return undef unless $self->SEARCH_CONDITION($where_clause);
    }
    return 1;
}

##############################################################
# SELECT
##############################################################
#    SELECT [<set_quantifier>] <select_list>
#           | <set_function_specification>
#      FROM <from_clause>
#    [WHERE <search_condition>]
# [ORDER BY <order_by_clause>]
#    [LIMIT <limit_clause>]
##############################################################

sub SELECT {
    my($self,$str) = @_;
    $self->{"struct"}->{"command"} = 'SELECT';
    my($from_clause,$where_clause,$order_clause,$groupby_clause,$limit_clause);
    $str =~ s/^SELECT (.+)$/$1/i;
    if ( $str =~ s/^(.+) LIMIT (.+)$/$1/i    ) { $limit_clause = $2; }
    if ( $str =~ s/^(.+) ORDER BY (.+)$/$1/i ) { $order_clause = $2; }
    if ( $str =~ s/^(.+) GROUP BY (.+)$/$1/i ) { $groupby_clause = $2; }
    if ( $str =~ s/^(.+?) WHERE (.+)$/$1/i   ) { $where_clause = $2; }
    if ( $str =~ s/^(.+?) FROM (.+)$/$1/i    ) { $from_clause  = $2; }

#    else {
#        return $self->do_err("Couldn't find FROM clause in SELECT!");
#    }
#    return undef unless $self->FROM_CLAUSE($from_clause);
    my $has_from_clause = $self->FROM_CLAUSE($from_clause) if $from_clause;

    return undef unless $self->SELECT_CLAUSE($str);

    if ($where_clause) {
        return undef unless $self->SEARCH_CONDITION($where_clause);
    }
    if ($groupby_clause) {
        return undef unless $self->GROUPBY_LIST($groupby_clause);
    }
    if ($order_clause) {
        return undef unless $self->SORT_SPEC_LIST($order_clause);
    }
    if ($limit_clause) {
        return undef unless $self->LIMIT_CLAUSE($limit_clause);
    }
    if ( ( $self->{"struct"}->{join}->{"clause"}
           and $self->{"struct"}->{join}->{"clause"} eq 'ON'
         )
      or ( $self->{"struct"}->{"multiple_tables"}
            and !(scalar keys %{$self->{"struct"}->{join}})
       ) ) {
           return undef unless $self->IMPLICIT_JOIN();
    }
    return 1;
}
sub GROUPBY_LIST {
    my($self,$gclause) = @_;
    return 1 if !$gclause;
    my @cols = split /,/,$gclause;
    $self->{struct}->{group_by} = \@cols;
    return 1;
}
sub IMPLICIT_JOIN {
    my $self = shift;
    delete $self->{"struct"}->{"multiple_tables"};
    if ( !$self->{"struct"}->{join}->{"clause"}
           or $self->{"struct"}->{join}->{"clause"} ne 'ON'
    ) {
        $self->{"struct"}->{join}->{"type"}    = 'INNER';
        $self->{"struct"}->{join}->{"clause"}  = 'IMPLICIT';
    }
    if (defined $self->{"struct"}->{"keycols"} ) {
        my @keys;
        my @keys2 = @keys = @{ $self->{"struct"}->{"keycols"} };
        $self->{"struct"}->{join}->{"table_order"} = $self->order_joins(\@keys2);
        @{$self->{"struct"}->{join}->{"keycols"}} = @keys;
        delete $self->{"struct"}->{"keycols"};
    }
    else {
        return $self->do_err("No equijoin condition in WHERE or ON clause");
    }
    return 1;
}

sub EXPLICIT_JOIN {
    my $self = shift;
    my $remainder = shift;
    return undef unless $remainder;
    my($tableA,$tableB,$keycols,$jtype,$natural);
    if ($remainder =~ /^(.+?) (NATURAL|INNER|LEFT|RIGHT|FULL|UNION|JOIN)(.+)$/s){
        $tableA = $1;
        $remainder = $2.$3;
    }
    else {
        ($tableA,$remainder) = $remainder =~ /^(\S+) (.*)/;
    }
        if ( $remainder =~ /^NATURAL (.+)/) {
            $self->{"struct"}->{join}->{"clause"} = 'NATURAL';
            $natural++;
            $remainder = $1;
        }
        if ( $remainder =~ 
           /^(INNER|LEFT|RIGHT|FULL|UNION) JOIN (.+)/
        ) {
          $jtype = $self->{"struct"}->{join}->{"clause"} = $1;
          $remainder = $2;
          $jtype = "$jtype OUTER" if $jtype !~ /INNER|UNION/;
      }
        if ( $remainder =~ 
           /^(LEFT|RIGHT|FULL) OUTER JOIN (.+)/
        ) {
          $jtype = $self->{"struct"}->{join}->{"clause"} = $1 . " OUTER";
          $remainder = $2;
      }
      if ( $remainder =~ /^JOIN (.+)/) {
          $jtype = 'INNER';
          $self->{"struct"}->{join}->{"clause"} = 'DEFAULT INNER';
          $remainder = $1;
      }
      if ( $self->{"struct"}->{join} ) {
          if ( $remainder && $remainder =~ /^(.+?) USING \(([^\)]+)\)(.*)/) {
              $self->{"struct"}->{join}->{"clause"} = 'USING';
              $tableB = $1;
              my $keycolstr = $2;
              $remainder = $3;
              @$keycols = split /,/,$keycolstr;
          }
          if ( $remainder && $remainder =~ /^(.+?) ON (.+)/) {
              $self->{"struct"}->{join}->{"clause"} = 'ON';
              $tableB = $1;
              my $keycolstr = $2;
              $remainder = $3;
              if ($keycolstr =~ / OR /i ) {
                  return $self->do_err(qq~Can't use OR in an ON clause!~,1);
	      }
              @$keycols = split / AND /i,$keycolstr;

return undef unless $self->TABLE_NAME_LIST($tableA.','.$tableB);
#              $self->{"tmp"}->{"is_table_name"}->{"$tableA"} = 1;
#              $self->{"tmp"}->{"is_table_name"}->{"$tableB"} = 1;
              for (@$keycols) {
                  my %is_done;
                  my($arg1,$arg2) = split / = /;
                  my($c1,$c2)=($arg1,$arg2);
                  $c1 =~ s/^.*\.([^\.]+)$/$1/;
                  $c2 =~ s/^.*\.([^\.]+)$/$1/;
                  if ($c1 eq $c2) {
                      return undef unless $arg1 = $self->ROW_VALUE($c1);
                      if ( $arg1->{type} eq 'column' and !$is_done{$c1}
                      ){
                          push @{$self->{struct}->{keycols}},$arg1->{value};
                          $is_done{$c1}=1;
 	              }
                  }
                  else {
                      return undef unless $arg1 = $self->ROW_VALUE($arg1);
                      return undef unless $arg2 = $self->ROW_VALUE($arg2);
                      if ( $arg1->{"type"}eq 'column'
                      and $arg2->{"type"}eq 'column'){
                          push @{ $self->{"struct"}->{"keycols"} }
                              , $arg1->{"value"};
                           push @{ $self->{"struct"}->{"keycols"} }
                              , $arg2->{"value"};
                           # delete $self->{"struct"}->{"where_clause"};
	              }
                  }
              }
          }
          elsif ($remainder =~ /^(.+?)$/i) {
  	      $tableB = $1;
              $remainder = $2;
          }
          $remainder =~ s/^\s+// if $remainder;
      }

      if ($jtype) {
          $jtype = "NATURAL $jtype" if $natural;
          if ($natural and $keycols) {
              return $self->do_err(
                  qq~Can't use NATURAL with a USING or ON clause!~
              );
	  }
          return undef unless $self->TABLE_NAME_LIST("$tableA,$tableB");
          $self->{"struct"}->{join}->{"type"}    = $jtype;
          $self->{"struct"}->{join}->{"keycols"} = $keycols if $keycols;
          return 1;
      }
      return $self->do_err("Couldn't parse explicit JOIN!");
}

sub SELECT_CLAUSE {
    my($self,$str) = @_;
    return undef unless $str;
    if ($str =~ s/^(DISTINCT|ALL) (.+)$/$2/i) {
        $self->{"struct"}->{"set_quantifier"} = uc $1;
    }
    if ($str =~ /[()]/) {
        #return undef unless $self->SET_FUNCTION_SPEC($str);
#        $self->SET_FUNCTION_SPEC($str);
    }
#    else {
        return undef unless $self->SELECT_LIST($str);
#    }
}

sub FROM_CLAUSE {
    my($self,$str) = @_;
    return undef unless $str;
    if ($str =~ / JOIN /i ) {
        return undef unless $self->EXPLICIT_JOIN($str);
    }
    else {
        return undef unless $self->TABLE_NAME_LIST($str);
    }
}

sub INSERT {
    my($self,$str) = @_;
    my $col_str;
    $str =~ s/^INSERT\s+INTO\s+/INSERT /i; # allow INTO to be optional
    my($table_name,$val_str) = $str =~
        /^INSERT\s+(.+?)\s+VALUES\s+\((.+?)\)$/i;
    if ($table_name and $table_name =~ /[()]/ ) {
    ($table_name,$col_str,$val_str) = $str =~
        /^INSERT\s+(.+?)\s+\((.+?)\)\s+VALUES\s+\((.+?)\)$/i;
    }
    return $self->do_err('No table name specified!') unless $table_name;
    return $self->do_err('Missing values list!') unless defined $val_str;
    return undef unless $self->TABLE_NAME($table_name);
    $self->{"struct"}->{"command"} = 'INSERT';
    $self->{"struct"}->{"table_names"} = [$table_name];
    if ($col_str) {
        return undef unless $self->COLUMN_NAME_LIST($col_str);
    }
    else {
          $self->{"struct"}->{"column_names"} = ['*'];
    }
    return undef unless $self->LITERAL_LIST($val_str);
    return 1;
}

###################################################################
# UPDATE ::=
#
# UPDATE <table> SET <set_clause_list> [ WHERE <search_condition>]
#
###################################################################
sub UPDATE {
    my($self,$str) = @_;
    $self->{"struct"}->{"command"} = 'UPDATE';
    my($table_name,$remainder) = $str =~
        /^UPDATE (.+?) SET (.+)$/i;
    return $self->do_err(
        'Incomplete UPDATE clause'
    ) if !$table_name or !$remainder;
    return undef unless $self->TABLE_NAME($table_name);
    $self->{"tmp"}->{"is_table_name"}  = {$table_name => 1};
    $self->{"struct"}->{"table_names"} = [$table_name];
    my($set_clause,$where_clause) = $remainder =~
        /(.*?) WHERE (.*)$/i;
    $set_clause = $remainder if !$set_clause;
    return undef unless $self->SET_CLAUSE_LIST($set_clause);
    if ($where_clause) {
        return undef unless $self->SEARCH_CONDITION($where_clause);
    }
    my @vals = @{$self->{"struct"}->{"values"}};
    my $num_val_placeholders=0;
    for my $v(@vals) {
       $num_val_placeholders++ if $v->{"type"} eq 'placeholder';
    }
    $self->{"struct"}->{"num_val_placeholders"}=$num_val_placeholders;
    return 1;
}

############
# FUNCTIONS
############
sub LOAD {
    my($self,$str) = @_;
    $self->{"struct"}->{"command"} = 'LOAD';
    $self->{"struct"}->{"no_execute"} = 1;
    my($package) = $str =~ /^LOAD\s+(.+)$/;
    $str = $package;
    $package =~ s/\?(\d+)\?/$self->{"struct"}->{"literals"}->[$1]/g;
    my $mod = $package . '.pm';
    $mod =~ s~::~/~g;
    eval { require $mod; };
    die "Couldn't load '$package': $@\n" if $@;
    my %subs = eval '%'.$package.'::';
    for my $sub ( keys %subs ){
        next unless $sub =~ /^SQL_FUNCTION_([A-Z_0-9]+)$/;
        my $funcName = uc $1;
        $self->{opts}->{function_names}->{$funcName}=1;
        $self->{opts}->{function_defs}->{$funcName}->{sub} = {
            value => $package.'::'.'SQL_FUNCTION_'.$funcName ,
            type => 'string'
        };
    }
    return 1;
}

sub CREATE_RAM_TABLE {
    my $self = shift;
    my $stmt = shift;
    $self->{"struct"}->{"is_ram_table"} = 1;
    $self->{"struct"}->{"command"} = 'CREATE_RAM_TABLE';
    my($table_name,$table_element_def,%is_col_name);
    if ($stmt =~ /^(\S+)\s+LIKE\s*(.+)$/si ) {
        $table_name        = $1;
        $table_element_def = $2;
        if ($table_element_def =~ /^(.*)\s+KEEP CONNECTION\s*$/i) {
            $table_element_def = $1;
            $self->{struct}->{ram_table_keep_connection}=1;
	}
    }
    else {
        return $self->CREATE("CREATE TABLE $stmt");
    }
    return undef unless $self->TABLE_NAME($table_name);
    for my $col(split ',',$table_element_def) {
        push @{$self->{"struct"}->{"column_names"}},$self->ROW_VALUE($col);
    }
    $self->{"struct"}->{"table_names"} = [$table_name];
    return 1;
}
sub CREATE_FUNCTION {
    my $self = shift;
    my $stmt = shift;
    $self->{"struct"}->{"command"} = 'CREATE_FUNCTION';
    $self->{"struct"}->{"no_execute"} = 1;
    my($func,$subname);
    $stmt =~ s/\s*EXTERNAL//i;
    if( $stmt =~ /^(\S+)\s+NAME\s+(.*)$/smi) {
        $func    = trim($1);
        $subname = trim($2);
    }
    $func    ||= $stmt;
    $subname ||= $func;
    if ($func =~ /^\?QI(\d+)\?$/) {
        $func = $self->{struct}->{quoted_ids}->[$1];
    }
    if ($subname =~ /^\?QI(\d+)\?$/) {
        $subname = $self->{struct}->{quoted_ids}->[$1];
    }
    $self->{opts}->{function_names}->{uc $func}=1;
    $self->{opts}->{function_defs}->{uc $func}->{sub}
        = {value=>$subname,type=>'string'};
    return 1;
}
sub CALL {
    my $self = shift;
    my $stmt = shift;
    $stmt =~ s/^CALL\s+(.*)/$1/i;
    $self->{"struct"}->{"command"} = 'CALL';
    $self->{"struct"}->{"procedure"} = $self->ROW_VALUE($stmt);
    return 1;
}
sub CREATE_TYPE {
    my($self,$type)=@_;
    $self->{"struct"}->{"command"} = 'CREATE_TYPE';
    $self->{"struct"}->{"no_execute"} = 1;
    $self->feature('valid_data_types',uc $type,1);
}
sub DROP_TYPE {
    my($self,$type)=@_;
    $self->{"struct"}->{"command"} = 'DROP_TYPE';
    $self->{"struct"}->{"no_execute"} = 1;
    $self->feature('valid_data_types',uc $type,0);
}
sub CREATE_KEYWORD {
    my($self,$type)=@_;
    $self->{"struct"}->{"command"} = 'CREATE_KEYWORD';
    $self->{"struct"}->{"no_execute"} = 1;
    $self->feature('reserved_words',uc $type,1);
}
sub DROP_KEYWORD {
    my($self,$type)=@_;
    $self->{"struct"}->{"command"} = 'DROP_KEYWORD';
    $self->{"struct"}->{"no_execute"} = 1;
    $self->feature('reserved_words',uc $type,0);
}
sub CREATE_OPERATOR {
    my($self,$stmt)=@_;
    $self->{"struct"}->{"command"} = 'CREATE_OPERATOR';
    $self->{"struct"}->{"no_execute"} = 1;

    my($func,$subname);
    $stmt =~ s/\s*EXTERNAL//i;
    if( $stmt =~ /^(\S+)\s+NAME\s+(.*)$/smi) {
        $func    = trim($1);
        $subname = trim($2);
    }
    $func    ||= $stmt;
    $subname ||= $func;
    if ($func =~ /^\?QI(\d+)\?$/) {
        $func = $self->{struct}->{quoted_ids}->[$1];
    }
    if ($subname =~ /^\?QI(\d+)\?$/) {
        $subname = $self->{struct}->{quoted_ids}->[$1];
    }
    $self->{opts}->{function_names}->{uc $func}=1;
    $self->{opts}->{function_defs}->{uc $func}->{sub}
        = {value=>$subname,type=>'string'};

    $self->feature('valid_comparison_operators',uc $func,1);
    $self->create_op_regexen();

}
sub DROP_OPERATOR {
    my($self,$type)=@_;
    $self->{"struct"}->{"command"} = 'DROP_OPERATOR';
    $self->{"struct"}->{"no_execute"} = 1;
    $self->feature('valid_comparison_operators',uc $type,0);
    $self->create_op_regexen();
}

#########
# CREATE
#########
sub CREATE {
    my $self = shift;
    my $stmt = shift;
    my $features = 'TYPE|KEYWORD|FUNCTION|OPERATOR|PREDICATE';
    if ($stmt =~ /^\s*CREATE\s+($features)\s+(.+)$/si ) {
        my($sub,$arg) = ($1,$2);
        $sub = 'CREATE_' . uc $sub;
        return $self->$sub($arg);
    }
#    if ($stmt =~ /^\s*CREATE\s+FUNCTION (.+)$/si ) {
#        return $self->CREATE_FUNCTION($1);
#    }
#    if ($stmt =~ /^\s*CREATE\s+TYPE\s+(.+)$/si ) {
#        return $self->CREATE_TYPE($1);
#    }
    $stmt =~ s/^CREATE (LOCAL|GLOBAL) /CREATE /si;
    if ($stmt =~ /^\s*CREATE\s+(TEMP|TEMPORARY)\s+TABLE\s+(.+)$/si ) {
        $stmt = "CREATE TABLE $2";
        $self->{"struct"}->{"is_ram_table"} = 1;
        #  $self->{"struct"}->{"command"} = 'CREATE_RAM_TABLE';
        # return $self->CREATE_RAM_TABLE($1);
    }
    $self->{"struct"}->{"command"} = 'CREATE';
    my($table_name,$table_element_def,%is_col_name);
    # if ($stmt =~ /^CREATE (LOCAL|GLOBAL) TEMPORARY TABLE(.*)$/si ) {
    #    $self->{"struct"}->{"table_type"} = "$1 TEMPORARY";
    #    $stmt = "CREATE TABLE$2";
    # }
    if ($stmt =~ /^(.*) ON COMMIT (DELETE|PRESERVE) ROWS\s*$/si ) {
        $stmt = $1;
        $self->{"struct"}->{"commit_behaviour"} = $2;
#        return $self->do_err(
#           "Can't specify commit behaviour for permanent tables."
#        )
#           if !defined $self->{"struct"}->{"table_type"}
#              or $self->{"struct"}->{"table_type"} !~ /TEMPORARY/;
    }
    if ($stmt =~ /^CREATE TABLE (\S+) \((.*)\)$/si ) {
       $table_name        = $1;
       $table_element_def = $2;
    } 
    elsif ($stmt =~ /^CREATE TABLE (\S+) AS (.*)$/si) {
        $table_name  = $1;
        my $subquery = $2;
        return undef unless $self->TABLE_NAME($table_name);
        $self->{"struct"}->{"table_names"} = [$table_name];
        $self->{"struct"}->{"subquery"} = $subquery;
        return 1;
    }
    else {
        return $self->do_err( "Can't find column definitions!" );
    }
    return undef unless $self->TABLE_NAME($table_name);
    $table_element_def =~ s/\s+\(/(/g;
    my $primary_defined;
    for my $col(split ',',$table_element_def) {
        my($name,$type,$constraints)=($col =~/\s*(\S+)\s+(\S+)\s*(.*)/);
        if (!$type) {
            return $self->do_err( "Column definition is missing a data type!" );
	}
        return undef if !($self->IDENTIFIER($name));
#        if ($name =~ /^\?QI(.+)\?$/ ) {
            $name = $self->replace_quoted_ids($name);
#        }
        $constraints =~ s/^\s+//;
        $constraints =~ s/\s+$//;
        if ($constraints) {
           $constraints =~ s/PRIMARY KEY/PRIMARY_KEY/i;
           $constraints =~ s/NOT NULL/NOT_NULL/i;
           my @c = split /\s+/, $constraints;
           my %has_c;
           for my $constr(@c) {
   	       if ( $constr =~ /^\s*(UNIQUE|NOT_NULL|PRIMARY_KEY)\s*$/i ) {
                   my $cur_c = uc $1;
                   if ($has_c{$cur_c}++) {
  		       return $self->do_err(
                           qq~Duplicate column constraint: '$constr'!~
                       );
		   }
                   if ($cur_c eq 'PRIMARY_KEY' and $primary_defined++ ) {
  		       return $self->do_err(
                           qq~Can't have two PRIMARY KEYs in a table!~
                        );
		   }
                   $constr =~ s/_/ /g;
                   push @{$self->{"struct"}->{"column_defs"}->{"$name"}->{"constraints"} }, $constr;

	       }
               else {
		   return $self->do_err("Unknown column constraint: '$constr'!");
	       }
	   }
	}
        $type = uc $type;
        my $length;
        if ( $type =~ /(.+)\((.+)\)/ ) {
            $type = $1;
            $length = $2;
	}
        if (!$self->{"opts"}->{"valid_data_types"}->{"$type"}) {
            return $self->do_err("'$type' is not a recognized data type!");
	}
        $self->{"struct"}->{"column_defs"}->{"$name"}->{"data_type"} = $type;
        $self->{"struct"}->{"column_defs"}->{"$name"}->{"data_length"} = $length;
        push @{$self->{"struct"}->{"column_names"}},$name;
        #push @{$self->{"struct"}->{ORG_NAME}},$name;
        my $tmpname = $name;
        $tmpname = uc $tmpname unless $tmpname =~ /^"/;
        return $self->do_err("Duplicate column names!") 
          if $is_col_name{$tmpname}++;

    } 
    $self->{"struct"}->{"table_names"} = [$table_name];
    return 1;
}


###############
# SQL SUBRULES
###############

sub SET_CLAUSE_LIST {
    my $self       = shift;
    my $set_string = shift;
    my @sets = split /,/,$set_string;
    my(@cols,@vals);
    for(@sets) {
        my($col,$val) = split / = /,$_;
        return $self->do_err('Incomplete SET clause!') if !defined $col or !defined $val;
        push @cols, $col;
        push @vals, $val;
    }
    return undef unless $self->COLUMN_NAME_LIST(join ',',@cols);
    return undef unless $self->LITERAL_LIST(join ',',@vals);
    return 1;
}

sub SET_QUANTIFIER {
    my($self,$str) = @_;
    if ($str =~ /^(DISTINCT|ALL)\s+(.*)$/si) {
        $self->{"struct"}->{"set_quantifier"} = uc $1;
        $str = $2;
    }
    return $str;
}

#
#	DAA v1.11
#	modify to transform || strings into
#	CONCAT(<expr>); note that we
#	only xform the topmost expressions;
#	if a concat is contained within a subfunction,
#	it should get handled by ROW_VALUE()
#
sub transform_concat {
	my ($obj, $colstr) = @_;
	
	pos($colstr) = 0;
	my $parens = 0;
	my $spos = 0;
	my @concats = ();
	my $alias = ($colstr=~s/^(.+)(\s+AS\s+\S+)$/$1/) ? $2 : '';

	while ($colstr=~/\G.*?([\(\)\|])/gcs) {
		if ($1 eq '(') {
			$parens++; 
		}
		elsif ($1 eq ')') {
			$parens--; 
		}
		elsif ((! $parens) && 
			(substr($colstr, $-[1] + 1, 1) eq '|')) {
#
# its a concat outside of parens, push prior string on stack
#
			push @concats, substr($colstr, $spos, $-[1] - $spos);
			$spos = $+[1] + 1;
			pos($colstr) = $spos;
		}
	}
#
#	no concats, return original
#
	return $colstr unless scalar @concats;
#
#	don't forget the last one!
#
	push @concats, substr($colstr, $spos);
	return 'CONCAT(' . join(', ', @concats) . ")$alias";
}
#
#	DAA v1.10
#	improved column list extraction
#	original doesn't seem to handle
#	commas within function argument lists
#
#	DAA v1.11
#	modify to transform || strings into
#	CONCAT(<expr-list>)
#
sub extract_column_list {
	my ($self, $colstr) = @_;
	
	my @collist = ();
	pos($colstr) = 0;
	my $parens = 0;
	my $spos = 0;
	while ($colstr=~/\G.*?([\(\),])/gcs) {
		if ($1 eq '(') {
			$parens++; 
		}
		elsif ($1 eq ')') {
			$parens--; 
		}
		elsif (! $parens) {	# its a comma outside of parens
			push @collist, substr($colstr, $spos, $-[1] - $spos);
			$collist[-1]=~s/^\s+//;
			$collist[-1]=~s/\s+$//;
			return $self->do_err('Bad column list!')
				if ($collist[-1] eq '');
			$spos = $+[1];
		}
	}
	return $self->do_err('Unbalanced parentheses!')
		if $parens;
#
#	don't forget the last one!
#
	push @collist, substr($colstr, $spos);
	$collist[-1]=~s/^\s+//;
	$collist[-1]=~s/\s+$//;
	return $self->do_err('Bad column list!')
		if ($collist[-1] eq '');
#
#	scan for and convert string concats to CONCAT()
#
	foreach (0..$#collist) {
		$collist[$_] = $self->transform_concat($collist[$_])
			if ($collist[$_]=~/\|\|/);
	}

	return @collist;
}

sub SELECT_LIST {
    my $self = shift;
    my $col_str = shift;
    if ( $col_str =~ /^\s*\*\s*$/ ) {
        $self->{"struct"}->{"column_names"} = ['*'];
        return 1;
    }
    my @col_list = $self->extract_column_list($col_str);
    return undef unless scalar @col_list;

    my(@newcols,$newcol,%aliases,$newalias);
    for my $col (@col_list) {
#	DAA
#	need better alias test here, since AS is a common
#	keyword that might be used in a function
#
        my ($fld, $alias) = ($col=~/^(.+)\s+AS\s+([A-Z]\w*)$/i)
                          ? ($1, $2)
                          : ($col, undef);
        $col = $fld;
        if ($col =~ /^(\S+)\.\*$/) {
        	my $table = $1;
        	my %is_table_alias = %{$self->{"tmp"}->{"is_table_alias"}};
        	$table = $is_table_alias{$table} if $is_table_alias{$table};
        	$table = $is_table_alias{"\L$table"} if $is_table_alias{"\L$table"};
            return undef unless $self->TABLE_NAME($table);
            $table = $self->replace_quoted_ids($table);
            push @newcols, "$table.*";
        }
        else {
            #
            # SELECT_LIST COLUMN IS A COMPUTED COLUMN WITH A SET FUNCTION
            #
	    $newcol = $self->SET_FUNCTION_SPEC($col);
            #
            # SELECT_LIST COLUMN IS A COMPUTED COLUMN WITH A NON-SET FUNCTION
            #
	    if (!$newcol) {
                my $func_obj = $self->ROW_VALUE($col);
                if ( ref($func_obj) =~ /::Function$/ 
or (ref($func_obj)eq 'HASH'and $func_obj->{type}and $func_obj->{type}eq'function')
){
#                    die "Functions in the SELECT LIST must have an alias!\n"
#                        unless defined $alias;
                    $alias ||= $func_obj->{name};
                    $newcol = uc $alias;
                    $self->{struct}->{col_obj}->{$newcol}
                         = SQL::Statement::Util::Column->new(
                               uc $alias,[],$alias,$func_obj
                           );
                }

                #
                # SELECT_LIST COLUMN IS NOT A COMPUTED COLUMN
                #
                else {
                    return undef unless $newcol = $self->COLUMN_NAME($col);
        	}
    	    }
            $newalias = $self->COLUMN_NAME($alias||$newcol);
            $self->{struct}->{ORG_NAME}->{$newcol} = $newalias;
            $aliases{uc $newalias} = $newcol;
            push @newcols, $newcol;
            if (!$alias) {
                $alias = $fld;
                $alias =~ s/^.*\.([^\.]+)$/$1/;
	    }
            if (!$self->{struct}->{col_obj}->{$newcol}) {
                    $self->{struct}->{col_obj}->{uc $newcol}
                         = SQL::Statement::Util::Column->new(
                               uc $newcol,[],$alias
                           );

	    }
        }
    }
    $self->{"struct"}->{"column_aliases"} = \%aliases;
    $self->{"struct"}->{"column_names"} = \@newcols;
    return 1;
}

sub SET_FUNCTION_SPEC {
    my($self,$col_str) = @_;

    my @funcs = split /,/, $col_str;
    my %iscol;
    for my $func(@funcs) {
        if ($func =~ /^(COUNT|AVG|SUM|MAX|MIN) \((.*)\)\s*$/i ) {
            my $set_function_name = uc $1;
            my $set_function_arg  = $2;
            my $distinct;
            if ( $set_function_arg =~ s/(DISTINCT|ALL) (.+)$/$2/i ) {
                $distinct = uc $1;
                $self->{"struct"}->{"set_quantifier"} = $distinct;
			} 
            my $count_star = 1 if $set_function_name eq 'COUNT'
                              and $set_function_arg eq '*';

            my $ok = $self->COLUMN_NAME($set_function_arg)
				if !$count_star;

            return undef 
            	if !$count_star and !$ok;


			if ($set_function_arg !~ /^"/) {
                $set_function_arg = uc $set_function_arg;
			} 

            $self->{struct}->{has_set_functions}=1;

            push @{ $self->{"struct"}->{'set_function'}}, {
                name     => $set_function_name,
                arg      => $set_function_arg,
                distinct => $distinct,
            };
            return $set_function_arg
                 if !$iscol{$set_function_arg}++
        }
        else {
            push @{ $self->{"struct"}->{'set_function'}}, {name => $func};
            return undef;
            # return $self->do_err("Bad set function before FROM clause.");
		}
    }
}
sub LIMIT_CLAUSE {
    my($self,$limit_clause) = @_;
#    $limit_clause = trim($limit_clause);
    $limit_clause =~ s/^\s+//;
    $limit_clause =~ s/\s+$//;

    return 1 if !$limit_clause;
    my($offset,$limit,$junk) = split /,/, $limit_clause;
    return $self->do_err('Bad limit clause!')
         if (defined $limit and $limit =~ /[^\d]/)
         or ( defined $offset and $offset =~ /[^\d]/ )
         or defined $junk;
    if (defined $offset and !defined $limit) {
        $limit = $offset;
        undef $offset;
    }
    $self->{"struct"}->{"limit_clause"} = {
        limit  => $limit,
        offset => $offset,
     };
     return 1;
}

sub is_number {
    my $x=shift;
    return 0 if !defined $x;
    return 1 if $x =~ /^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/;
    return 0;
}

sub SORT_SPEC_LIST {
        my($self,$order_clause) = @_;
        return 1 if !$order_clause;
        my %is_table_name = %{$self->{"tmp"}->{"is_table_name"}};
        my %is_table_alias = %{$self->{"tmp"}->{"is_table_alias"}};
        my @ocols;
        my @order_columns = split ',',$order_clause;
        for my $col(@order_columns) {
            my $newcol;
            my $newarg;
	    if ($col =~ /\s*(\S+)\s+(ASC|DESC)/si ) {
                $newcol = $1;
                $newarg = uc $2;
	    }
	    elsif ($col =~ /^\s*(\S+)\s*$/si ) {
                $newcol = $1;
            }
            else {
	      return $self->do_err(
                 'Junk after column name in ORDER BY clause!'
              );
	    }
            return undef if !($newcol = $self->COLUMN_NAME($newcol));
            if ($newcol =~ /^(.+)\..+$/s ) {
              my $table = $1;
              if ($table =~ /^'/) {
	          if (!$is_table_name{"$table"} and !$is_table_alias{"$table"} ) {
                return $self->do_err( "Table '$table' in ORDER BY clause "
                             . "not in FROM clause."
                             );
	      }}
	      elsif (!$is_table_name{"\L$table"} and !$is_table_alias{"\L$table"} ) {
                return $self->do_err( "Table '$table' in ORDER BY clause "
                             . "not in FROM clause."
                             );
	      }
	    }
            push @ocols, {$newcol => $newarg};
	}
        $self->{"struct"}->{"sort_spec_list"} = \@ocols;
        return 1;
}

sub SEARCH_CONDITION {
    my $self = shift;
    my $str  = shift;
    $str =~ s/^\s*WHERE (.+)/$1/;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    return $self->do_err("Couldn't find WHERE clause!") unless $str;
#
#	DAA
#	make these OO so subclasses can override them
#
    $str = $self->get_btwn( $str );
    $str = $self->get_in( $str );
#
#	DAA
#	add another abstract method so subclasses
#	can inject their own syntax transforms
#
	$str = $self->transform_syntax( $str );

    my $open_parens  = $str =~ tr/\(//;
    my $close_parens = $str =~ tr/\)//;
    if ($open_parens != $close_parens) {
        return $self->do_err("Mismatched parentheses in WHERE clause!");
    }
    $str = nongroup_numeric( $self->nongroup_string( $str ) );
    my $pred = $open_parens
        ? $self->parens_search($str,[])
        : $self->non_parens_search($str,[]);
    return $self->do_err("Couldn't find predicate!") unless $pred;
    $self->{"struct"}->{"where_clause"} = $pred;
    return 1;
}

############################################################
# UTILITY FUNCTIONS CALLED TO PARSE PARENS IN WHERE CLAUSE
############################################################

# get BETWEEN clause
#
#	DAA
#	rewrite to remove recursion and optimize code
#
sub get_btwn {
	my $self = shift;	# DAA make OO for subclassing
    my $str = shift;
    while ($str =~ /^(.+?)\b(NOT\s+)?BETWEEN (.+)$/i ) {
        my $front = $1;
        my $back  = $3;
        my $not = $2 ? 1 : undef;
#
#	scan the front piece to determine where
#	it really starts (esp wrt parens)		
#
		my $col = ($front=~s/^(.+\b(AND|NOT|OR))\b(.+)$/$1/i) ? $3 : $front;
		$front = '' 
			if ($col eq $front);
#
#	check on the number of parens
#	we've got
#
		my $parens = 0;
		$parens += ($1 eq '(') ? 1 : -1
			while ($col=~/\G.*?([\(\)])/gcs);

		return $self->do_err("Unmatched right parentheses!") 
			if ($parens < 0);
#
#	trim leading parens if any
#
		pos($col) = 0;
		while ($parens && ($col=~/\G.*?([\(\)])/gcs)) {
			return $self->do_err("Unmatched right parentheses!") 
				if ($1 eq ')');

			$parens--;
		}
		
		$front .= substr($col, 0, pos($col));
		$col = substr($col, pos($col));

		return $self->do_err("Incomplete BETWEEN predicate!") 
			unless ($back =~ s/^(.+?) AND (.+)$/$2/i);
        my $val1 = $1;

        my $val2 = ($back =~ s/^(.+?)( (AND|OR).+)$/$2/i) ? $1 : $back;
        $back = '' 
        	if ($val2 eq $back);
#
#	look for closing parens to match any remaining open
#	parens
#
		if ($parens) {
			$parens += ($1 eq '(') ? 1 : -1
				while ($parens && ($val2=~/\G.*?([\(\)])/gcs));
			$back = substr($val2, pos($val2)) . $back; 
			$val2 = substr($val2, 0, pos($val2));
		}
		elsif ($val2=~/\G.*?([\(\)])/gcs) {
			$parens += ($1 eq '(') ? 1 : -1
				while (($parens >= 0) && ($val2=~/\G.*?([\(\)])/gcs));
			$back = substr($val2, pos($val2)) . $back; 
			$val2 = substr($val2, 0, pos($val2));
		}

        $str = $not ?
        	"$front ($col <= $val1 OR $col >= $val2) $back" :
        	"$front ($col > $val1 AND $col < $val2) $back";
    }
    return $str;
}
# get IN clause
#
#  a IN (b,c)     -> (a=b OR a=c)
#  a NOT IN (b,c) -> (a<>b AND a<>c)
#
sub get_in {
	my $self = shift;	# DAA make OO for subclassing
    my $str = shift;
    my $in_inside_parens = 0;
#
#	DAA optimize regex
#	and fix to properly track parens
#
    while ($str =~ /^(.+?)\b(NOT\s+)?IN \((.+)$/i ) {
        my($col, $contents);
        my $front = $1;
        my $back  = $3;
        my $not = $2 ? 1 : 0;
#
#	scan the front piece to determine where
#	it really starts (esp wrt parens)		
#
		my $pos = ($front=~/^.+\b(AND|NOT|OR)\b(.+)$/igcs) ? $-[2] : 0;
		pos($front) = $pos; 	# reset
#
#	this can be an arbitrary expression,
#	so scan for balanced parens
#
		$in_inside_parens += ($1 eq '(') ? 1 : -1
			while ($front=~/\G.*?([\(\)])/gcs);

		return $self->do_err("Unmatched right parentheses during IN processing!") 
			if ($in_inside_parens < 0);
#
#	reset scanner so we can find the true beginning
#	of the expression
#
		pos($front) = $pos;
		$in_inside_parens--,
		$pos = $+[0]
			while ($in_inside_parens && ($front=~/\G.*?\(/gcs));
#
#	we've isolated the left expression
#
		$col = substr($front, $pos);
		$front = substr($front, 0, $pos);
#
#	now isolate the right expression list
#
		$in_inside_parens = 1;	# for the opening paren

		$in_inside_parens += ($1 eq '(') ? 1 : -1
			while ($in_inside_parens && 
				($back=~/\G.*?([\(\)])/gcs));
		
		$contents = substr($back, 0, $+[0] - 1);
		$back = substr($back, $+[0]);

		return $self->do_err("Unmatched left parentheses during IN processing!") 
			if ($in_inside_parens > 0);
#
#	need a better arglist extractor
#
#        my @vals = split /,/, $contents;
#
		my @vals = ();
		my $spos = 0;
		my $parens = 0;
		my $epos = 0;
		while ($contents=~/\G.*?([\(\),])/gcs) {
			$epos = $+[0];
			push(@vals, substr($contents, $spos, $epos - $spos - 1)),
			$spos = $epos,
			next
				unless ($parens or ($1 ne ','));
			$parens += ($1 eq '(') ? 1 : -1;
		}
#
#	don't forget the last argument
#
		$epos = length($contents),
		push(@vals, substr($contents, $spos, $epos - $spos))
			if ($spos != length($contents));

		my ($op, $combiner) = $not ? ('<>', ' AND ') : ('=', ' OR ');
        @vals = map { "$col $op $_" } @vals;
        $str = "$front (" . join($combiner, @vals) . ") $back";
        $str =~ s/\s+/ /g;
#
#	DAA
#	removed recursion
#
#        return $self->get_in($str);	
    }
	$str =~ s/^\s+//;
	$str =~ s/\s+$//;
	$str =~ s/\(\s+/(/;
	$str =~ s/\s+\)/)/;
    return $str;
}

# groups clauses by nested parens
#
#	DAA
#	rewrite to correct paren scan
#	and optimize code, and remove
#	recursion
#
sub parens_search {
    my $self = shift;
    my $str  = shift;
    my $predicates = shift;
    my $index = scalar @$predicates;

    # to handle WHERE (a=b) AND (c=d)
    # but needs escape space to not foul up AND/OR

#	locate all open parens
#	locate all close parens
#	apply non_paren_search to contents of 
#	inner parens

	my $lparens = ($str=~tr/\(//);
	my $rparens = ($str=~tr/\)//);
	return $self->do_err('Unmatched ' .
		(($lparens > $rparens) ? 'left' : 'right') .
		' parentheses!')
		unless ($lparens == $rparens);

	return $self->non_parens_search($str, $predicates)
		unless $lparens;

	my @lparens = ();
	while ($str=~/\G.*?([\(\)])/gcs) {
		push(@lparens, $-[1]),
		next
			if ($1 eq '(');
#
#	got a close paren, so pop the position of matching
#	left paren and extract the expression, removing the
#	parens
#
		my $pos = pop @lparens;
		my $predlen = $+[1] - $pos;
        my $pred = substr($str, $pos+1, $predlen - 2);
#
#	note that this will pass thru any prior ^$index^ xlation,
#	so we don't need to recurse to recover the predicate
#
		substr($str, $pos, $predlen) = $pred,
		pos($str) = $pos + length($pred),
		next
        	unless ($pred =~ / (AND|OR) /i );
#
#	handle AND/OR
#
		push(@$predicates, substr($str, $pos+1, $predlen-2));
		my $replacement = "^$#$predicates^";
		substr($str, $pos, $predlen) = $replacement;
		pos($str) = $pos + length($replacement);
	}

	return $self->non_parens_search($str,$predicates);
}

# creates predicates from clauses that either have no parens
# or ANDs or have been previously grouped by parens and ANDs
#
#	DAA
#	rewrite to fix paren scanning
#
sub non_parens_search {
    my $self = shift;
    my $str = shift;
    my $predicates = shift;
    my $neg  = 0;
    my $nots = {};

    $neg  = 1,
    $nots = { pred => 1}
    	if ( $str =~ s/^NOT (\^.+)$/$1/i );

    my( $pred1, $pred2, $op );
    my $and_preds =[];
    ($str,$and_preds) = group_ands($str);
    $str = $and_preds->[$1]
    	if $str =~ /^\s*~(\d+)~\s*$/;

	return $self->non_parens_search($$predicates[$1], $predicates)
		if ($str=~/^\s*\^(\d+)\^\s*$/);

	if ($str=~/\G(.*?)\s+(AND|OR)\s+(.*)$/igcs) {
		($pred1, $op, $pred2) = ($1, $2, $3);
	
		if ($pred1=~/^\s*\^(\d+)\^\s*$/) {
			$pred1 = $self->non_parens_search($$predicates[$1],$predicates);
		}
		else {
			$pred1 =~ s/\~(\d+)\~$/$and_preds->[$1]/g;
			$pred1 = $self->non_parens_search($pred1,$predicates);
		}
#
#	handle pred2 as a full predicate
#
		$pred2 =~ s/\~(\d+)\~$/$and_preds->[$1]/g;
		$pred2 = $self->non_parens_search($pred2,$predicates);

        return {
            neg  => $neg,
            nots => $nots,
            arg1 => $pred1,
            op   => uc $op,
            arg2 => $pred2,
        };
	}
#
#	terminal predicate
#	need to check for singleton functions here
#
	my $xstr = $str;
	my ($k,$v);
	if ($str=~/^\s*([A-Z]\w*)\s*\[/gcs) {
#
#	we've got a function, check if its a singleton
#
		my $parens = 1;
		my $spos = $-[1];
		my $epos = 0;
		$epos = $-[1],
		$parens += ($1 eq '[') ? 1 : -1
			while (($parens > 0) && ($str=~/\G.*?([\[\]])/gcs));
		$k = substr($str, $spos, $epos - $spos + 1);
		$k=~s/\?(\d+)\?/$self->{struct}{literals}[$1]/g;
#
#	for now we assume our parens are balanced
#	now look for a predicate operator and a right operand
#
		$v = $1,
		$v=~s/\?(\d+)\?/$self->{struct}{literals}[$1]/g
			if ($str =~ /\G\s+\S+\s*(.+)\s*$/gcs);
	}
	else {
		$xstr =~ s/\?(\d+)\?/$self->{struct}{literals}[$1]/g;
		($k,$v) = $xstr =~ /^(\S+?)\s+\S+\s*(.+)\s*$/;
	}
	push @{ $self->{struct}{where_cols}{$k}}, $v 
		if defined $k;
	return $self->PREDICATE($str);
}

# groups AND clauses that aren't already grouped by parens
#
sub group_ands{
    my $str       = shift;
    my $and_preds = shift || [];
    return($str,$and_preds) 
    	unless $str =~ / AND / and $str =~ / OR /;

    return $str,$and_preds
	    unless ($str =~ /^(.*?) AND (.*)$/i );

	my($front, $back)=($1,$2);
	my $index = scalar @$and_preds;
	$front = $1
		if ($front =~ /^.* OR (.*)$/i );

	$back = $1
		if ($back =~ /^(.*?) (OR|AND) .*$/i );

	my $newpred = "$front AND $back";
	push @$and_preds, $newpred;
	$str =~ s/\Q$newpred/~$index~/i;
	return group_ands($str,$and_preds);
}

# replaces string function parens with square brackets
# e.g TRIM (foo) -> TRIM[foo]
#
#	DAA update to support UDFs
#	and remove recursion
#
sub nongroup_string {
	my $self = shift;
    my $str = shift;
#
#	add in any user defined functions
#
    my $f = FUNCTION_NAMES;
	$f .= '|' . uc $_
    	foreach (keys %{$self->{opts}{function_names}});
#
#	we need a scan here to permit arbitrarily nested paren
#	arguments to functions
#
	my $parens = 0;
	my $pos;
	my @lparens = ();
	while ($str=~/\G.*?((($f)\s*\()|[\(\)])/igcs) {
		if ($1 eq ')') {
#
#	close paren, see if any pending function open
#	paren matches it
#
			$parens--;
			$pos = $+[0],
			substr($str, $+[0]-1, 1) = ']',
			pos($str) = $pos,
			pop @lparens
				if (@lparens && ($lparens[-1] == $parens));
		}
		elsif ($1 eq '(') {
#
#	just an open paren, count it and go on
#
			$parens++;
		}
		else {
#
#	new function definition, capture its open paren
#	also uppercase the function name
#
			$pos = $+[0];
			substr($str, $-[3], length($3)) = uc $3;
			substr($str, $+[0]-1, 1) = '[';
			pos($str) = $pos;
			push @lparens, $parens;
			$parens++;
		}
	}

#	return $self->do_err('Unmatched ' .
#		(($parens > 0) ? 'left' : 'right') . ' parentheses!')
#		if $parens;
#
#	DAA
#	remove scoped recursion
#
#	return ( $str =~ /($f)\s*\(/i ) ?
#		nongroup_string($str) : $str;
	return $str;
}

# replaces math parens with square brackets
# e.g (4-(6+7)*9) -> MATH[4-MATH[6+7]*9]
#
sub nongroup_numeric {
    my $str = shift;
    my $has_op;
#
#	DAA
#	optimize regex
#
    if ( $str =~ /\(([\w \*\/\+\-\[\]\?]+)\)/ ) {
        my $match = $1;
        if ($match !~ /(LIKE |IS|BETWEEN|IN)/i ) {
            my $re    = quotemeta($match);
            $str =~ s/\($re\)/MATH\[$match\]/;
		}
        else {
			$has_op++;
		}
    }
#
#	DAA
#	remove scoped recursion
#
	return ( !$has_op and $str =~ /\(([\w \*\/\+\-\[\]\?]+)\)/ ) ?
		nongroup_numeric($str) : $str;
}
############################################################


#########################################################
# LITERAL_LIST ::= <literal> [,<literal>]
#########################################################
sub LITERAL_LIST {
    my $self = shift;
    my $str  = shift;
    my @tokens = split /,/, $str;
    my @values;
    for my $tok(@tokens) {
        my $val  = $self->ROW_VALUE($tok);
        return $self->do_err(
            qq('$tok' is not a valid value or is not quoted!)
        ) unless $val;
        push @values, $val;
    }
    $self->{"struct"}->{"values"} = \@values;
    return 1;
}


###################################################################
# LITERAL ::= <quoted_string> | <question mark> | <number> | NULL
###################################################################
sub LITERAL {
    my $self = shift;
    my $str  = shift;
#
#	DAA
#	strip parens (if any)
#
	$str = $1 
		while ($str=~/^\s*\(\s*(.+)\s*\)\s*$/);

    return 'null' if $str =~ /^NULL$/i;    # NULL
#    return 'empty_string' if $str =~ /^~E~$/i;    # NULL
    if ($str eq '?') {
          $self->{struct}->{num_placeholders}++;
          return 'placeholder';
    } 
#    return 'placeholder' if $str eq '?';   # placeholder question mark
    return 'string' if $str =~ /^'.*'$/s;  # quoted string
    return 'number' if $str =~             # number
       /^[+-]?(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/;
    return undef;
}
###################################################################
# PREDICATE
###################################################################
sub PREDICATE {
    my $self = shift;
    my $str  = shift;

	my($arg1, $op, $arg2, $opexp);

   	$opexp = $self->{opts}{valid_comparison_NOT_ops_regex},
   	($arg1,$op,$arg2) = $str =~ /$opexp/i
		if $self->{opts}{valid_comparison_NOT_ops_regex};

	$opexp = $self->{opts}{valid_comparison_twochar_ops_regex},
	($arg1,$op,$arg2) = $str =~ /$opexp/i
	    if (!defined($op) && $self->{opts}{valid_comparison_twochar_ops_regex});

	$opexp = $self->{opts}{valid_comparison_ops_regex},
	($arg1,$op,$arg2) = $str =~ /$opexp/i
	    if (!defined($op) && $self->{opts}{valid_comparison_ops_regex});

    $op = uc $op;

	#
	### USER-DEFINED PREDICATE
	#
	$arg1 = $str,

	$op   = 'USER_DEFINED',
	$arg2 = '' unless (defined $arg1 && defined $op && defined $arg2);

#	my $uname = $self->is_func($arg1);
#        if (!$uname) {
 #           $arg1 =~ s/^(\S+).*$/$1/;
#	    return $self->do_err("Bad predicate: '$arg1'!");
#        }

    my $negated = 0;  # boolean value showing if predicate is negated
    my %not;          # hash showing elements modified by NOT
    #
    # e.g. "NOT bar = foo"        -> %not = (arg1=>1)
    #      "bar NOT LIKE foo"     -> %not = (op=>1)
    #      "NOT bar NOT LIKE foo" -> %not = (arg1=>1,op=>1);
    #      "NOT bar IS NOT NULL"  -> %not = (arg1=>1,op=>1);
    #      "bar = foo"            -> %not = undef;
    #
    $not{arg1}++
	    if ( $arg1 =~ s/^NOT (.+)$/$1/i );

    $not{op}++
    	if ( $op =~ s/^(.+) NOT$/$1/i
    	  || $op =~ s/^NOT (.+)$/$1/i );

    $negated = 1 if %not and scalar keys %not == 1;

    return undef unless $arg1 = $self->ROW_VALUE($arg1);

    if ($op ne 'USER_DEFINED') {                # USER-PREDICATE;
        return undef unless $arg2 = $self->ROW_VALUE($arg2);
    }
    else {
#        $arg2 = $self->ROW_VALUE($arg2);
    }

	push(@{ $self->{"struct"}->{"keycols"} }, $arg1->{"value"}),
	push(@{ $self->{"struct"}->{"keycols"} }, $arg2->{"value"})
    	if ( ref($arg1)eq 'HASH' and ($arg1->{"type"}||'') eq 'column'
    		and ($arg2->{"type"}||'') eq 'column'
			and $op  eq '=');
    return {
        neg  => $negated,
        nots => \%not,
        arg1 => $arg1,
        op   => $op,
        arg2 => $arg2,
    };
}

sub undo_string_funcs {
	my $self = shift;
    my $str = shift;
    my $f = FUNCTION_NAMES;
#
#	don't forget our UDFs
#
	$f .= '|' . uc $_
    	foreach (keys %{$self->{opts}{function_names}});
#
#	eliminate recursion:
#	we have to scan for closing brackets, since we may
#	have intervening MATH elements with brackets
#
	my $brackets = 0;
	my $pos;
	my @lbrackets = ();
	while ($str=~/\G.*?((($f)\s*\[)|[\[\]])/igcs) {
		if ($1 eq ']') {
#
#	close paren, see if any pending function open
#	paren matches it
#
			$brackets--;
			$pos = $+[0],
			substr($str, $+[0]-1, 1) = ')',
			pos($str) = $pos,
			pop @lbrackets
				if (@lbrackets && ($lbrackets[-1] == $brackets));
		}
		elsif ($1 eq '[') {
#
#	just an open paren, count it and go on
#
			$brackets++;
		}
		else {
#
#	new function definition, capture its open paren
#	also uppercase the function name
#
			$pos = $+[0];
			substr($str, $-[3], length($3)) = uc $3;
			substr($str, $+[0]-1, 1) = '(';
			pos($str) = $pos;
			push @lbrackets, $brackets;
			$brackets++;
		}
	}

#	return undo_string_funcs($str)
#    	if ($str =~ /($f)\[/);

    return $str;
}

sub undo_math_funcs {
    my $str = shift;
#
#	eliminate recursion
#
    1 while ($str =~ s/MATH\[([^\]\[]+?)\]/($1)/);

#	return undo_math_funcs($str)
#    	if ($str =~ /MATH\[/);

    return $str;
}
#
#	DAA
#	need better nested function/parens handling
#
sub extract_func_args {
	my ($self, $value) = @_;

    my @final_args = ();
	my $spos = 0;
	my $parens = 0;
	my $epos = 0;
	my $delim = 0;
	while ($value=~/\G.*?([\(\),])/gcs) {
   		$epos = $+[0];
   		$delim = $1;
		push(@final_args,
			$self->ROW_VALUE(substr($value,$spos,$epos-$spos-1))),
		$spos = $epos,
	    next
	    	unless ($parens or ($delim ne ','));

	    $parens += ($delim eq '(') ? 1 : -1
	    	unless ($delim eq ',');
	}
#
#	don't forget the last argument
#
	$epos = length($value),
	push(@final_args, 
		$self->ROW_VALUE(substr($value, $spos, $epos - $spos)))
		if ($spos != length($value));
	return @final_args;
}

###################################################################
# ROW_VALUE ::= <literal> | <column_name>
###################################################################
sub ROW_VALUE {
    my $self = shift;
    my $str  = shift;

	$str=~s/^\s+//;
	$str=~s/\s+$//;
    $str = $self->undo_string_funcs($str);
    $str = undo_math_funcs($str);

    # USER-DEFINED FUNCTION
    #
    my $user_func_name = $str;
    my $user_func_args = '';
#
#	DAA
#	need better paren check here
#
#    if ($str =~ /^(\S+)\s*(.*)\s*$/ ) {
    if ($str =~ /^([^\s\(]+)\s*(.*)\s*$/ ) {
        $user_func_name = uc $1;
        $user_func_args = $2;
#
#	convert operator-like function to 
#	parenthetical format
#
        if ($self->{opts}->{function_names}->{$user_func_name}
        	and $user_func_args !~ /^\(.*\)$/) {
            $str = "$user_func_name ($user_func_args)";
        }
    } 
    else {
        $user_func_name =~ s/^(\S+).*$/$1/;
    }
    if ( $self->{opts}->{function_names}->{uc $user_func_name} 
         and $user_func_name !~ /(TRIM|SUBSTRING)/i
    ) {
        my($name, $value) = ($user_func_name,'');
        if ($str =~ /^(\S+)\s*\((.+)\)\s*$/i ) {
            $name  = uc $1;
            $value = $2;
        }
        if ($self->{opts}->{function_names}->{$name}) {

#
#	DAA
#	need a better argument extractor, since it can
#	contain arbitrary (possibly parenthesized) 
#	expressions/functions
#
#           if ($value =~ /\(/ ) {
#               $value = $self->ROW_VALUE($value);
#           }
#           my @args = split ',',$value;

            my @final_args = $self->extract_func_args($value);
            my $usr_sub = $self->{opts}->{"function_defs"}->{$name}->{"sub"}
                       if $self->{opts}->{function_defs}
                      and $self->{opts}->{function_defs}->{$name};
            $self->{struct}->{procedure} = {};
	    use SQL::Statement::Util;
	    return SQL::Statement::Util::Function->new(
                $name,
                $usr_sub->{value},
                \@final_args,
            ) if $usr_sub;
#            return {
#                type    => 'function',
#                name    => $name,
#                value   =>  {
#                              value => \@final_args,
#                              type  => 'multiple_args',
#                            },
#                usr_sub => $usr_sub,
#            };
        }
    }
    my $type;

    # MATH
    #
    if ($str =~ /[\*\+\-\/]/ ) {
        my @vals;
        my $i=-1;
        $str =~ s/([^\s\*\+\-\/\)\(]+)/push @vals,$1;$i++;"?$i?"/ge;
        my @newvalues;
        for (@vals) {
            my $val = $self->ROW_VALUE($_);
            if ($val && $val->{"type"} !~ /number|column|placeholder/) {
                 return $self->do_err(qq[
                     String '$val' not allowed in Numeric expression!
                 ]);
	    }
            push @newvalues,$val;
	}
        return {
            type => 'function',
            name => 'numeric_exp',
            str  => $str,
            vals => \@newvalues,
        }
    }

    # SUBSTRING (value FROM start [FOR length])
    #
    if ($str =~ /^SUBSTRING \((.+?) FROM (.+)\)\s*$/i ) {
        my $name  = 'SUBSTRING';
        my $start = $2;
        my $value = $self->ROW_VALUE($1);
        my $length;
        if ($start =~ /^(.+?) FOR (.+)$/i) {
            $start  = $1;
            $length = $2;
            $length = $self->ROW_VALUE($length);
	}
        $start = $self->ROW_VALUE($start);
        $str =~ s/\?(\d+)\?/$self->{"struct"}->{"literals"}->[$1]/g;
        return $self->do_err(
                "Can't use a string as a SUBSTRING position: '$str'!")
               if $start->{"type"} eq 'string'
               or ($start->{"length"} and $start->{"length"}->{"type"} eq 'string');
        return undef unless $value;
        return $self->do_err(
                "Can't use a number in SUBSTRING: '$str'!")
               if $value->{"type"} eq 'number';
        return {
            "type"   => 'function',
            "name"   => $name,
            "value"  => $value,
            "start"  => $start,
            "length" => $length,
        };
    }

    # TRIM ( [ [TRAILING|LEADING|BOTH] ['char'] FROM ] value )
    #
    if ($str =~ /^(TRIM) \((.+)\)\s*$/i ) {
        my $name  = uc $1;
        my $value = $2;
        my($trim_spec,$trim_char);
        if ($value =~ /^(.+) FROM ([^\(\)]+)$/i ) {
            my $front = $1;
            $value    = $2;
            if ($front =~ /^\s*(TRAILING|LEADING|BOTH)(.*)$/i ) {
                $trim_spec = uc $1;
                $trim_char = $2;
                $trim_char =~ s/^\s+//;
                $trim_char =~ s/\s+$//;
                undef $trim_char if length($trim_char)==0;
	    }
            else {
               $trim_char = $front;
               $trim_char =~ s/^\s+//;
               $trim_char =~ s/\s+$//;
	    }
	}
        $trim_char ||= '';
        $trim_char =~ s/\?(\d+)\?/$self->{"struct"}->{"literals"}->[$1]/g;
        $value = $self->ROW_VALUE($value);
        return undef unless $value;
        $str =~ s/\?(\d+)\?/$self->{"struct"}->{"literals"}->[$1]/g;
        my $value_type = $value->{type} if ref $value eq 'HASH';
           $value_type = $value->[0] if ref $value eq 'ARRAY';
        return $self->do_err(
                "Can't use a number in TRIM: '$str'!")
               if $value_type and $value_type eq 'number';
        return {
            type      => 'function',
            name      => $name,
            value     => $value,
            trim_spec => $trim_spec,
            trim_char => $trim_char,
        };
    }

    # UNKNOWN FUNCTION
    if ( $str =~ /^(\S+) \(/ ) {
        die "Unknown function '$1'\n";
    }

    # STRING CONCATENATION
    #
    if ($str =~ /\|\|/ ) {
        my @vals = split / \|\| /,$str;
        my @newvals;
        for my $val(@vals) {
            my $newval = $self->ROW_VALUE($val);
            return undef unless $newval;
            return $self->do_err(
                "Can't use a number in string concatenation: '$str'!")
                if $newval->{"type"} eq 'number';
            push @newvals,$newval;
	}
        return {
            type  => 'function',
            name  => 'str_concat',
            value => \@newvals,
        };
    }

    # NULL, PLACEHOLDER, NUMBER
    #
    if ( $type = $self->LITERAL($str) ) {
        undef $str if $type eq 'null';
#        if ($type eq 'empty_string') {
#           $str = '';
#           $type = 'string';
#	} 
        $str = '' if $str and $str eq q('');
        return { type => $type, value => $str };
    }

    # QUOTED STRING LITERAL
    #
    if ($str =~ /\?(\d+)\?/) {
        return { type  =>'string',
                 value  => $self->{"struct"}->{"literals"}->[$1] };
    }
    # COLUMN NAME
    #
    return undef unless $str = $self->COLUMN_NAME($str);
    if ( $str =~ /^(.*)\./ && !$self->{"tmp"}->{"is_table_name"}->{"\L$1"}
       and !$self->{"tmp"}->{"is_table_alias"}->{"\L$1"} ) {
        return $self->do_err(
            "Table '$1' in WHERE clause not in FROM clause!"
        );
    }
#    push @{ $self->{"struct"}->{"where_cols"}},$str
#       unless $self->{"tmp"}->{"where_cols"}->{"$str"};
    $self->{"tmp"}->{"where_cols"}->{"$str"}++;
    return { type => 'column', value => $str };
}

###############################################
# COLUMN NAME ::= [<table_name>.] <identifier>
###############################################

sub COLUMN_NAME {
    my $self   = shift;
    my $str = shift;
    my($table_name,$col_name);
    if ( $str =~ /^\s*(\S+)\.(\S+)$/s ) {
      if (!$self->{"opts"}->{"valid_options"}->{"SELECT_MULTIPLE_TABLES"}) {
          return $self->do_err('Dialect does not support multiple tables!');
      }
      $table_name = $1;
      $col_name   = $2;
      return undef unless $table_name = $self->TABLE_NAME($table_name);
      $table_name = $self->replace_quoted_ids($table_name);
      my $ref;
      if ($table_name =~ /^"/) { #"
          if (!$self->{"tmp"}->{"is_table_name"}->{"$table_name"}
          and !$self->{"tmp"}->{"is_table_alias"}->{"$table_name"}
         ) {
          $self->do_err(
                "Table '$table_name' referenced but not found in FROM list!"
          );
          return undef;
      } 
      }
      elsif (!$self->{"tmp"}->{"is_table_name"}->{"\L$table_name"}
       and !$self->{"tmp"}->{"is_table_alias"}->{"\L$table_name"}
         ) {
          $self->do_err(
                "Table '$table_name' referenced but not found in FROM list!"
          );
          return undef;
      } 
    }
    else {
      $col_name = $str;
    }
    $col_name =~ s/^\s+//;
    $col_name =~ s/\s+$//;
    my $user_func = $col_name;
    $user_func =~ s/^(\S+).*$/$1/;
    if ($col_name =~ /(TRIM|SUBSTRING)/i) {
       # ?
    }
    else {
      undef $user_func unless $self->{opts}->{function_names}->{uc $user_func};
    }
    if (!$user_func) {
        return undef unless $col_name eq '*'
                         or $self->IDENTIFIER($col_name);
    }
    #
    # MAKE COL NAMES ALL UPPER CASE UNLESS IS DELIMITED IDENTIFIER
    my $orgcol = $col_name;

    if ($col_name =~ /^\?QI(\d+)\?$/) {
        $col_name = $self->replace_quoted_ids($col_name);
    }
    else {
        $col_name = uc $col_name unless $self->{struct}->{command} eq 'CREATE'
    ##############################################
    #
    # JZ addition to RR's alias patch
    #
                                     or $col_name =~ /^"/;

    }
    #
    $col_name = $self->{struct}->{column_aliases}->{$col_name}
             if $self->{struct}->{column_aliases}->{$col_name};
#    $orgcol = $self->replace_quoted_ids($orgcol);
    ##############################################

    if ($table_name) {
       my $alias = $self->{tmp}->{is_table_alias}->{"\L$table_name"};
       $table_name = $alias if defined $alias;
		$table_name = uc $table_name;
       $col_name = "$table_name.$col_name";
    }
    return $col_name;
}

#########################################################
# COLUMN NAME_LIST ::= <column_name> [,<column_name>...]
#########################################################
sub COLUMN_NAME_LIST {
    my $self = shift;
    my $col_str = shift;
    my @col_list = split ',',$col_str;
    if (!(scalar @col_list)) {
        return $self->do_err('Missing column name list!');
    }
    my @newcols;
    my $newcol;
    for my $col(@col_list) {
    $col =~ s/^\s+//;
    $col =~ s/\s+$//;
#        return undef if !($newcol = $self->COLUMN_NAME(trim($col)));
        return undef if !($newcol = $self->COLUMN_NAME($col));
        push @newcols, $newcol;
    }
    $self->{"struct"}->{"column_names"} = \@newcols;
    return 1;
}


#####################################################
# TABLE_NAME_LIST := <table_name> [,<table_name>...]
#####################################################
sub TABLE_NAME_LIST {
    my $self = shift;
    my $table_name_str = shift;
    my %aliases = ();
    my @tables;
    $table_name_str =~ s/(\?\d+\?),/$1:/g;  # fudge commas in functions
    my @table_names = split ',', $table_name_str;
    if ( scalar @table_names > 1
        and !$self->{"opts"}->{"valid_options"}->{'SELECT_MULTIPLE_TABLES'}
    ) {
        return $self->do_err('Dialect does not support multiple tables!');
    }
    my %is_table_alias;
    for my $table_str(@table_names) {
        $table_str =~ s/(\?\d+\?):/$1,/g;  # unfudge commas in functions
        $table_str =~ s/\s+\(/\(/g;  # fudge spaces in functions
        my($table,$alias);
        my(@tstr) = split /\s+/,$table_str;
        if    (@tstr == 1) { $table = $tstr[0]; }
        elsif (@tstr == 2) { $table = $tstr[0]; $alias = $tstr[1]; }
        elsif (@tstr == 3) {
            return $self->do_err("Can't find alias in FROM clause!")
                   unless uc($tstr[1]) eq 'AS';
            $table = $tstr[0]; $alias = $tstr[2];
        }
        else {
		    return $self->do_err("Can't find table names in FROM clause!")
		}
        $table =~ s/\(/ \(/g;  # unfudge spaces in functions
        my $u_name = $table;
        $u_name =~ s/^(\S+)\s*(.*$)/$1/;
        my $u_args=$2;
#        $u_name = uc $u_name;
#        if ($self->{opts}->{function_names}->{$u_name}) {
        if ($u_name = $self->is_func($u_name) ) {
#            my $u_func = $self->ROW_VALUE($table);
            $u_args = " $u_args" if $u_args;
            my $u_func = $self->ROW_VALUE($u_name.$u_args);
            $self->{"struct"}->{"table_func"}->{$u_name} = $u_func;
            $self->{"struct"}->{"temp_table"} = 1;
            $table = $u_name;
		}
        else {
	        return undef unless $self->TABLE_NAME($table);
		}
        $table = $self->replace_quoted_ids($table);
        push @tables, $table;
        if ($alias) {
            return undef unless $self->TABLE_NAME($alias);
            $alias = $self->replace_quoted_ids($alias);
            if ($alias =~ /^"/) {
                push @{$aliases{$table}},"$alias";
                $is_table_alias{"$alias"}=$table;
		    }
            else {
                push @{$aliases{$table}},"\L$alias";
                $is_table_alias{"\L$alias"}=$table;
		    }
		}
    }
    my %is_table_name = map { lc $_ => 1 } @tables;
    $self->{"tmp"}->{"is_table_alias"}  = \%is_table_alias;
    $self->{"tmp"}->{"is_table_name"}  = \%is_table_name;
    $self->{"struct"}->{"table_names"} = \@tables;
    $self->{"struct"}->{"table_alias"} = \%aliases;
    $self->{"struct"}->{"multiple_tables"} = 1 if @tables > 1;
    return 1;
}

sub is_func(){
    my($self,$name) =@_;
    $name =~ s/^(\S+).*$/$1/;
    return $name if $self->{opts}->{function_names}->{$name};
    return uc $name if $self->{opts}->{function_names}->{uc $name};
}

#############################
# TABLE_NAME := <identifier>
#############################
sub TABLE_NAME {
    my $self = shift;
    my $table_name = shift;
    if( $table_name =~ /^(.+?)\.([^\.]+)$/ ) {
        my $schema = $1;  # ignored
        $table_name = $2;
    }
    if ($table_name =~ /\s*(\S+)\s+\S+/s) {
          return $self->do_err("Junk after table name '$1'!");
    }
    $table_name =~ s/\s+//s;
    if (!$table_name) {
        return $self->do_err('No table name specified!');
    }
    return $table_name if $self->IDENTIFIER($table_name);
#    return undef if !($self->IDENTIFIER($table_name));
#    return 1;
}


###################################################################
# IDENTIFIER ::= <alphabetic_char> { <alphanumeric_char> | _ }...
#
# and must not be a reserved word or over 128 chars in length
###################################################################
sub IDENTIFIER {
    my $self = shift;
    my $id   = shift;
    if ($id =~ /^\?QI(.+)\?$/ ) {
        return 1;
    }
    return 1 if $id =~ /^".+?"$/s; # QUOTED IDENTIFIER
    if( $id =~ /^(.+)\.([^\.]+)$/ ) {
        my $schema = $1;  # ignored
        $id = $2;
    }
    my $err  = "Bad table or column name '$id' ";        # BAD CHARS
    if ($id =~ /\W/) {
        $err .= "has chars not alphanumeric or underscore!";
        return $self->do_err( $err );
    }
    if ($id =~ /^_/ or $id =~ /^\d/) {                    # BAD START
        $err .= "starts with non-alphabetic character!";
        return $self->do_err( $err );
    }
    if ( length $id > 128 ) {                              # BAD LENGTH
        $err .= "contains more than 128 characters!";
        return $self->do_err( $err );
    }
    $id = uc $id;
    if ( $self->{"opts"}->{"reserved_words"}->{$id} ) {   # BAD RESERVED WORDS
        $err .= "is a SQL reserved word!";
        return $self->do_err( $err );
    }
    return 1;
}

########################################
# PRIVATE METHODS AND UTILITY FUNCTIONS
########################################
sub order_joins {
    my $self = shift;
    my $links = shift;
    for my $link(@$links) {
      if ($link !~ /\./) {
          return [];
      }
    }
    @$links = map { s/^(.+)\..*$/$1/; $1; } @$links;
    my @all_tables;
    my %relations;
    my %is_table;
    while (@$links) {
        my $t1 = shift @$links;
        my $t2 = shift @$links;
        return undef unless defined $t1 and defined $t2;
        push @all_tables, $t1 unless $is_table{$t1}++;
        push @all_tables, $t2 unless $is_table{$t2}++;
        $relations{$t1}{$t2}++;
        $relations{$t2}{$t1}++;
    }
    my @tables = @all_tables;
    my @order = shift @tables;
    my %is_ordered = ( $order[0] => 1 );
    my %visited;
    while(@tables) {
        my $t = shift @tables;
        my @rels = keys %{$relations{$t}};
        for my $t2(@rels) {
            next unless $is_ordered{$t2};
            push @order, $t;
            $is_ordered{$t}++;
            last;
        }
        if (!$is_ordered{$t}) {
            push @tables, $t if $visited{$t}++ < @all_tables;
        }
    }
    return $self->do_err(
        "Unconnected tables in equijoin statement!"
    ) if @order < @all_tables;
    return \@order;
}

sub bless_me {
    my $class  = shift;
    my $self   = shift || {};
    return bless $self, $class;
}

# PROVIDE BACKWARD COMPATIBILIT FOR JOCHEN'S FEATURE ATTRIBUTES TO NEW
#
#
sub set_feature_flags {
    my($self,$select,$create) = @_;
    if (defined $select) {
        delete $self->{"select"};
        $self->{"opts"}->{"valid_options"}->{"SELECT_MULTIPLE_TABLES"} =
            $self->{"opts"}->{"select"}->{join} =  $select->{join};
    }
    if (defined $create) {
        delete $self->{"create"};
        for my $key(keys %$create) {
            my $type = $key;
            $type =~ s/type_(.*)/\U$1/;
            $self->{"opts"}->{"valid_data_types"}->{"$type"} =
                $self->{"opts"}->{"create"}->{"$key"} = $create->{"$key"};
	}
    }
}

sub clean_sql {
    my $self = shift;
    my $sql  = shift;
    my $fields;
    my $i=-1;
    my $e = '\\';
    $e = quotemeta($e);

    #
    # patch from cpan@goess.org, adds support for col2=''
    #
    # 
    # $sql =~ s~'(([^'$e]|$e.|'')+)'~push(@$fields,$1);$i++;"?$i?"~ge;
    $sql =~ s~(?<!')'(([^'$e]|$e.|'')+)'~push(@$fields,$1);$i++;"?$i?"~ge;
    #
    @$fields = map { s/''/\\'/g; $_ } @$fields;
    if ( $sql =~ tr/[^\\]'// % 2 == 1 ) {
    $sql =~ s/^.*\?(.+)$/$1/;
        die "Mismatched single quote before: '$sql'\n";
    }
    if ($sql =~ /\?\?(\d)\?/) {
        $sql = $fields->[$1];
        die "Mismatched single quote: '$sql\n";
    }
    @$fields = map { s/$e'/'/g; s/^'(.*)'$/$1/; $_} @$fields;

    #
    # From Steffen G. to correctly return newlines from $dbh->quote;
    #
    @$fields = map { s/([^\\])\\r/$1\r/g; $_ } @$fields;
    @$fields = map { s/([^\\])\\n/$1\n/g; $_ } @$fields;

    $self->{"struct"}->{"literals"} = $fields;

    my $qids;
    $i=-1;
    $e = q/""/;
#    $sql =~ s~"(([^"$e]|$e.)+)"~push(@$qids,$1);$i++;"?QI$i?"~ge;
    $sql =~ s~"(([^"]|"")+)"~push(@$qids,$1);$i++;"?QI$i?"~ge;
    #@$qids = map { s/$e'/'/g; s/^'(.*)'$/$1/; $_} @$qids;
    $self->{"struct"}->{"quoted_ids"} = $qids if $qids;

#    $sql =~ s~'(([^'\\]|\\.)+)'~push(@$fields,$1);$i++;"?$i?"~ge;
#    @$fields = map { s/\\'/'/g; s/^'(.*)'$/$1/; $_} @$fields;
#print "$sql [@$fields]\n";# if $sql =~ /SELECT/;

## before line 1511
    my $comment_re = $self->{"comment_re"};
#    if ( $sql =~ s/($comment_re)//gs) {
#       $self->{"comment"} = $1;
#    }
    if ( $sql =~ /(.*)$comment_re$/s) {
       $sql = $1;
       $self->{"comment"} = $2;
    }
    if ($sql =~ /^(.*)--(.*)(\n|$)/) {
       $sql               = $1;
       $self->{"comment"} = $2;
    }

    $sql =~ s/\n/ /g;
    $sql =~ s/\s+/ /g;
    $sql =~ s/(\S)\(/$1 (/g; # ensure whitespace before (
    $sql =~ s/\)(\S)/) $1/g; # ensure whitespace after )
    $sql =~ s/\(\s*/(/g;     # trim whitespace after (
    $sql =~ s/\s*\)/)/g;     # trim whitespace before )
       #
       # $sql =~ s/\s*\(/(/g;   # trim whitespace before (
       # $sql =~ s/\)\s*/)/g;   # trim whitespace after )
    for my $op( qw( = <> < > <= >= \|\|) ) {
        $sql =~ s/(\S)$op/$1 $op/g;
        $sql =~ s/$op(\S)/$op $1/g;
    }
    $sql =~ s/< >/<>/g;
    $sql =~ s/< =/<=/g;
    $sql =~ s/> =/>=/g;
    $sql =~ s/\s*,/,/g;
    $sql =~ s/,\s*/,/g;
    $sql =~ s/^\s+//;
    $sql =~ s/\s+$//;
    return $sql;
}

sub trim {
    my $str = shift or return '';
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    return $str;
}

sub do_err {
    my $self = shift;
    my $err  = shift;
    my $errtype  = shift;
    my @c = caller 4;
    $err = "$err\n\n";
#    $err = $errtype ? "DIALECT ERROR: $err in $c[3]"
#                    : "SQL ERROR: $err in $c[3]";
    $err = $errtype ? "DIALECT ERROR: $err"
                    : "SQL ERROR: $err";
    $self->{"struct"}->{"errstr"} = $err;
    #$self->{"errstr"} = $err;
    warn $err if $self->{"PrintError"};
    die $err if $self->{"RaiseError"};
    return undef;
}
#
#	DAA
#	abstract method so subclasses can provide
#	their own syntax transformations
#
sub transform_syntax {
	my ($self, $str) = @_;
	return $str;
}

1;

__END__

=pod

=head1 NAME

 SQL::Parser -- validate and parse SQL strings

=head1 SYNOPSIS

 use SQL::Parser;                                     # CREATE A PARSER OBJECT
 my $parser = SQL::Parser->new();

 $parser->feature( $class, $name, $value );           # SET OR FIND STATUS OF
 my $has_feature = $parser->feature( $class, $name ); # A PARSER FEATURE

 $parser->dialect( $dialect_name );                   # SET OR FIND STATUS OF
 my $current_dialect = $parser->dialect;              # A PARSER DIALECT


=head1 DESCRIPTION

SQL::Parser is part of the SQL::Statement distribution and, most interaction with the parser should be done through SQL::Statement.  The methods shown above create and modify a parser object.  To use the parser object to parse SQL and to examine the resulting structure, you should use SQL::Statement.

B<Important Note>: Previously SQL::Parser had its own hash-based interface for parsing, but that is now deprecated and will eventually be phased out in favor of the object-oriented parsing interface of SQL::Statement.  If you are unable to transition some features to the new interface or have concerns about the phase out, please contact Jeff.  See L<The Parse Structure> for details of the now-deprecated hash method if you still need them.

=head1 METHODS

=head2 new()

Create a new parser object

 use SQL::Parser;
 my $parser = SQL::Parser->new();

The new() method creates a SQL::Parser object which can then be 
used to parse and validate the syntax of SQL strings. It takes two
optional parameters - 1) the name of the SQL dialect that will define
the syntax rules for the parser and 2) a reference to a hash which can 
contain additional attributes of the parser.  If no dialect is specified, 
'AnyData' is the default.

 use SQL::Parser;
 my $parser = SQL::Parser->new( $dialect_name, \%attrs );

The dialect_name parameter is a string containing any valid
dialect such as 'ANSI', 'AnyData', or 'CSV'.  See the section on
the dialect() method below for details.

The attribute parameter is a reference to a hash that can
contain error settings for the PrintError and RaiseError
attributes.

An example:

  use SQL::Parser;
  my $parser = SQL::Parser->new('AnyData', {RaiseError=>1} );

  This creates a new parser that uses the grammar rules
  contained in the .../SQL/Dialects/AnyData.pm file and which
  sets the RaiseError attribute to true.


=head2 dialect()

 $parser->dialect( $dialect_name );     # load a dialect configuration file
 my $dialect = $parser->dialect;        # get the name of the current dialect

 For example:

   $parser->dialect('AnyData');  # loads the AnyData config file
   print $parser->dialect;       # prints 'AnyData'

The $dialect_name parameter may be the name of any dialect
configuration file on your system.  Use the
$parser->list('dialects') method to see a list of available
dialects.  At a minimum it will include "ANSI", "CSV", and
"AnyData".  For backwards compatiblity 'Ansi' is accepted as a
synonym for 'ANSI', otherwise the names are case sensitive.

Loading a new dialect configuration file erases all current
parser features and resets them to those defined in the
configuration file.

=head2 feature()

Features define the rules to be used by a specific parser
instance.  They are divided into the following classes:

    * valid_commands
    * valid_options
    * valid_comparison_operators
    * valid_data_types
    * reserved_words

Within each class a feature name is either enabled or
disabled. For example, under "valid_data_types" the name "BLOB"
may be either disabled or enabled.  If it is not eneabled
(either by being specifically disabled, or simply by not being
specified at all) then any SQL string using "BLOB" as a data
type will throw a syntax error "Invalid data type: 'BLOB'".

The feature() method allows you to enable, disable, or check the
status of any feature.

 $parser->feature( $class, $name, 1 );             # enable a feature

 $parser->feature( $class, $name, 0 );             # disable a feature

 my $feature = $parser->feature( $class, $name );  # show status of a feature

 For example:

 $parser->feature('reserved_words','FOO',1);       # make 'FOO' a reserved word

 $parser->feature('valid_data_types','BLOB',0);    # disallow 'BLOB' as a
                                                   # data type

                                                   # determine if the LIKE
                                                   # operator is supported
 my $LIKE = $parser->feature('valid_operators','LIKE');

See the section below on "Backwards Compatibility" for use of
the feature() method with SQL::Statement 0.1x style parameters.

=head1 Supported SQL syntax

The SQL::Statement distribution can be used to either just parse SQL statements or to execute them against actual data.  A broader set of syntax is supported in the parser than in the executor.  For example the parser allows you to specify column constraints like PRIMARY KEY.  Currently, these are ignored by the execution engine.  Likewise syntax such as RESTRICT and CASCADE on DROP statements or LOCAL GLOBAL TEMPPORARY tables in CREATE are supported by the parser but ignored by the executor.  

To see the list of Supported SQL syntax formerly kept in this pod, see L<SQL::Statement>.


=head1 Subclassing SQL::Parser

In the event you need to either extend or modify SQL::Parser's
default behavior, the following methods may be overriden
to modify the behavior:

=over

=item C<$self->E<gt>C<get_btwn($string)>

Processes the BETWEEN...AND... predicates; default converts to
2 range predicates.

=item C<$self->E<gt>C<get_in($string)>

Process the IN (...list...) predicates; default converts to
a series of OR'd '=' predicate, or AND'd '<>' predicates for 
NOT IN.

=item C<$self->E<gt>C<transform_syntax($string)>

Abstract method; default simply returns the original string.
Called after get_btwn() and get_in(), but before any further
predicate processing is applied. Possible uses include converting
other predicate syntax not recognized by SQL::Parser into user-defined
functions.

=back

=head1 The parse structure

This section outlines the B<now-deprecated> hash interface to the parsed
structure.  It is included B<for backwards compatability only>.  You should
use the SQL::Statement object interface to the structure instead.  See L<SQL::Statement>.

B<Parse Structures>

Here are some further examples of the data structures returned
by the structure() method after a call to parse().  Only
specific details are shown for each SQL instance, not the entire
struture.

B<parse()>

Once a SQL::Parser object has been created with the new()
method, the parse() method can be used to parse any number of
SQL strings.  It takes a single required parameter -- a string
containing a SQL command.  The SQL string may optionally be
terminated by a semicolon.  The parse() method returns a true
value if the parse is successful and a false value if the parse
finds SQL syntax errors.

Examples:

  1) my $success = $parser->parse('SELECT * FROM foo');

  2) my $sql = 'SELECT * FROM foo';
     my $success = $parser->parse( $sql );

  3) my $success = $parser->parse(qq!
         SELECT id,phrase
           FROM foo
          WHERE id < 7
            AND phrase <> 'bar'
       ORDER BY phrase;
   !);

  4) my $success = $parser->parse('SELECT * FRoOM foo ');

In examples #1,#2, and #3, the value of $success will be true
because the strings passed to the parse() method are valid SQL
strings.

In example #4, however, the value of $success will be false
because the string contains a SQL syntax error ('FRoOM' instead
of 'FROM').

In addition to checking the return value of parse() with a
variable like $success, you may use the PrintError and
RaiseError attributes as you would in a DBI script:

 * If PrintError is true, then SQL syntax errors will be sent as
   warnings to STDERR (i.e. to the screen or to a file if STDERR
   has been redirected).  This is set to true by default which
   means that unless you specifically turn it off, all errors
   will be reported.

 * If RaiseError is true, then SQL syntax errors will cause the
   script to die, (i.e. the script will terminate unless wrapped
   in an eval).  This is set to false by default which means
   that unless you specifically turn it on, scripts will
   continue to operate even if there are SQL syntax errors.

Basically, you should leave PrintError on or else you will not
be warned when an error occurs.  If you are simply validating a
series of strings, you will want to leave RaiseError off so that
the script can check all strings regardless of whether some of
them contain SQL errors.  However, if you are going to try to
execute the SQL or need to depend that it is correct, you should
set RaiseError on so that the program will only continue to
operate if all SQL strings use correct syntax.

IMPORTANT NOTE #1: The parse() method only checks syntax, it
does NOT verify if the objects listed actually exist.  For
example, given the string "SELECT model FROM cars", the parse()
method will report that the string contains valid SQL but that
will not tell you whether there actually is a table called
"cars" or whether that table contains a column called 'model'.
Those kinds of verifications can be performed by the
SQL::Statement module, not by SQL::Parser by itself.

IMPORTANT NOTE #2: The parse() method uses rules as defined by
the selected dialect configuration file and the feature()
method.  This means that a statement that is valid in one
dialect may not be valid in another.  For example the 'CSV' and
'AnyData' dialects define 'BLOB' as a valid data type but the
'ANSI' dialect does not.  Therefore the statement 'CREATE TABLE
foo (picture BLOB)' would be valid in the first two dialects but
would produce a syntax error in the 'ANSI' dialect.

B<structure()>

After a SQL::Parser object has been created and the parse()
method used to parse a SQL string, the structure() method
returns the data structure of that string.  This data structure
may be passed on to other modules (e.g. SQL::Statement) or it
may be printed out using, for example, the Data::Dumper module.

The data structure contains all of the information in the SQL
string as parsed into its various components.  To take a simple
example:

 $parser->parse('SELECT make,model FROM cars');
 use Data::Dumper;
 print Dumper $parser->structure;

Would produce:

 $VAR1 = {
          'column_names' => [
                              'make',
                              'model'
                            ],
          'command' => 'SELECT',
          'table_names' => [
                             'cars'
                           ]
        };


 'SELECT make,model, FROM cars'

      command => 'SELECT',
      table_names => [ 'cars' ],
      column_names => [ 'make', 'model' ],

 'CREATE TABLE cars ( id INTEGER, model VARCHAR(40) )'

      column_defs => {
          id    => { data_type => INTEGER     },
          model => { data_type => VARCHAR(40) },
      },

 'SELECT DISTINCT make FROM cars'

      set_quantifier => 'DISTINCT',

 'SELECT MAX (model) FROM cars'

    set_function   => {
        name => 'MAX',
        arg  => 'models',
    },

 'SELECT * FROM cars LIMIT 5,10'

    limit_clause => {
        offset => 5,
        limit  => 10,
    },

 'SELECT * FROM vars ORDER BY make, model DESC'

    sort_spec_list => [
        { make  => 'ASC'  },
        { model => 'DESC' },
    ],

 "INSERT INTO cars VALUES ( 7, 'Chevy', 'Impala' )"

    values => [ 7, 'Chevy', 'Impala' ],


=head1 AUTHOR & COPYRIGHT

 This module is copyright (c) 2001,2005 by Jeff Zucker.
 All rights reserved.

 The module may be freely distributed under the same terms as
 Perl itself using either the "GPL License" or the "Artistic
 License" as specified in the Perl README file.

 Jeff can be reached at: jzuckerATcpan.org

=cut
