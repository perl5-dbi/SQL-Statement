package SQL::Statement;
#########################################################################
#
# This module is copyright (c), 2001 by Jeff Zucker, All Rights Reserved
#
# It may be freely distributed under the same terms as Perl itself.
#
# See below for help (search for SYNOPSIS)
#########################################################################
use strict;
use SQL::Parser;
use SQL::Eval;
use SQL::Statement::RAM;
use vars qw($VERSION $new_execute $numexp $s2pops $arg_num $dlm $warg_num $HAS_DBI $DEBUG);
BEGIN {
    eval { require 'Data/Dumper.pm'; $Data::Dumper::Indent=1};
    *bug = ($@) ? sub {warn @_} : sub { print Data::Dumper::Dumper(\@_) };
    eval { require 'DBI.pm' };
    $HAS_DBI = 1 unless $@;
    *is_number = ($HAS_DBI)
               ? *DBI::looks_like_number
	       : sub {
                     my @strs = @_;
                     for my $x(@strs) {
                         return 0 if !defined $x;
                         return 0 if $x !~ $numexp;
                     }
                     return 1;
                 };
}

#use locale;

$VERSION = '1.16_01';
$dlm = '~';
$arg_num=0;
$warg_num=0;
$s2pops = {
              'LIKE'   => {'s'=>'LIKE',n=>'LIKE'},
              'CLIKE'  => {'s'=>'CLIKE',n=>'CLIKE'},
              'RLIKE'  => {'s'=>'RLIKE',n=>'RLIKE'},
              '<'  => {'s'=>'lt',n=>'<'},
              '>'  => {'s'=>'gt',n=>'>'},
              '>=' => {'s'=>'ge',n=>'>='},
              '<=' => {'s'=>'le',n=>'<='},
              '='  => {'s'=>'eq',n=>'=='},
              '<>' => {'s'=>'ne',n=>'!='},
};
BEGIN {
  if ($] < 5.005 ) {
    sub qr {}
  }
}

sub new {
    my $class  = shift;
    my $sql    = shift;
    my $flags  = shift;

    # IF USER DEFINED extend_csv IN SCRIPT
    # USE THE ANYDATA DIALECT RATHER THAN THE CSV DIALECT
    # WITH DBD::CSV

    if ($main::extend_csv or $main::extend_sql ) {
       $flags = SQL::Parser->new('AnyData');
    }
    my $parser = $flags;
    my $self   = new2($class);
    $flags->{"PrintError"}    = 1 unless defined $flags->{"PrintError"};
    $flags->{"text_numbers"}  = 1 unless defined $flags->{"text_numbers"};
    $flags->{"alpha_compare"} = 1 unless defined $flags->{"alpha_compare"};
    for (keys %$flags) {
        $self->{$_}=$flags->{$_};
    }
    my $parser_dialect = $flags->{"dialect"} || 'AnyData';
    $parser_dialect = 'AnyData' if $parser_dialect =~ /^(CSV|Excel)$/;

    # Dean Arnold improvement to allow better subclassing
    # if (!ref($parser) or (ref($parser) and ref($parser) !~ /^SQL::Parser/)) {
    if ( !ref($parser)
         or ( ref($parser)
              and ( !$parser->isa('SQL::Parser') )
            )
    ){
        $parser = new SQL::Parser($parser_dialect,$flags);
    }
    if ($] < 5.005 ) {
    $numexp = exists $self->{"text_numbers"}
        ? '^([+-]?|\s+)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$'
        : '^\s*[+-]?\s*\.?\s*\d';
  }
    else {
    $numexp = exists $self->{"text_numbers"}
###new
#        ? qr/^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/
        ? qr/^([+-]?|\s+)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/
###endnew
        : qr/^\s*[+-]?\s*\.?\s*\d/;
    }
    $self->prepare($sql,$parser);
    return $self;
}

sub new2 {
    my $class  = shift;
    my $self   = {};
    return bless $self, $class;
}

sub prepare {
    my $self   = shift;
    my $sql    = shift;
    return $self if $self->{"already_prepared"}->{"$sql"};
    my $parser =  shift;
    my $rv;
    if( $rv = $parser->parse($sql) ) {
       %$self = (%$self,%{$parser->{"struct"}});
       my $tables  = $self->{"table_names"};
       my $columns = $parser->{"struct"}->{"column_names"};
       if ($columns and scalar @$columns == 1 and $columns->[0] eq '*') {
        $self->{"asterisked_columns"} = 1;
       }
       undef $self->{"columns"};
       my $values  = $self->{"values"};
       my $param_num = -1;
       if ($self->{"limit_clause"}) {
	 $self->{"limit_clause"} =
             SQL::Statement::Limit->new( $self->{"limit_clause"} );
       }
       my $max_placeholders = $self->{num_placeholders} || 0;
       if ($max_placeholders) {
           for my $i(0..$max_placeholders-1) {
               $self->{"params"}->[$i] = SQL::Statement::Param->new($i);
           }
       }
       if ($self->{"sort_spec_list"}) {
           for my $i(0..scalar @{$self->{"sort_spec_list"}} -1 ) {
                my($col,$direction) = each %{ $self->{"sort_spec_list"}->[$i] };
                my $tname;
                 if ($col && $col=~/(.*)\.(.*)/) {
                    $tname = $1; $col=$2;
                }
               undef $direction unless $direction && $direction eq 'DESC';
               $self->{"sort_spec_list"}->[$i] =
                    SQL::Statement::Order->new(
                        col   => SQL::Statement::Column->new($col,[$tname]),
                        desc  => $direction,
                    );
           }
       }
       for (@$columns) {
           my $newcol = $_;
           #  my $col_obj = delete $self->{col_obj}->{$newcol};
           my $col_obj =  $self->{col_obj}->{$newcol};
           if ($col_obj and ref($col_obj)=~/::Column$/ ) {
               $self->{"computed_column"}->{$newcol} = $col_obj
                   if defined $col_obj->function;
               $newcol = $col_obj;
	   }
           else {
               $newcol = SQL::Statement::Column->new($newcol,$tables);
	   }
           push @{ $self->{"columns"} }, $newcol;
       }
       for (@$tables) {
           push @{ $self->{"tables"} },
                SQL::Statement::Table->new($_);
       }
       if ($self->{"where_clause"}) {
           if ($self->{"where_clause"}->{"combiners"}) {
               for ( @{ $self->{"where_clause"}->{"combiners"} } ) {
                   if(/OR/i) { $self->{"has_OR"} = 1; last;}
               }
           }
       }
       $self->{"already_prepared"}->{"$sql"}++;
       return $self;
    }
    else {
       $self->{"errstr"} =  $parser->errstr;
       $self->{"already_prepared"}->{"$sql"}++;
       return undef;
    }
}

sub execute {
    my($self, $data, $params) = @_;
    $new_execute=1;
    ($self->{'NUM_OF_ROWS'}, $self->{'NUM_OF_FIELDS'},
          $self->{'data'}) =  (0,0,[]) and return 'OEO' if $self->{no_execute};
    $self->{procedure}->{data}=$data if $self->{procedure};
    $self->{'params'}= $params;
    my($table, $msg);
    my($command) = $self->command();
    return $self->do_err( 'No command found!') unless $command;
    ($self->{'NUM_OF_ROWS'}, $self->{'NUM_OF_FIELDS'},
          $self->{'data'}) = $self->$command($data, $params);

    # MUNGE COLUMN NAME CASE

    # $sth->{NAME} IS ALWAYS UPPER-CASED DURING PROCESSING
    #
    my $names = $self->{NAME};

    # FOR ASTERISKED QUERIES - WE USE STORED CASE OF COLUMNS
    # 
    @$names = map {
        my $org = $self->{ORG_NAME}->{$_};
        $org =~ s/^"//;
        $org =~ s/"$//;
        $org =~ s/""/"/g;
        $org;
    } @$names  if $self->{asterisked_columns};

    $names = $self->{org_col_names}  unless $self->{asterisked_columns};

    my $newnames;
#
#	DAA
#    for (0..$#{@$names}) {
    my @orgnames=@{$names} if $names;
    for my $i(0..$#$names) {
        my $newname = $orgnames[$i];
           $newname = $self->{columns}->[$i]->display_name 
        	if $self->{columns}->[$i]->can('display_name');
        push @$newnames,$newname;
    }
    $self->{NAME} = $newnames;

    my $tables;
    @$tables = map {$_->{"name"}} @{ $self->{"tables"} };
    delete $self->{'tables'};  # Force closing the tables
    for (@$tables) {
        push @{ $self->{"tables"} }, SQL::Statement::Table->new($_);
    }
    $self->{'NUM_OF_ROWS'} || '0E0';
}

sub CREATE ($$$) {
    my($self, $data, $params) = @_;
    my $names;
    # CREATE TABLE AS ...
    if (my $subquery = $self->{subquery}) {
         my $sth;
         # AS IMPORT
         if ($subquery =~ /^IMPORT/i) {
             $sth = $data->{Database}->prepare("SELECT * FROM $subquery");
             $sth->execute(@$params);
             $names  = $sth->{NAME};
         }
         # AS SELECT
         else {
             $sth = $data->{Database}->prepare($subquery);
             $sth->execute();
             $names  = $sth->{NAME};
         }
         $names = $sth->{NAME} unless defined $names;
         my $tbl_data = $sth->{f_stmt}->{data};
	 my $tbl_name = $self->tables(0)->name;
	 # my @tbl_cols = map {$_->name} $sth->{f_stmt}->columns;
         #my @tbl_cols=map{$_->name} $sth->{f_stmt}->columns if $sth->{f_stmt};
         my @tbl_cols;
#            @tbl_cols=@{ $sth->{NAME} } unless @tbl_cols;
         @tbl_cols=@{ $names } unless @tbl_cols;
         my $create_sql = "CREATE TABLE $tbl_name ";
            $create_sql = "CREATE TEMP TABLE $tbl_name "
                        if $self->{"is_ram_table"};
         my @coldefs = map { "$_ TEXT" } @tbl_cols;
         $create_sql .= '('.join(',',@coldefs).')';
	 $data->{Database}->do($create_sql);
         my $colstr    = ('?,')x@tbl_cols;
         my $insert_sql = "INSERT INTO $tbl_name VALUES($colstr)";
         $data->{Database}->do($insert_sql,{},@$_) for @$tbl_data;
         return (0,0);
    }
    my($eval,$foo) = $self->open_tables($data, 1, 1);
    return undef unless $eval;
    $eval->params($params);
    my($row) = [];
    my($col);
    my($table) = $eval->table($self->tables(0)->name());
    foreach $col ($self->columns()) {
        push(@$row, $col->name());
    }
    $table->push_names($data, $row);
    (0, 0);
}

sub CALL {
    my($self, $data, $params) = @_;
    my $dbh = $data->{Database};
    $self->{procedure}->{data} = $data;
    ( $self->{'NUM_OF_ROWS'}   ,
      $self->{'NUM_OF_FIELDS'} ,
      $self->{'data'}
    ) =  $self->get_row_value($self->{procedure});
}

sub DROP ($$$) {
    my($self, $data, $params) = @_;
    if ($self->{ignore_missing_table}) {
         eval { $self->open_tables($data,0,0) };
         if ($@ and $@ =~ /no such (table|file)/i ) {
             return (-1,0);
	 }
    }
    my($eval) = $self->open_tables($data, 0, 1);
#    return undef unless $eval;
    return (-1,0) unless $eval;
#    $eval->params($params);
    my($table) = $eval->table($self->tables(0)->name());
    $table->drop($data);
#use mylibs; zwarn $self->{f_stmt};
    (-1, 0);
}

sub INSERT ($$$) {
    my($self, $data, $params) = @_;
    my($eval,$all_cols) = $self->open_tables($data, 0, 1);
    return undef unless $eval;
    $eval->params($params);
    $self->verify_columns($data,$eval, $all_cols) if scalar ($self->columns());
    my($table) = $eval->table($self->tables(0)->name());
    $table->seek($data, 0, 2);
    my($array) = [];
    my($val, $col, $i);
    my($cNum) = scalar($self->columns());
    my $param_num = 0;
    if ($cNum) {
        # INSERT INTO $table (row, ...) VALUES (value, ...)
        for ($i = 0;  $i < $cNum;  $i++) {
            $col = $self->columns($i);
            $val = $self->row_values($i);
            if ($val and ref($val) eq 'SQL::Statement::Param') {
                $val = $eval->param($val->num());
          }
          elsif ($val and $val->{type} eq 'placeholder') {
                $val = $eval->param($param_num++);
	    }
            else {
	         $val = $self->get_row_value($val,$eval);
	    }
            $array->[$table->column_num($col->name())] = $val;
        }
    } else {
        return $self->do_err("Bad col names in INSERT");
    }
    $table->push_row($data, $array);
    (1, 0);
}

sub DELETE ($$$) {
    my($self, $data, $params) = @_;
    my($eval,$all_cols) = $self->open_tables($data, 0, 1);
    return undef unless $eval;
    $eval->params($params);
    $self->verify_columns($data,$eval, $all_cols);
    my($table) = $eval->table($self->tables(0)->name());
    my($affected) = 0;
    my(@rows, $array);
    if ( $table->can('delete_one_row') ) {
        while (my $array = $table->fetch_row($data)) {
            if ($self->eval_where($eval,'',$array)) {
                ++$affected;
                $array = $self->{fetched_value} if $self->{fetched_from_key};
                $table->delete_one_row($data,$array);
                return ($affected, 0) if $self->{fetched_from_key};
	      }
        }
        return ($affected, 0);
    }
    while ($array = $table->fetch_row($data)) {
        if ($self->eval_where($eval,'',$array)) {
            ++$affected;
        } else {
            push(@rows, $array);
        }
    }
    $table->seek($data, 0, 0);
    foreach $array (@rows) {
        $table->push_row($data, $array);
    }
    $table->truncate($data);
    ($affected, 0);
}

sub UPDATE ($$$) {
    my($self, $data, $params) = @_;
    my $valnum = $self->{num_val_placeholders};
    my @val_params;
    if ($valnum) {
         @val_params   = splice @$params, 0,$valnum;
#        @$params = (@$params,@val_params);

#         my @where_params = $params->[$valnum+1..scalar @$params-1];
#        @$params = (@where_params,@val_params);
    }
    my($eval,$all_cols) = $self->open_tables($data, 0, 1);
    return undef unless $eval;
    $eval->params($params);
    $self->verify_columns($data,$eval, $all_cols);
    my($table) = $eval->table($self->tables(0)->name());
    my $tname = $self->tables(0)->name();
    my($affected) = 0;
    my(@rows, $array, $f_array, $val, $col, $i);
    while ($array = $table->fetch_row($data)) {
        if ($self->eval_where($eval,$tname,$array)) {
            if( $self->{fetched_from_key} and $table->can('update_one_row') ){
                $array = $self->{fetched_value};
            }
        my $param_num =$arg_num;
    my $col_nums = $eval->{"tables"}->{"$tname"}->{"col_nums"} ;
    my $cols;
    %$cols   = reverse %{ $col_nums };
    my $rowhash;
    ####################################
    # Dan Wright
    ####################################
    # for (keys %$cols) {
    #    $rowhash->{$cols->{$_}} = $array->[$_];
    # }
    while (my($name, $number) = each %$col_nums ) {
        $rowhash->{$name} = $array->[$number];
    }
    ####################################

            for ($i = 0;  $i < $self->columns();  $i++) {
                $col = $self->columns($i);
                $val = $self->row_values($i);
                if (ref($val) eq 'SQL::Statement::Param') {
#                    $val = $eval->param($val->num());
                    $val = shift @val_params;
                }
                elsif ($val->{type} eq 'placeholder') {
#                    $val = $eval->param($param_num++);
                    $val = shift @val_params;
	        }
                else {
     	            $val = $self->get_row_value($val,$eval,$rowhash);
	        }
                $array->[$table->column_num($col->name())] = $val;
            }
            ++$affected;
        }
        if ($self->{fetched_from_key}){
            $table->update_one_row($data,$array);
            return ($affected, 0);
        }
        push(@rows, $array);
    }
    $table->seek($data, 0, 0);
    foreach $array (@rows) {
        $table->push_row($data, $array);
    }
    $table->truncate($data);
    ($affected, 0);
}

sub find_join_columns {
    my $self = shift;
    my @all_cols = @_;
    my $display_combine = 'NONE';
    $display_combine = 'NATURAL' if $self->{"join"}->{"type"} =~ /NATURAL/;
    $display_combine = 'USING'   if $self->{"join"}->{"clause"} =~ /USING/;
    $display_combine = 'NAMED' if !$self->{"asterisked_columns"};
    my @display_cols;
    my @keycols = ();
    @keycols = @{ $self->{"join"}->{"keycols"} } if $self->{"join"}->{"keycols"};
    @keycols  = map {s/\./$dlm/; $_} @keycols;
    my %is_key_col;
    %is_key_col = map { $_=> 1 } @keycols;

    # IF NAMED COLUMNS, USE NAMED COLUMNS
    #
    if ($display_combine eq 'NAMED') {
        @display_cols =  $self->columns;
#
#	DAA
#	need to get to $self's table objects to get the name
#
#        @display_cols = map {$_->table . $dlm . $_->name} @display_cols;
#        @display_cols = map {$_->table->{NAME} . $dlm . $_->name} @display_cols;

		my @tbls = $self->tables;
		my %tables = ();
		
		$tables{$_->name} = $_
			foreach (@tbls);
		
		foreach (0..$#display_cols) {
        	$display_cols[$_] = 
        		($display_cols[$_]->table ?
        			$tables{$display_cols[$_]->table}->name : '') . 
        			$dlm . $display_cols[$_]->name;
        }
    }

    # IF ASTERISKED COLUMNS AND NOT NATURAL OR USING
    # USE ALL COLUMNS, IN ORDER OF NAMING OF TABLES
    #
    elsif ($display_combine eq 'NONE') {
        @display_cols =  @all_cols;
    }

    # IF NATURAL, COMBINE ALL SHARED COLUMNS
    # IF USING, COMBINE ALL KEY COLUMNS
    #
    else  {
        my %is_natural;
        for my $full_col(@all_cols) {
            my($table,$col) = $full_col =~ /^([^$dlm]+)$dlm(.+)$/;
            next if $display_combine eq 'NATURAL' and $is_natural{$col};
            next if $display_combine eq 'USING' and $is_natural{$col} and
                 $is_key_col{$col};
            push @display_cols,  $full_col;
            $is_natural{$col}++;
        }
    }
    my @shared = ();
    my %is_shared;
    if ($self->{"join"}->{"type"} =~ /NATURAL/ ) {
        for my $full_col(@all_cols) {
            my($table,$col) = $full_col =~ /^([^$dlm]+)$dlm(.+)$/;
            push @shared, $col if  $is_shared{$col}++;
        }
    }
    else {
        @shared = @keycols;
        # @shared = map {s/^[^_]*_(.+)$/$1/; $_} @keycols;
        # @shared = grep !$is_shared{$_}++, @shared
    }
    $self->{"join"}->{"shared_cols"} = \@shared;
    $self->{"join"}->{"display_cols"} = \@display_cols;
}

sub JOIN {
    my($self, $data, $params) = @_;
    if ($self->{"join"}->{"type"} =~ /RIGHT/ ) {
        my @tables = $self->tables;
        $self->{"tables"}->[0] = $tables[1];
        $self->{"tables"}->[1] = $tables[0];
    }
    my($eval,$all_cols) = $self->open_tables($data, 0, 0);
    return undef unless $eval;
    $eval->params($params);
    $self->verify_columns( $data,$eval, $all_cols );
    if ($self->{"join"}->{"keycols"} 
     and $self->{"join"}->{"table_order"}
     and scalar @{$self->{"join"}->{"table_order"}} == 0
    ) {
        $self->{"join"}->{"table_order"} = $self->order_joins(
            $self->{"join"}->{"keycols"}
        );
    }
    my  @tables = $self->tables;
    # GET THE LIST OF QUALIFIED COLUMN NAMES FOR DISPLAY
    # *IN ORDER BY NAMING OF TABLES*
    #
    my @all_cols;
    for my $table(@tables) {
        my @cols = @{ $eval->table($table->{name})->col_names };
        for (@cols) {
            push @all_cols, $table->{name} . $dlm . $_;
#            push @all_cols, $table . $dlm . $_;
	}
    }
    $self->find_join_columns(@all_cols);

    # JOIN THE TABLES
    # *IN ORDER *BY JOINS*
    #
    @tables = @{ $self->{"join"}->{"table_order"} }
           if $self->{"join"}->{"table_order"}
           and $self->{"join"}->{"type"} !~ /RIGHT/;
    my $tableA;
    my $tableB;
    $tableA = shift @tables;
    $tableB = shift @tables;
    $tableA = $tableA->{name} if ref $tableA;
    $tableB = $tableB->{name} if ref $tableB;
    my $tableAobj = $eval->table($tableA);
    my $tableBobj = $eval->table($tableB);
    $tableAobj->{NAME} ||= $tableA;
    $tableBobj->{NAME} ||= $tableB;
    $self->join_2_tables($data,$params,$tableAobj,$tableBobj);
    for my $next_table(@tables) {
        $tableAobj = $self->{"join"}->{"table"};
        $tableBobj = $eval->table($next_table);
        $tableBobj->{"NAME"} ||= $next_table;
        $self->join_2_tables($data,$params,$tableAobj,$tableBobj);
        $self->{"cur_table"} = $next_table;
    }
    return $self->SELECT($data,$params);
}

sub join_2_tables {
    my($self, $data, $params, $tableAobj, $tableBobj) = @_;
    my $tableA = $tableAobj->{"NAME"};
    my $tableB = $tableBobj->{"NAME"};
    my $share_type = 'IMPLICIT';
    $share_type    = 'NATURAL' if $self->{"join"}->{"type"} =~ /NATURAL/;
    $share_type    = 'USING'   if $self->{"join"}->{"clause"} =~ /USING/;
    $share_type    = 'ON' if $self->{"join"}->{"clause"} =~ /ON/;
    $share_type    = 'USING' if $share_type eq 'ON'
                            and scalar @{ $self->{join}->{keycols} } == 1;
    my $join_type  = 'INNER';
    $join_type     = 'LEFT'  if $self->{"join"}->{"type"} =~ /LEFT/;
    $join_type     = 'RIGHT' if $self->{"join"}->{"type"} =~ /RIGHT/;
    $join_type     = 'FULL'  if $self->{"join"}->{"type"} =~ /FULL/;
    my @colsA = @{$tableAobj->col_names};
    my @colsB = @{$tableBobj->col_names};
    my %iscolA = map { $_=>1} @colsA;
    my %iscolB = map { $_=>1} @colsB;
    my %isunqualA = map { $_=>1} @colsA;
    my %isunqualB = map { $_=>1} @colsB;
    my @shared_cols;
    my %is_shared;
    my @tmpshared = @{ $self->{"join"}->{"shared_cols"} };
    if ($share_type eq 'ON' and $join_type eq 'RIGHT') {
        @tmpshared = reverse @tmpshared;
    }
    if ($share_type eq 'USING') {
        for (@tmpshared) {
             push @shared_cols, $tableA . $dlm . $_;
             push @shared_cols, $tableB . $dlm . $_;
        }
    }
    if ($share_type eq 'NATURAL') {
        for my $c(@colsA) {
            $c =~ s/^[^$dlm]+$dlm(.+)$/$1/ if $tableA eq "${dlm}tmp";
 	    if ($iscolB{$c}) {
                push @shared_cols, $tableA . $dlm . $c;
                push @shared_cols, $tableB . $dlm . $c;
	    }
        }
    }
    my @all_cols = map { $tableA . $dlm . $_ } @colsA;
    @all_cols = ( @all_cols, map { $tableB . $dlm . $_ } @colsB);
    @all_cols = map { s/${dlm}tmp$dlm//; $_; } @all_cols;
    if ($tableA eq "${dlm}tmp") {
        #@colsA = map {s/^[^_]+_(.+)$/$1/; $_; } @colsA;
    }
    else {
        @colsA = map { $tableA . $dlm . $_ } @colsA;
    }
    @colsB = map { $tableB . $dlm . $_ } @colsB;
    my %isa;
    my $i=0;
    my $col_numsA = { map { $_=>$i++}  @colsA };
    $i=0;
    my $col_numsB = { map { $_=>$i++} @colsB };
    %iscolA = map { $_=>1} @colsA;
    %iscolB = map { $_=>1} @colsB;
    my @blankA = map {undef} @colsA;
    my @blankB = map {undef} @colsB;
    if ($share_type =~/^(ON|IMPLICIT)$/ ) {
        while (@tmpshared) {
            my $k1 = shift @tmpshared;
            my $k2 = shift @tmpshared;
            next unless ($iscolA{$k1} or $iscolB{$k1});
            next unless ($iscolA{$k2} or $iscolB{$k2});
            next if !$iscolB{$k1} and !$iscolB{$k2};
            my($t,$c) = $k1 =~ /^([^$dlm]+)$dlm(.+)$/;
            next if !$isunqualA{$c};
            push @shared_cols, $k1 unless $is_shared{$k1}++;
            ($t,$c) = $k2 =~ /^([^$dlm]+)$dlm(.+)$/;
            next if !$isunqualB{$c};
            push @shared_cols, $k2 if !$is_shared{$k2}++;
        }
    }
    %is_shared = map {$_=>1} @shared_cols;
    for my $c(@shared_cols) {
      if ( !$iscolA{$c} and !$iscolB{$c} ) {
          $self->do_err("Can't find shared columns!");
      }
    }
    my($posA,$posB)=([],[]);
    for my $f(@shared_cols) {
         push @$posA, $col_numsA->{$f} if $iscolA{$f};
         push @$posB, $col_numsB->{$f} if $iscolB{$f};
    }
#use mylibs; zwarn $self->{join};
    # CYCLE THROUGH TABLE B, CREATING A HASH OF ITS VALUES
    #
    my $hashB={};
    while (my $array = $tableBobj->fetch_row($data)) {
        my $has_null_key=0;
        my @key_vals = @$array[@$posB];
        for (@key_vals) { next if defined $_; $has_null_key++; last; }
        next if $has_null_key and  $join_type eq 'INNER';
        my $hashkey = join ' ',@key_vals;
        push @{$hashB->{"$hashkey"}}, $array;
    }
    # CYCLE THROUGH TABLE A
    #
    my $joined_table;
    my %visited;
    while (my $arrayA = $tableAobj->fetch_row($data)) {
        my $has_null_key = 0;
        my @key_vals = @$arrayA[@$posA];
        for (@key_vals) { next if defined $_; $has_null_key++; last; }
        next if ($has_null_key and  $join_type eq 'INNER');
        my $hashkey = join ' ',@key_vals;
        my $rowsB = $hashB->{"$hashkey"};
        if (!defined $rowsB and $join_type ne 'INNER' ) {
            push @$rowsB, \@blankB;
	}
        for my $arrayB(@$rowsB) {
            my @newRow = (@$arrayA,@$arrayB);
            if ($join_type ne 'UNION' ) {
                 push @$joined_table,\@newRow;
             }
        }
        $visited{$hashkey}++; #        delete $hashB->{"$hashkey"};
    }

    # ADD THE LEFTOVER B ROWS IF NEEDED
    #
    if ($join_type=~/(FULL|UNION)/) {
      while (my($k,$v)= each%$hashB) {
         next if $visited{$k};
 	 for my $rowB(@$v) {
             my @arrayA;
             my @tmpB;
             my $rowhash;
             @{$rowhash}{@colsB}=@$rowB;
             for my $c(@all_cols) {
                 my($table,$col) = $c =~ /^([^$dlm]+)$dlm(.+)/;
                 push @arrayA,undef if $table eq $tableA;
                 push @tmpB,$rowhash->{$c} if $table eq $tableB;
	     }
             @arrayA[@$posA]=@tmpB[@$posB] if $share_type =~ /(NATURAL|USING)/;
             my @newRow = (@arrayA,@tmpB);
             push @$joined_table, \@newRow;
	 }
      }
    }
    undef $hashB;
    undef $tableAobj;
    undef $tableBobj;
    $self->{"join"}->{"table"} =
        SQL::Statement::TempTable->new(
            $dlm . 'tmp',
            \@all_cols,
            $self->{"join"}->{"display_cols"},
            $joined_table
    );
}

sub run_functions{
    my($self, $data, $params) = @_;
    my @row=();
    for my $col($self->columns) {
        my $val =  $self->get_row_value(
 	    $self->{computed_column}->{$col->name}->{function}
					
        );
       push @row, $val;
    }
    (1,scalar @row, [\@row]);
}

sub SELECT ($$) {
    my($self, $data, $params) = @_;
    $self->{"params"} ||= $params;
    return $self->run_functions($data,$params) 
           if @{$self->{table_names}} == 0;
    my($eval,$all_cols,$tableName,$table);
    if (defined $self->{"join"} ) {
        return $self->JOIN($data,$params) if !defined $self->{"join"}->{"table"};
        $tableName = $dlm . 'tmp';
        $table     = $self->{"join"}->{"table"};
    }
    else {
        ($eval,$all_cols) = $self->open_tables($data, 0, 0);
        return undef unless $eval;
        $eval->params($params);
        $self->verify_columns( $data,$eval, $all_cols );
        $tableName = $self->tables(0)->name();
        $table = $eval->table($tableName);
    }
    my $rows = [];

    # In a loop, build the list of columns to retrieve; this will be
    # used both for fetching data and ordering.
    my($cList, $col, $tbl, $ar, $i, $c);
    my $numFields = 0;
    my %columns;
    my @names;
    my %funcs =();
#
#	DAA
#
#	lets just disable this and see where it leads...
#
#    if ($self->{"join"}) {
#          @names = @{ $table->col_names };
#          for my $col(@names) {
#             $columns{$tableName}->{"$col"} = $numFields++;
#             push(@$cList, $table->column_num($col));
#          }
#    }
#    else {
        foreach my $column ($self->columns()) {
            if (ref($column) eq 'SQL::Statement::Param') {
                my $val = $eval->param($column->num());
                if ($val =~ /(.*)\.(.*)/) {
                    $col = $1;
                    $tbl = $2;
                } else {
                    $col = $val;
                    $tbl = $tableName;
                }
            } else {
                ($col, $tbl) = ($column->name(), $column->table());
        	}
        	if ($col eq '*') {
        	    $ar = $table->col_names();
        	    for ($i = 0;  $i < @$ar;  $i++) {
        	        my $cName = $ar->[$i];
        	        $columns{$tbl}->{"$cName"} = $numFields++;
        	        $c = SQL::Statement::Column->new({'table' => $tableName,
						'column' => $cName});
					push(@$cList, $i);
	                push(@names, $cName);
	            }
	        } else {
	            $tbl ||= '';
	            $columns{$tbl}->{"$col"} = $numFields++;
            #
            # handle functions in select list
            #
#
#	DAA
#
#	check for a join temp table; if so, check if we can locate
#	the column in its delimited set
#
	            my $cnum = (($tableName eq '~tmp') && ($tbl ne '')) ?
	            	$table->column_num($tbl . $dlm . $col) :
	            	$table->column_num($col);

	            if (!defined $cnum) {
	                 $funcs{$col} = $column->{function};
	                 $cnum = $col;
			    }
	            push(@$cList, $cnum );
            # push(@$cList, $table->column_num($col));
	            push(@names, $col);
	        }
	    }
#    }
    $cList = [] unless defined $cList;
    $self->{'NAME'} = \@names;
    if ($self->{"join"}) {
        @{$self->{'NAME'}} = map { s/^[^$dlm]+$dlm//; $_} @names;
    }
    $self->verify_order_cols($table);
    my @order_by = $self->order();
    my @extraSortCols = ();
    my $distinct = $self->distinct();
    if ($distinct) {
        # Silently extend the ORDER BY clause to the full list of
        # columns.
        my %ordered_cols;
        foreach my $column (@order_by) {
            ($col, $tbl) = ($column->column(), $column->table());
            $tbl ||= $self->colname2table($col);
            $ordered_cols{$tbl}->{"$col"} = 1;
        }
        while (my($tbl, $cref) = each %columns) {
            foreach my $col (keys %$cref) {
                if (!$ordered_cols{$tbl}->{"$col"}) {
                    $ordered_cols{$tbl}->{"$col"} = 1;
                    push(@order_by,
                         SQL::Statement::Order->new
                         ('col' => SQL::Statement::Column->new
                          ({'table' => $tbl,
                            'column' => $col}),
                          'desc' => 0));
                }
            }
        }
    }
    if (@order_by) {
        my $nFields = $numFields;
        # It is possible that the user gave an ORDER BY clause with columns
        # that are not part of $cList yet. These columns will need to be
        # present in the array of arrays for sorting, but will be stripped
        # off later.
        my $i=-1;
        foreach my $column (@order_by) {
            $i++;
            ($col, $tbl) = ($column->column(), $column->table());
            my $pos;
            if ($self->{"join"}) {
                  $tbl ||= $self->colname2table($col);
                  $pos = $table->column_num($tbl."$dlm$col");
                  if (!defined $pos) {
                  $tbl = $self->colname2table($col);
                  $pos = $table->column_num($tbl."_$col");
		  }
	    }
            $tbl ||= $self->colname2table($col);
            next if exists($columns{$tbl}->{"$col"});
            $pos = $table->column_num($col) unless defined $pos;
            push(@extraSortCols, $pos);
            $columns{$tbl}->{"$col"} = $nFields++;
        }
    }
    my $e = $eval;
    if ($self->{"join"}) {
          $e = $table;
    }

    # begin count for limiting if there's a limit clasue and no order clause
    #
    my $limit_count = 0 if $self->limit and !$self->order;
    my $row_count = 0;
    my $offset = $self->offset || 0;
    while (my $array = $table->fetch_row($data)) {
        if (eval_where($self,$e,$tableName,$array,\%funcs)) {
            next if defined($limit_count) and $row_count++ < $offset;
            $limit_count++ if defined $limit_count;
            $array = $self->{fetched_value} if $self->{fetched_from_key};

            # Note we also include the columns from @extraSortCols that
            # have to be ripped off later!
            @extraSortCols = () unless @extraSortCols;
            my @row = map {  (defined $_ and /^\d+$/ and defined $array->[$_])
                          ? $array->[$_]
                          : $self->{func_vals}->{$_} ;
                          } (@$cList, @extraSortCols);
            push(@$rows, \@row);

            # We quit here if its a primary key search
            # or if there's a limit clause without order clause
            # and the limit has been reached
            #
            return (scalar(@$rows),$numFields,$rows)
 	        if $self->{fetched_from_key}
                or (defined($limit_count) and $limit_count >= $self->limit);
        }
    }
    if (@order_by) {
        my @sortCols = map {
            my $col = $_->column();
            my $tbl = $_->table();
 	    if ($self->{"join"}) {
               $tbl = 'shared' if $table->is_shared($col);
               $tbl ||= $self->colname2table($col);
	    }
             $tbl ||= $self->colname2table($col);
             ($columns{$tbl}->{"$col"}, $_->desc())
        } @order_by;
        #die "\n<@sortCols>@order_by\n";
        my($c, $d, $colNum, $desc);
        my $sortFunc = sub {
            my $result;
            $i = 0;
            do {
                $colNum = $sortCols[$i++];
                $desc = $sortCols[$i++];
                $c = $a->[$colNum];
                $d = $b->[$colNum];
                if (!defined($c)) {
                    $result = defined $d ? -1 : 0;
                } elsif (!defined($d)) {
                    $result = 1;
	        } elsif ( is_number($c,$d) ) {
#	        } elsif ( $c =~ $numexp && $d =~ $numexp ) {
                    $result = ($c <=> $d);
                } else {
  		    if ($self->{"case_fold"}) {
                        $result = lc $c cmp lc $d || $c cmp $d;
		    }
                    else {
                        $result = $c cmp $d;
		    }
                }
                if ($desc) {
                    $result = -$result;
                }
            } while (!$result  &&  $i < @sortCols);
            $result;
        };
        if ($distinct) {
            my $prev;
            @$rows = map {
                if ($prev) {
                    $a = $_;
                    $b = $prev;
                    if (&$sortFunc() == 0) {
                        ();
                    } else {
                        $prev = $_;
                    }
                } else {
                    $prev = $_;
                }
            } ($] > 5.00504 ?
               sort $sortFunc @$rows :
               sort { &$sortFunc } @$rows);
        } else {
            @$rows = $] > 5.00504 ?
                (sort $sortFunc @$rows) :
                (sort { &$sortFunc } @$rows)
        }

        # Rip off columns that have been added for @extraSortCols only
        if (@extraSortCols) {
            foreach my $row (@$rows) {
                splice(@$row, $numFields, scalar(@extraSortCols));
            }
        }
    }
#
#	DAA
#
#	why is this needed at this point ?
#	shouldn't we just use the names as given ?
#	or do we need to provide fully qualified names ?
#
#       JZ : this is needed for all explicit joins, to trim the fields
#
    my $has_computed_columns = scalar keys %{$self->{computed_column}}
       if $self->{computed_column};
    if ( $self->{"join"}
     and $self->{asterisked_columns}
     and !$has_computed_columns) {
        my @final_cols = @{$self->{"join"}->{"display_cols"}};
        @final_cols = map {$table->column_num($_)} @final_cols;
        my @names = map { $self->{"NAME"}->[$_]} @final_cols;
        $numFields = scalar @names;
        $self->{"NAME"} = \@names;
        my $i = -1;
        for my $row(@$rows) {
            $i++;
            @{ $rows->[$i] } = @$row[@final_cols];
        }
    }
    elsif ($self->{"join"} and $has_computed_columns) {
        my @final_cols = map {$table->column_num($_)}
        @{$self->{"join"}->{"display_cols"}};
        my %col_map = ();

        $col_map{$self->{NAME}[$_]} = $_
            foreach (0..$#{$self->{NAME}});

        foreach (0..$#final_cols) {
            next if defined($final_cols[$_]);
#
#    must be computed column, lookup its position in
#    row
#
        my $col = $self->{join}{display_cols}[$_];
        $col = substr($col, 1)
            if (substr($col, 0, 1) eq $dlm);
        $final_cols[$_] = $col_map{$col}; 
        }
    }
###################################################################
    if ( defined $self->limit ) {
        my $offset = $self->offset || 0;
        my $limit  = $self->limit  || 0;
        @$rows = splice @$rows, $offset, $limit;
    }
    return $self->group_by($rows) if $self->{group_by};
    if ($self->{"set_function"}) {
        my $numrows = scalar( @$rows );
        my $numcols = scalar @{ $self->{"NAME"} };
        my $i=0;
        my %colnum = map {$_=>$i++} @{ $self->{"NAME"} };
        for my $i(0 .. scalar @{$self->{"set_function"}} -1 ) {
            my $arg = $self->{"set_function"}->[$i]->{"arg"};
            $self->{"set_function"}->[$i]->{"sel_col_num"} = $colnum{$arg} if defined $arg and defined $colnum{$arg};
        }
        my($name,$arg,$sel_col_num);
        my @set;
        my $final=0;
        $numrows=0;
        my @final_row = map {undef} @{$self->{"set_function"}};
  #      my $start;
        for my $c(@$rows) {
            $numrows++;
            my $sf_index = -1;
 	    for my $sf(@{$self->{"set_function"}}) {
              $sf_index++;
	      if ($sf->{arg} and $sf->{"arg"} eq '*') {
                  $final_row[$sf_index]++;
	      }
              else {
                my $cn = $sf->{"sel_col_num"};
                my $v = $c->[$cn] if defined $cn;
                my $name = $sf->{"name"};
                next unless defined $v;
                my $final = $final_row[$sf_index];
                $final++      if $name =~ /COUNT/;
#                $final += $v  if $name =~ /SUM|AVG/;
                if( $name =~ /SUM|AVG/) {
                    return $self->do_err("Can't use $name on a string!")
#                      unless $v =~ $numexp;
                      unless is_number($v);
                    $final += $v;
                }
                #
                # Thanks Dean Kopesky dean.kopesky@reuters.com
                # submitted patch to make MIN/MAX do cmp on strings
                # and == on numbers
                #
                # but thanks also to Michael Kovacs mkovacs@turing.une.edu.au
                # for catching a problem when a MAX column is 0
                # necessitating !$final instead of ! defined $final
                #
                $final  = $v  if !$final
                              or ( $name eq 'MAX'
                                   and $v
                                   and $final
                                   and anycmp($v,$final) > 0
                                 );
                $final  = $v  if !$final
                              or ( $name eq 'MIN' 
                                   and defined $v
                                   and anycmp($v,$final) < 0
                                  );
                $final_row[$sf_index] = $final;
	      }
	    }
	}
        for my $i(0..$#final_row) {
	  if ($self->{"set_function"}->[$i]->{"name"} eq 'AVG') {
              $final_row[$i] = $final_row[$i]/$numrows;
	  }
	}
        return ( $numrows, scalar @final_row, [\@final_row]);
    }
    (scalar(@$rows), $numFields, $rows);
}
sub group_by {
    my($self,$rows)=@_;
#    my @columns_requested = map {$_->name} @{$self->{columns}};
    my $columns_requested = $self->{columns};
    my $numcols=scalar(@$columns_requested);
#    my $numcols=scalar(@{$self->{"set_function"}});
    my $i=0;
    my %colnum = map {$_=>$i++} @{ $self->{"NAME"} };
#    my %colnum = map {$_=>$i++} @columns_requested;
    my $set_cols;

    my @all_cols=();
    my $set_columns = $self->{set_function};
    for my $c1(@$columns_requested) {
        for my $c2(@$set_columns) {
#            printf "%s %s\n",$c1->{name}, $c2->{arg};
            next unless uc $c1->{name} eq uc $c2->{arg};
            $c1->{arg}= $c2->{arg};
            $c1->{name}= $c2->{name};
            last;
        }
        push @all_cols,$c1;
    }
#    $self->{set_function}=\@all_cols;

    for (@{ $self->{"set_function"} }) {
       push @$set_cols, $_->{name};
    }
    my($keycol,$keynum);
    for my $i(0 .. $numcols-1 ) {
        my $arg = $self->{"set_function"}->[$i]->{"arg"};
#         print $self->{NAME}->[$i],$arg,"\n";
        if (!$arg ) {
            $arg =$set_cols->[$i];
#            $arg =$columns_requested[$i];
            $keycol = $self->{"set_function"}->[$i]->{"name"};
            $keynum =  $colnum{uc $arg};
        }
        $self->{"set_function"}->[$i]->{"sel_col_num"} = $colnum{uc $arg};
    }

    my $display_cols = $self->{"set_function"};
    my $numFields    = scalar(@$display_cols);
#    my $keyfield = $self->{group_by}->[0];
#    my $keynum=0;
#    for my$i(0..$#{$display_cols}) {
#        $keynum=$i if uc $display_cols->[$i]->{name} eq uc $keyfield;
#printf "%s.%s,%s\n",$i,$display_cols->[$i]->{name},$keyfield;
#    }
    my $g = SQL::Statement::Group->new($keynum,$display_cols,$rows);
    $rows = $g->calc;
    my $x = [ map {$_->{name}} @$display_cols ];
    $self->{NAME} = [ map {$_->{name}} @$display_cols ];
    %{$self->{ORG_NAME}} = map {
        my $n = $_->{name};
        $n .= "_" . $_->{arg} if $_->{arg};
        $_->{name}=>$n;
    } @$display_cols ;
    return (scalar(@$rows), $numFields, $rows);
}
sub anycmp($$) {
    my ($a,$b) = @_;
    $a = '' unless defined $a;
    $b = '' unless defined $b;
#    return ($a =~ $numexp && $b =~ $numexp)
    return ( is_number($a,$b) )
        ? ($a <=> $b)
        : ($a cmp $b);
}

sub eval_where {
    my $self   = shift;
    my $eval   = shift;
    my $tname  = shift;
    my $rowary = shift;
    my $funcs  = shift || ();
    $tname ||= $self->tables(0)->name();
    my $cols;
    my $col_nums;
	$col_nums = $self->{join} ? $eval->{col_nums}
                                  : $eval->{tables}->{$tname}->{col_nums} ;

    %$cols   = reverse %{ $col_nums };
    ####################################
    # Dan Wright
    ####################################
    my $rowhash;
    while (my($name, $number) = each %$col_nums ) {
        $rowhash->{$name} = $rowary->[$number];
    }
    ####################################
    my($f,$fval);
    $rowhash->{$f} = $self->{func_vals}->{$f}
                   = $self->get_row_value( $fval, $eval, $rowhash )
		while ( ($f,$fval) = each %$funcs);

    my @truths;
    $arg_num=0;  # set placeholder start
    my $where = $self->{"where_clause"} || return 1;
    my $match = $self->process_predicate ($where,$eval,$rowhash);

    # we set the $new_execute flag to 0 to allow reuse;
    # it's set to 1 at the start of execute()
    # we set it here because all predicates have been processed
    # at this point
    #
    $new_execute=0;  # we don't need to get the reuse values again;

    return $match;
}


sub process_predicate {
    my($self,$pred,$eval,$rowhash) = @_;
    if ($pred->{op}eq'USER_DEFINED' and !$pred->{arg2}) {
        my $match = $self->get_row_value( $pred->{"arg1"}, $eval, $rowhash );
        if ($pred->{"neg"}) {
           $match = $match ? 0 : 1;
        }
        return $match;
    }
    if ($pred->{op} eq 'OR') {
        #
        # From Dan Wright
        #
	# Demorgan's law:  ^(A OR B) = ^A AND ^B
        #
	# We can always return if match1 is true.   We short-circuit the OR
	# with a true value, or short-circuit the AND with a false value on
        # negation.
	my $match1 = process_predicate($self,$pred->{"arg1"},$eval,$rowhash);
	return $pred->{'neg'} ? 0 : 1 if $match1;

	my $match2 = process_predicate($self,$pred->{"arg2"},$eval,$rowhash);

	# Same logic applies for short-circuit on the second argument.
	return $pred->{'neg'} ? 0 : 1 if $match2;

	# Neither short circuit caught, both arguments were false.
	return $pred->{'neg'} ? 1 : 0;
    }
    elsif ($pred->{op} eq 'AND') {
        my $match1 = process_predicate($self,$pred->{"arg1"},$eval,$rowhash);
        if ($pred->{"neg"}) {
	    return 1 unless $match1;
        }
        else {
	    return 0 unless $match1;
	}
        my $match2 = process_predicate($self,$pred->{"arg2"},$eval,$rowhash);
        if ($pred->{"neg"}) {
            return $match2 ? 0 : 1;
        }
        else {
	    return $match2 ? 1 : 0;
	}
    }
    else {

        # The old way, now replaced, called get_row_value everytime
        #
         my $val1 = $self->get_row_value( $pred->{"arg1"}, $eval, $rowhash );
         my $val2 = $self->get_row_value( $pred->{"arg2"}, $eval, $rowhash );

        # define types that we only need to call get_row_value on once
        # per execute
        #
##        my %is_value = map {$_=>1} qw(placeholder string number null);

        # use a reuse value if defined, get_row_value() otherwise
        #
        # except we ignore the reuse value if this is the first pass
        # on an execute() since placeholders need to be reset on the
        # first pass
        #
        # $new_execute is set to 1 at the start of execute()
        # and set to 0 at the end of  eval_where()
        #

##        my $val1 = (!$new_execute and defined $pred->{arg1}->{reuse})
##                 ? $pred->{arg1}->{reuse}
##	         : $self->get_row_value( $pred->{"arg1"}, $eval, $rowhash );
##        my $val2 = (!$new_execute and defined $pred->{arg2}->{reuse})
##                 ? $pred->{arg2}->{reuse}
##	         : $self->get_row_value( $pred->{"arg2"}, $eval, $rowhash );

        # the first time we call get_row_value, we set the reuse value
        # for the argument object with its scalar value
        #
##        my $type1 = $pred->{arg1}->{type} if ref($pred->{arg1}) eq 'HASH';
##        my $type2 = $pred->{arg2}->{type} if ref($pred->{arg2}) eq 'HASH';
##	$pred->{arg1}->{reuse} = $val1
##                              if $type1 and $is_value{$type1} and $new_execute;
##	$pred->{arg2}->{reuse} = $val2
##                              if $type2 and $is_value{$type2} and $new_execute;

        my $op     = $pred->{op};
        my $opfunc = $op;
        #
        # currently we treat NULL and '' as the same
        # eventually fix
        #
    	if ( $op !~ /^IS/i and (
       	      !defined $val1 or $val1 eq '' or
       	      !defined $val2 or $val2 eq '' 
  	)) {
              $op = $s2pops->{"$op"}->{'s'};
	}
        elsif (defined $val1 and defined $val2 and $op !~ /^IS/i ) {
              $op = ( is_number($val1) and is_number($val2) )
                  ? $s2pops->{"$op"}->{'n'}
                  : $s2pops->{"$op"}->{'s'};
	}
        my $neg = $pred->{"neg"};
        my $table_type = ref($eval);
        if ($table_type !~ /TempTable/) {
#        if (ref $eval !~ /TempTable/) {
            my($table) = $eval->table($self->tables(0)->name());
            if ($pred->{op} eq '=' and !$neg and $table->can('fetch_one_row')){
                my $key_col = $table->fetch_one_row(1,1);
                if ($pred->{arg1}->{value} =~ /^$key_col$/i) {
                    $self->{fetched_from_key}=1;
                    $self->{fetched_value} = $table->fetch_one_row(0,$val2);
                    return 1;
	        }
            }
	}
#       my $match = $self->is_matched($val1,$op,$val2) || 0;
       my $match;
        if ($op) {
            $match = $self->is_matched($val1,$op,$val2) || 0;
	}
        else {
            my $sub = $self->{opts}->{function_defs}->{uc $opfunc}->{sub}->{value};
            my $func = $self->{loaded_function}->{uc $opfunc}
              ||= SQL::Statement::Util::Function->new(
                      uc $opfunc, $sub, [$val1,$val2]
                  );
            $func->{args}=[$val1,$val2];
	    $match = $self->get_row_value( $func, $eval, $rowhash );
	}
        if ($pred->{"neg"}) {
           $match = $match ? 0 : 1;
        }
        return $match;
    }
}

sub is_matched {
    my($self,$val1,$op,$val2)=@_;
    return (!defined $val1 or $val1 eq '') ? 1 : 0 if ($op eq 'IS');
    $val1 = '' unless defined $val1;
    $val2 = '' unless defined $val2;
    return undef unless (defined $val1 and defined $val2);
    $val2 = quotemeta($val2),
    $val2 =~ s/\\%/.*/g,
    $val2 =~ s/_/./g
        if (($op eq 'LIKE') || ($op eq 'CLIKE'));
    return 0 if ( !$self->{"alpha_compare"} && (
        ($op eq 'lt') ||
 	($op eq 'gt') ||
    	($op eq 'le') ||
    	($op eq 'ge')
    ));
    return ($op eq 'LIKE' ) ? ($val1 =~ /^$val2$/s) :
        ($op eq 'CLIKE' )   ? ($val1 =~ /^$val2$/si) :
    	($op eq 'RLIKE' )   ? ($val1 =~ /$val2/is) :
    	($op eq '<' )       ? ($val1 <  $val2) :
    	($op eq '>' )       ? ($val1 >  $val2) :
    	($op eq '==')       ? ($val1 == $val2) :
    	($op eq '!=')       ? ($val1 != $val2) :
    	($op eq '<=')       ? ($val1 <= $val2) :
    	($op eq '>=')       ? ($val1 >= $val2) :
    	($op eq 'lt')       ? ($val1 lt $val2) :
    	($op eq 'gt')       ? ($val1 gt $val2) :
    	($op eq 'eq')       ? ($val1 eq $val2) :
    	($op eq 'ne')       ? ($val1 ne $val2) :
    	($op eq 'le')       ? ($val1 le $val2) :
    	($op eq 'ge')       ? ($val1 ge $val2) :
    	0;
}

sub fetch {
    my($self) = @_;
    $self->{data} ||= [];
    my $row = shift @{ $self->{data} };
    return undef unless $row and scalar @$row;
    return $row;
}
sub open_tables {
    my($self, $data, $createMode, $lockMode) = @_;
    my @call = caller 4;
    my $caller = $call[3];
    if ($caller) {
        $caller =~ s/^([^:]*::[^:]*)::.*$/$1/;
    }
    my @c;
    my $t;
    my $is_col;
    my @tables = $self->tables;
    my $count=-1;
    for ( @tables) {
        $count++;
        my $name = $_->{"name"};
        if( $name =~ /^(.+)\.([^\.]+)$/ ) {
            my $schema = $1;  # ignored
            $name = $_->{name} = $2;
        }
        if (my $u_func = $self->{"table_func"}->{ uc $name}) {
            $t->{"$name"} = $self->get_user_func_table($name,$u_func);
	}
        elsif ($data->{Database}->{sql_ram_tables}->{uc $name}) {
            $t->{"$name"} = $data->{Database}->{sql_ram_tables}->{uc $name};
            $t->{"$name"}->{index}=0;
 	    $t->{$name}->init_table($data,$name,$createMode,$lockMode)
                if $t->{$name}->can('init_table');
	}
        elsif ( $self->{"is_ram_table"} or !($self->can('open_table'))) {
            $t->{"$name"} = $data->{Database}->{sql_ram_tables}->{uc $name}
                          = SQL::Statement::RAM->new( uc $name, [], [] ); 
	}
        else {
            undef $@;
            eval{
                my $open_name = $self->{org_table_names}->[$count];
               if ($caller && $caller =~ /^DBD::AnyData/) {
                   $caller .= '::Statement' if $caller !~ /::Statement/;
                   $t->{"$name"} = $caller->open_table($data, $open_name,
                                                       $createMode, $lockMode);
    	       }
               else {
                   $t->{"$name"} = $self->open_table($data, $open_name,
                                                     $createMode, $lockMode);
	       }

	    };
            my $err = $t->{"$name"}->{errstr};
            return $self->do_err($err) if $err;
            return $self->do_err($@) if $@;
	}
my @cnames;
#$DEBUG=1;
#for my $c(@{$t->{"$name"}->{"col_names"}}) {
my $table_cols= $t->{"$name"}->{"org_col_names"};
   $table_cols= $t->{"$name"}->{"col_names"} unless $table_cols;
for my $c(@$table_cols) {
  my $newc;
  if ($c =~ /^"/) {
 #    $c =~ s/^"(.+)"$/$1/;
     $newc = $c;
  }
  else {
#     $newc = lc $c;
     $newc = uc $c;
  }
   push @cnames, $newc;
   $self->{ORG_NAME}->{$newc}=$c;
}
#
# set the col_num => col_obj hash for the table
#
my $col_nums;
my $i=0;
for (@cnames) {
  $col_nums->{$_} = $i++;
}
$t->{"$name"}->{"col_nums"}  = $col_nums; # upper cased
$t->{"$name"}->{"col_names"} = \@cnames;

        my $tcols = $t->{"$name"}->col_names;
        my @newcols;
        for (@$tcols) {
            next unless defined $_;
            my $ncol = $_;
            $ncol = $name.'.'.$ncol unless $ncol =~ /\./;
            push @newcols, $ncol;
	}
        @c = ( @c, @newcols );
    }
    ##################################################
    # Patch from Cosimo Streppone <cosimoATcpan.org>

    # my $all_cols = $self->{all_cols} 
    #             || [ map {$_->{name} }@{$self->{columns}} ]
    #             || [];
    # @$all_cols = (@$all_cols,@c);
    # $self->{all_cols} = $all_cols;
    my $all_cols = [];
    if(!$self->{all_cols}) {
        $all_cols   = [ map {$_->{name}} @{$self->{columns}} ];
        $all_cols ||= []; # ?
        @$all_cols  = (@$all_cols, @c);
        $self->{all_cols} = $all_cols;
    }
    ##################################################
    return SQL::Eval->new({'tables' => $t}), \@c;
}
sub verify_columns {
    my( $self, $data,$eval, $all_cols )  = @_;
    #
    # NOTE FOR LATER:
    # perhaps cache column names and skip this after first table open
    #
    $all_cols ||= [];
    my @tmp_cols  = @$all_cols;
    my $usr_cols;
    my $cnum=0;
    my @tmpcols = $self->columns;
    for my $c(@tmpcols) {
       if ($c->{"name"} eq '*' and defined $c->{"table"}) {
          return $self->do_err("Can't find table ". $c->{"table"})
              unless $eval->{"tables"}->{$c->{"table"}};
          my $tcols = $eval->{"tables"}->{$c->{"table"}}->col_names;
          return $self->do_err("Couldn't find column names!")
              unless $tcols and ref $tcols eq 'ARRAY' and @$tcols;
          for (@$tcols) {
              push @$usr_cols, SQL::Statement::Column->new( $_,
                                                            [$c->{"table"}]
                                                          );
	  }
       }
       else {
	  $c = SQL::Statement::Column->new( $c->{"name"},[$c->{"table"}])
             unless ref($c) and ref($c)=~/::Column/;
	  push @$usr_cols, $c;
       }
    }
    $self->{"columns"} = $usr_cols;
    @tmpcols = map {$_->{name}} @$usr_cols;
    my $fully_qualified_cols=[];

    my %col_exists   = map {$_=>1} @tmp_cols;

    my %short_exists = map {s/^([^.]*)\.(.*)/$1/; $2=>$1} @tmp_cols;
    my(%is_member,@duplicates,%is_duplicate);
    @duplicates = map {s/[^.]*\.(.*)/$1/; $_} @$all_cols;
    @duplicates = grep($is_member{$_}++, @duplicates);
    %is_duplicate = map { $_=>1} @duplicates;
    my $is_fully;
    my $i=-1;
    my $num_tables = $self->tables;
    for my $c(@tmpcols) {
       my($table,$col);
       if ($c =~ /(\S+)\.(\S+)/) {
           $table = $1;
           $col   = $2;
       }
       else {
       $i++;
       ($table,$col) = ( $usr_cols->[$i]->{"table"},
                         $usr_cols->[$i]->{"name"}
                       );
       }
       next unless $col;
###new
       if (ref $table eq 'SQL::Statement::Table') {
          $table = $table->name;
       }
###endnew
#print "Content-type: text/html\n\n"; print $self->command; print "$col!!!<p>";
       if ( $col eq '*' and $num_tables == 1) {
          # $table ||= $self->tables->[0]->{"name"};
          $table ||= $self->tables(0)->{"name"};
          if (ref $table eq 'SQL::Statement::Table') {
            $table = $table->name;
          }
          my @table_names = $self->tables;
          my $tcols = $eval->{"tables"}->{"$table"}->col_names;
# @$tcols = map{lc $_} @$tcols ;
          return $self->do_err("Couldn't find column names!")
              unless $tcols and ref $tcols eq 'ARRAY' and @$tcols;
          for (@$tcols) {
              push @{ $self->{"columns"} },
                    SQL::Statement::Column->new($_,\@table_names);
          }
          $fully_qualified_cols = $tcols;
          my @newcols;
	  for (@{$self->{"columns"}}) {
              push @newcols,$_ unless $_->{"name"} eq '*';
	  }
          $self->{"columns"} = \@newcols;
       }
       elsif ( $col eq '*' and defined $table) {
              $table = $table->name if ref $table eq 'SQL::Statement::Table';
              my $tcols = $eval->{"tables"}->{"$table"}->col_names;
# @$tcols = map{lc $_} @$tcols ;
          return $self->do_err("Couldn't find column names!")
              unless $tcols and ref $tcols eq 'ARRAY' and @$tcols;
              for (@$tcols) {
                  push @{ $self->{"columns"} },
                        SQL::Statement::Column->new($_,[$table]);
              }
              @{$fully_qualified_cols} = (@{$fully_qualified_cols}, @$tcols);
       }
       elsif ( $col eq '*' and $num_tables > 1) {
          my @table_names = $self->tables;
          for my $table(@table_names) {
              $table = $table->name if ref $table eq 'SQL::Statement::Table';
              my $tcols = $eval->{"tables"}->{"$table"}->col_names;
# @$tcols = map{lc $_} @$tcols ;
          return $self->do_err("Couldn't find column names!")
              unless $tcols and ref $tcols eq 'ARRAY' and @$tcols;
              for (@$tcols) {
                  push @{ $self->{"columns"} },
                        SQL::Statement::Column->new($_,[$table]);
              }
              @{$fully_qualified_cols} = (@{$fully_qualified_cols}, @$tcols);
              my @newcols;
  	      for (@{$self->{"columns"}}) {
                  push @newcols,$_ unless $_->{"name"} eq '*';
	      }
              $self->{"columns"} = \@newcols;
	  }
       }
       else {
           my $col_obj = $self->{computed_column}->{$c};
           if (!$table and !$col_obj) {
               return $self->do_err("Ambiguous column name '$c'")
                   if $is_duplicate{$c};
               return $self->do_err("No such column '$c'")
                      unless $short_exists{"$c"} or ($c !~ /^"/ and $short_exists{"\U$c"});
               $table = $short_exists{"$c"};
               $col   = $c;
           }
           elsif (!$col_obj) {
               my $is_user_def = 1 if $self->{opts}->{function_defs}->{$col};
               return $self->do_err("No such column '$table.$col'")
                     unless $col_exists{"$table.$col"}
                      or $col_exists{"\L$table.".$col}
                      or $is_user_def;

;
           }
           next if $table and $col and $is_fully->{"$table.$col"};
####
  $self->{"columns"}->[$i]->{"name"} = $col;
####
           $self->{"columns"}->[$i]->{"table"} = $table;
           push @$fully_qualified_cols, "$table.$col" if $table and $col;
           $is_fully->{"$table.$col"}++ if $table and $col;
       }
       if ( $col eq '*' and defined $table) {
              my @newcols;
  	      for (@{$self->{"columns"}}) {
                  push @newcols,$_ unless $_->{"name"} eq '*';
	      }
              $self->{"columns"} = \@newcols;
       }
    }
    # NON-AGGREGATE FUNCTIONS in SELECT LIST
    for my $i(0..$#{$self->{columns}}) {
        my $fcname = $self->{columns}->[$i]->name;
 	if (my $col_func = $self->{computed_column}->{$fcname}) {
            $self->{columns}->[$i]  = $col_func;
        }
    }
    #
    # CLEAN parser's {strcut} - no, maybe needed by second execute?
    #
    # delete $self->{opts};  # need $opts->{function_defs}
    # delete $self->{select_procedure};
    return $fully_qualified_cols;
}

sub distinct {
    my $self = shift;
    return 1 if $self->{"set_quantifier"}
       and $self->{"set_quantifier"} eq 'DISTINCT';
    return 0;
}

sub column_names {
    my($self)=@_;
    my @cols = map{$_->name}$self->columns
}
sub command { shift->{"command"} }

sub params {
    my $self = shift;
    my $val_num = shift;
    if (!$self->{"params"}) { return 0; }
    if (defined $val_num) {
        return $self->{"params"}->[$val_num];
    }
    if (wantarray) {
        return @{$self->{"params"}};
    }
    else {
        return scalar @{ $self->{"params"} };
    }

}
sub row_values {
    my $self = shift;
    my $val_num = shift;
    if (!$self->{"values"}) { return 0; }
    if (defined $val_num) {
        #        return $self->{"values"}->[$val_num]->{"value"};
        return $self->{"values"}->[$val_num];
    }
    if (wantarray) {
        return map{$_->{"value"} } @{$self->{"values"}};
    }
    else {
        return scalar @{ $self->{"values"} };
    }

}

sub get_row_value {
    my($self,$structure,$eval,$rowhash) = @_;
#    bug($structure);
    $structure = '' unless defined $structure;
    return $rowhash->{$structure} unless ref $structure;
    my $type = $structure->{"type"} if ref $structure eq 'HASH';
    $type ||='';

    #################################################################
    #
    # USER FUNCTIONS
    #
    # if the function hasn't been made into a S::S::Func object yet,
    # we create a new singlton S::S::Func object, otherwise
    # we use the existing S::S::Func object
    #
    use SQL::Statement::Util;
    if ( $type eq 'function' and $structure->{name} !~ /(TRIM|SUBSTRING)/i ){
        $self->{loaded_function}->{$structure->{name}}
            ||= SQL::Statement::Util::Function->new($structure);
        $structure = $self->{loaded_function}->{$structure->{name}};
    }
    #
    # Add the arguments from the S::S::Func object to an argslist
    # then call the function sending the cached sth, the current
    # rowhash, and the arguments list
    #
    if ( ref($structure) =~ /::Function/ ) {
        my @argslist=();
        for my $arg(@{$structure->args}) {
#            my $val = $arg unless ref $arg;
#            $val = $self->get_row_value($arg,$eval,$rowhash) unless defined $val;
            my $val = $self->get_row_value($arg,$eval,$rowhash);
            push @argslist, $val;
	}
        return $structure->run(
            $self->{procedure}->{data},
            $rowhash,
            @argslist
        );
#        my $sub = $structure->func;
#        return $structure->class->$sub(
#            $self->{procedure}->{data},
#            $rowhash,
#            @argslist
#        );
    }
    # end of USER FUNCTIONS
    #
    #################################################################

    return undef unless $type;
    $type = $structure->{name} if $type eq 'function'; # needed for TRIM+SUBST
    for ( $type ) {
        /string|number|null/      &&do { return $structure->{"value"} };
        /column/                  &&do {
                my $val = $structure->{"value"};
                my $tbl;
 		    if ($val =~ /^(.+)\.(.+)$/ ) {
                      ($tbl,$val) = ($1,$2);
		    }
                if ($self->{"join"}) {
                    # $tbl = 'shared' if $eval->is_shared($val);
                    $tbl ||= $self->colname2table($val);
                    $val = $tbl . "$dlm$val";
		}
                return $rowhash->{"$val"};
	};
        /placeholder/             &&do {
            my $val;
            if ($self->{"join"} or !$eval or ref($eval) =~ /Statement$/) {
                $val = $self->params($arg_num);
            }
            else {
                $val = $eval->param($arg_num);
            }
            $arg_num++;
            return $val;
        };
        /str_concat/              &&do {
                my $valstr ='';
        	for (@{ $structure->{"value"} }) {
                    my $newval = $self->get_row_value($_,$eval,$rowhash);
                    return undef unless defined $newval;
                    $valstr .= $newval;
	        }
                return $valstr;
        };
        /numeric_exp/             &&do {
           my @vals = @{ $structure->{"vals"} };
           my $str  = $structure->{"str"};
           for my $i(0..$#vals) {
               my $val = $self->get_row_value($vals[$i],$eval,$rowhash);
               return $self->do_err(
                   qq{Bad numeric expression '$vals[$i]->{"value"}'!}
               ) unless defined $val and is_number($val);
               $str =~ s/\?$i\?/$val/;
	   }
           $str =~ s/\s//g;
           $str =~ s/^([\)\(+\-\*\/0-9]+)$/$1/; # untaint
           return eval $str;
        };
#zzz
      my $vtype = $structure->{"value"}->{"type"};
#        my $vtype = $structure->{"type"};
#z
        my $value = $structure->{"value"}->{"value"} if ref $structure->{"value"} eq 'HASH';

### FOR USER-FUNCS
###         my $val_type = $structure->{value}->{type} 
###                     if ref $structure->{"value"} eq 'HASH';
###
        $value = $self->get_row_value($structure->{"value"},$eval,$rowhash)
               if $vtype eq 'function';

        /TRIM/                    &&do {
                my $trim_char = $structure->{"trim_char"} || ' ';
                my $trim_spec = $structure->{"trim_spec"} || 'BOTH';
                $trim_char = quotemeta($trim_char);
                if ($trim_spec =~ /LEADING|BOTH/ ) {
                    $value =~ s/^$trim_char+(.*)$/$1/;
		}
                if ($trim_spec =~ /TRAILING|BOTH/ ) {
                    $value =~ s/^(.*[^$trim_char])$trim_char+$/$1/;
		}
                return $value;
            };
        /SUBSTRING/                   &&do {
                my $start  = $structure->{"start"}->{"value"} || 1;
                my $offset = $structure->{"length"}->{"value"} || length $value;
                $value ||= '';
                return substr($value,$start-1,$offset)
                   if length $value >= $start-2+$offset;
        };
    }
}

#
# $num_of_cols = $stmt->columns()       # number of columns
# @cols        = $stmt->columns()       # array of S::S::Column objects
# $col         = $stmt->columns($cnum)  # S::S::Column obj for col number $cnum
# $col         = $stmt->columns($cname) # S::S::Column obj for col named $cname
#
sub columns {
    my $self = shift;
    my $col  = shift;
    if (!$self->{"columns"}) { return 0; }
    if (defined $col and $col =~ /^\d+$/) {      # arg1 = a number
        return $self->{"columns"}->[$col];
    }
    elsif (defined $col) {                       # arg1 = string
        for my $c(@{$self->{"columns"}}) {
            return $c if $c->name eq $col;
        }
    }
    if (wantarray) {                             # no arg1, array context
        return @{$self->{"columns"}};
    }
    else {                                       # no arg1,no array context
        return scalar @{ $self->{"columns"} };
    }

}
sub colname2colnum{
    my $self     = shift;
    my $colname  = shift;
    if (!$self->{"columns"}) { return undef; }
    for my $i(0..$#{$self->{"columns"}}) {
        return $i if $self->{columns}->[$i]->name eq $colname;
    }
}
sub colname2table {
    my $self = shift;
    my $col_name = shift;
    return undef unless defined $col_name;
    my $found_table;
    for my $full_col(@{$self->{all_cols}}) {
        my($table,$col) = $full_col =~ /^(.+)\.(.+)$/;
        next unless ($col||'') eq $col_name;
        $found_table = $table;
        last;
    }
    return $found_table;
}

sub verify_order_cols {
    my $self  = shift;
    my $table = shift;
    return unless $self->{"sort_spec_list"};
    my @ocols = $self->order;
    my @tcols = @{$table->col_names};
    my @n_ocols;
#die "@ocols";
#use mylibs; zwarn \@ocols; exit;
    for my $colnum(0..$#ocols) {
        my $col = $self->order($colnum);
#        if (!defined $col->table and defined $self->columns($colnum)) {
        if (!defined $col->table ) {
            my $cname = $ocols[$colnum]->{col}->name;
            my $tname = $self->colname2table($cname);
            return $self->do_err("No such column '$cname'.") unless $tname;
            $self->{"sort_spec_list"}->[$colnum]->{"col"}->{"table"}=$tname;
            push @n_ocols,$tname;
        }
    }
#    for (@n_ocols) {
#        die "$_" unless colname2table($_);
#    }
#use mylibs; zwarn $self->{"sort_spec_list"}; exit;
}

sub limit ($)  { shift->{limit_clause}->{limit}; }
sub offset ($) { shift->{limit_clause}->{'offset'}; }
sub order {
    my $self = shift;
    my $o_num = shift;
    if (!defined $self->{"sort_spec_list"}) { return (); }
    if (defined $o_num) {
        return $self->{"sort_spec_list"}->[$o_num];
    }
    if (wantarray) {
        return @{$self->{"sort_spec_list"}};
    }
    else {
        return scalar @{ $self->{"sort_spec_list"} };
    }

}
sub tables {
    my $self = shift;
    my $table_num = shift;
    if (defined $table_num) {
        return $self->{"tables"}->[$table_num];
    }
    if (wantarray) {
        return @{ $self->{"tables"} };
    }
    else {
#        return scalar @{ $self->{"table_names"} };
        return scalar @{ $self->{"tables"} };
    }

}
sub order_joins {
    my $self = shift;
    my $links = shift;
    my @new_keycols;
    for (@$links) {
       push @new_keycols, $self->colname2table($_) . ".$_";
    }
    my @tmp = @new_keycols;
    @tmp = map { s/\./$dlm/g; $_ } @tmp;
    $self->{"join"}->{"keycols"}  = \@tmp;
    @$links = map { s/^(.+)\..*$/$1/; $_; } @new_keycols;
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
    $self->{"join"}->{"table_order"} = \@order;
    return \@order;
}

sub do_err {
    my $self = shift;
    my $err  = shift;
    my $errtype  = shift;
    my @c = caller 6;
    #$err = "[" . $self->{"original_string"} . "]\n$err\n\n";
    #    $err = "$err\n\n";
    my $prog = $c[1];
    my $line = $c[2];
    $prog = defined($prog) ? " called from $prog" : '';
    $prog .= defined($line) ? " at $line" : '';
    $err =  "\nExecution ERROR: $err$prog.\n\n";

    $self->{"errstr"} = $err;
    warn $err if $self->{"PrintError"};
    die "$err" if $self->{"RaiseError"};
    return undef;
}

sub errstr {
    my $self = shift;
    $self->{"errstr"};
}

sub where {
    my $self = shift;
    return undef unless $self->{"where_clause"};
   $warg_num = 0;
    return SQL::Statement::Op->new(
        $self->{"where_clause"},
        $self->{"tables"},
    );
}

sub where_hash {
    my $self = shift;
    return $self->{where_clause};
}

sub get_user_func_table {
    my($self,$name,$u_func) = @_;
    my($data_aryref) =$self->get_row_value($u_func,$self,{});
    my $col_names = shift @$data_aryref;
    # my $tempTable = SQL::Statement::TempTable->new(
    #     $name, $col_names, $col_names, $data_aryref
    # );
    my $tempTable = SQL::Statement::RAM->new(
        $name, $col_names, $data_aryref
    );
    $tempTable->{all_cols} ||= $col_names;
    return $tempTable;
}


package SQL::Statement::Group;

sub new {
    my $class = shift;
    my ($keycol,$display_cols,$ary)=@_;
    my $self = {
       keycol       => $keycol,
       display_cols => $display_cols,
       records      => $ary,
    };
    return bless $self, $class;
}
sub calc {
    my $self=shift;
    $self->ary2hash( $self->{records} );
    my @cols = @{ $self->{display_cols} };
    for my $key(@{$self->{keys}}) {
        my $newrow;
        my $colnum=0;
        my %done;
        my @func;
        for my $col(@cols) {
	    if ($col->{arg}) {
                my $selkey = $col->{sel_col_num};
$selkey ||= 0;
		if (!defined $selkey) {
#use mylibs; zwarn $col;
#exit;
		} 
                $func[$selkey] = $self->calc_cols($key,$selkey)
                   unless defined $func[$selkey];
                push @$newrow, $func[$selkey]->{$col->{name}};
	    }
            else {
                push @$newrow, $self->{records}->{$key}->[-1]->[$col->{sel_col_num}];
#use mylibs; zwarn $newrow;
#exit;
	    }
            $colnum++;
	}
        push @{$self->{final}}, $newrow;
    }
    return $self->{final};
}
sub calc_cols {
    my($self,$key,$selcolnum)=@_;
    # $self->{counter}++;
    my( $sum,$count,$min,$max,$avg );
    my $ary = $self->{records}->{$key};
    for my $row(@$ary) {
        my $val = $row->[$selcolnum];
        $max = $val if !(defined $max) or SQL::Statement::anycmp($val,$max) > 0;
        $min = $val if !(defined $min) or SQL::Statement::anycmp($val,$min) < 0;
        $count++;
        $sum += $val if $val =~ $SQL::Statement::numexp;
    }
    $avg = $sum/$count if $count and $sum;
    return {
        AVG   => $avg,
        MIN   => $min,
        MAX   => $max,
        SUM   => $sum,
        COUNT => $count,
    };
}

sub ary2hash {
    my $self = shift;
    my $ary  = shift;
    my $keycolnum = $self->{keycol} || 0;
    my $hash;
    my @keys;
    my %is_key;
    for my $row(@$ary) {
        my $key = $row->[$keycolnum];
#die "@$row" unless defined $key;
        push @{$hash->{$key}}, $row;
        push @keys, $key unless $is_key{$key}++;
    }
    $self->{records}=$hash;
    $self->{keys}   =\@keys;
}

package SQL::Statement::Op;

sub new {
    my($class,$wclause,$tables) = @_;
    if (ref $tables->[0]) {
        @$tables = map {$_->name} @$tables;
    }
    my $self = {};
    $self->{"op"}   = $wclause->{"op"};
    $self->{"neg"}  = $wclause->{"neg"};
    $self->{"arg1"} = get_pred( $wclause->{"arg1"}, $tables );
    $self->{"arg2"} = get_pred( $wclause->{"arg2"}, $tables );
    return bless $self, $class;
}
sub get_pred {
    my $arg    = shift;
    my $tables = shift;
    if (defined $arg->{type}) {
        if ( $arg->{type} eq 'column') {
            return SQL::Statement::Column->new( $arg->{value}, $tables );
        }
        elsif ( $arg->{type} eq 'placeholder') {
            return SQL::Statement::Param->new( $main::warg_num++ );
        }
        else {
            return $arg->{value};
        }
    }
    else {
        return SQL::Statement::Op->new( $arg, $tables );
    }
}
sub op {
    my $self = shift;
    return $self->{"op"};
}
sub arg1 {
    my $self = shift;
    return $self->{"arg1"};
}
sub arg2 {
    my $self = shift;
    return $self->{"arg2"};
}
sub neg {
    my $self = shift;
    return $self->{"neg"};
}


package SQL::Statement::TempTable;
use base 'SQL::Eval::Table';

sub new {
    my $class      = shift;
    my $name       = shift;
    my $col_names  = shift;
    my $table_cols = shift;
    my $table      = shift;
    my $col_nums;
    for my $i(0..scalar @$col_names -1) {
      $col_names->[$i]= uc $col_names->[$i];
      $col_nums->{"$col_names->[$i]"}=$i;
    }
    my @display_order = map { $col_nums->{$_} } @$table_cols;
    my $self = {
        col_names  => $col_names,
        table_cols => \@display_order,
        col_nums   => $col_nums,
        table      => $table,
        NAME       => $name,
    };
    return bless $self, $class;
}
sub is_shared {my($s,$colname)=@_;return $s->{"is_shared"}->{"$colname"}}
sub col_nums { shift->{"col_nums"} }
sub col_names { shift->{"col_names"} }
sub column_num  { 
    my($s,$col) = @_;
    my $new_col = $s->{"col_nums"}->{"$col"};
    if (! defined $new_col) {
        my @tmp = split '~',$col;
        $new_col = lc($tmp[0]) . '~' . uc($tmp[1]);
        $new_col = $s->{"col_nums"}->{"$new_col"};
    }
    return $new_col
}
sub fetch_row { my $s=shift; return shift @{ $s->{"table"} } }

package SQL::Statement::Order;

sub new ($$) {
    my $proto = shift;
    my $self = {@_};
    bless($self, (ref($proto) || $proto));
}
sub table ($) { shift->{'col'}->table(); }
sub column ($) { shift->{'col'}->name(); }
sub desc ($) { shift->{'desc'}; }


package SQL::Statement::Limit;

sub new ($$) {
    my $proto = shift;
    my $self  = shift;
    bless($self, (ref($proto) || $proto));
}
#sub limit ($) { shift->{'limit'}; }
#sub offset ($) { shift->{'offset'}; }

package SQL::Statement::Param;

sub new {
    my $class = shift;
    my $num   = shift;
    my $self = { 'num' => $num };
    return bless $self, $class;
}

sub num ($) { shift->{'num'}; }


package SQL::Statement::Column;

sub new {
    my $class = shift;
    my $col_name = shift;
    my $tables = shift;
    my $table_name = $col_name;
    #my @c = caller 0; print $c[2];
    if (ref $col_name eq 'HASH') {
        $tables   = [ $col_name->{"table"} ];
        $col_name = $col_name->{"column"}  ;
    }
    # print " $col_name !\n";
    my $num_tables = scalar @{ $tables };
    if ($table_name && (
           $table_name =~ /^(".+")\.(.*)$/
        or $table_name =~ /^([^.]*)\.(.*)$/
        )) {
            $table_name = $1;
            $col_name = $2;
    }
    elsif ($num_tables == 1) {
        $table_name = $tables->[0];
    }
    else {
        undef $table_name;
    }
    my $self = {
        name => $col_name,
        table => $table_name,
    };
    return bless $self, $class;
}

#sub value { shift->{"value"} }
#sub type  { shift->{"type"} }
sub name  { shift->{"name"} }
sub table { shift->{"table"} }

package SQL::Statement::Table;

sub new {
    my $class = shift;
    my $table_name = shift;
    my $self = {
        name => $table_name,
    };
    return bless $self, $class;
}

sub name  { shift->{"name"} }
1;
__END__

=pod

=head1 NAME

SQL::Statement - SQL parsing and processing engine

=head1 SYNOPSIS

  # ... depends on what you want to do, see below

=head1 DESCRIPTION

The SQL::Statement module implements a pure Perl SQL parsing and execution engine.  While it by no means implements full ANSI standard, it does support many features including column and table aliases, built-in and user-defined functions, implicit and explicit joins, complexly nested search conditions, and other features.

SQL::Statement is a small embeddable Database Management System (DBMS),  This means that it provides all of the services of a simple DBMS except that instead of a persistant storage mechanism, it has two things: 1) an in-memory storage mechanism that allows you to prepare, execute, and fetch from SQL statements using temporary tables and 2) a set of software sockets where any author can plug in any storage mechanism.

There are three main uses for SQL::Statement. One or another (hopefully not all) may be irrelevant for your needs: 1) to access and manipulate data in CSV, XML, and other formats 2) to build your own DBD for a new data source 3) to parse and examine the structure of SQL statements.

=head1 INSTALLATION

There are no prerequisites for using this as a standalone parser.  If you want to access persistant stored data, you either need to write a subclass or use one of the DBI DBD drivers.  You can install this module using CPAN.pm, CPANPLUS.pm, PPM, apt-get, or other packaging tools.  Or you can download the tar.gz file form CPAN and use the standard perl mantra

 perl Makefile.PL
 make
 make test
 make install

It works fine on all platforms it's been tested on.  On Windows, you can use ppm or with the mantra use nmake, dmake, or make depending on which is available.

=head1 USAGE

=head2 How can I use SQL::Statement to access and modify data?

SQL::Statement provides the SQL engine for a number of existing DBI drivers including L<DBD::CSV>, L<DBD::DBM>, L<DBD::AnyData>, L<DBD::Excel>, L<DBD::Amazon>, and others.

These modules provide access to Comma Separated Values, Fixed Length, XML, HTML and many other kinds of text files, to Excel Spreadsheets, to BerkeleyDB and other DBM formats, and to non-traditional data sources like on-the-fly Amazon searches.

If your interest is in actually accessing and manipulating persistent data, you don't really want to use SQL::Statement directly.  Instead, use L<DBI> along with one of the DBDs mentioned above.  You'll be using SQL::Statement, but under the hood of the DBD.   See L<http://dbi.perl.org> for help with DBI and see L<SQL::Statement::Syntax> for a description of the SQL syntax that SQL::Statement provides for these modules and see the documentation for whichever DBD you are using for additional details.

=head2 How can I use it to parse and examine the structure of SQL statements?

SQL::Statement can be used stand-alone (without a subclass, without DBI) to parse and examine the structure of SQL statements.  See L<SQL::Statement::Structure> for details.

=head2 How can I use it to embed a SQL engine in a DBD or other module?

SQL::Statement is designed to be easily embedded in other modules and is especially suited for developing new DBI drivers (DBDs).  See L<SQL::Statement::Embed>.

=head2 What SQL Syntax is supported?

SQL::Statement supports a small but powerful subset of SQL commands. See L<SQL::Statement::Syntax>.

=head2 How can I extend the supported SQL syntax?

You can modify and extend the SQL syntax either by issuing SQL commands or by subclassing SQL::Statement.  See L<SQL::Statement::Syntax>.

=head1 How can I participate in ongoing development?

SQL::Statement is a large module with many potential future directions.  You are invited to help plan, code, test, document, or kibbitz about these directions.  A sourceforge site will be available soon.  If you want to join the development team, or just hear more about the development, write Jeff a note (<jzuckerATcpan.org>.

=head1 Where can I go for more help?

For questions about installation or usage, please ask on the dbi-users@perl.org mailing list or post a question on PerlMonks (L<http://www.perlmonks.org/>, where Jeff is known as jZed).  If you have a bug report, a patch, a suggestion, write Jeff at the email shown below.

=head1 ACKNOWLEDGEMENTS

Jochen Wiedmann created the original module as an XS (C) extension in 1998. Jeff Zucker took over the maintenance in 2001 and rewrote all of the C portions in perl and began extending the SQL support.  More recently Ilya Sterin provided help with SQL::Parser, Tim Bunce provided both general and specific support, Dan Wright and Dean Arnold have contributed extensively to the code, and dozens of people from around the world have submitted patches, bug reports, and suggestions.  Thanks to all!

If you're interested in helping develop SQL::Statement or want to use it with your own modules, feel free to contact Jeff.

=head1 AUTHOR AND COPYRIGHT

Copyright (c) 2001,2005 by Jeff Zucker: jzuckerATcpan.org

Portions Copyright (C) 1998 by Jochen Wiedmann: jwiedATcpan.org

All rights reserved.

You may distribute this module under the terms of either the GNU
General Public License or the Artistic License, as specified in
the Perl README file.

=cut
