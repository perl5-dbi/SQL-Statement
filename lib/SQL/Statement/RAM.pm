############################
package SQL::Statement::RAM;
############################
sub new {
    my($self,$tname,$col_names,$data_tbl) = @_;
    my $col_nums={}; my $i=0;
    for (@$col_names) { next unless $_; $col_nums->{$_} = $i; $i++; }
    my %table = (
        NAME      => $tname,
        index     => 0,
	records   => $data_tbl,
	col_nums  => $col_nums,
	col_names => $col_names,
    );
    return bless \%table, 'SQL::Statement::RAM::Table';
}
####################################
package SQL::Statement::RAM::Table;
####################################
sub column_num  { $_[0]->{"col_nums"}->{$_[1]} }
sub col_names   { shift->{col_names} }
sub get_pos { my $s=shift; $s->{CUR}= $s->{index}}

##################################
# fetch_row()
##################################
sub fetch_row {
    my($self,$data) = @_;
    my $currentRow = $self->{index};
    return undef unless $self->{records} ;
    return undef if $currentRow >= @{ $self->{records} };
    $self->{index} = $currentRow+1;
    $self->get_pos($self->{index});
    return $self->{records}->[ $currentRow ];
}
####################################
# push_row()
####################################
sub push_row {
    my($self,$data,$fields) = @_;
    my $currentRow = $self->{index};
    $self->{index} = $currentRow+1;
    $self->{records}->[$currentRow] = $fields;
    return 1;
}
##################################
# truncate()
##################################
sub truncate {
    my $self = shift;
    return splice @{$self->{records}}, $self->{index},1;
}
#####################################
# push_names()
#####################################
sub push_names {
    my($self, $data, $names) = @_;
    $self->{col_names} = $names;
    push @{$self->{org_col_names}},$_ for @$names;
    push @{$self->{parser}->{col_names}},$_ for @$names;
    my($col_nums) = {};
    for (my $i = 0;  $i < @$names;  $i++) {
        $col_nums->{$names->[$i]} = $i;
    }
    $self->{col_nums} = $col_nums;
}
#####################################
# drop()
#####################################
sub drop {
    my($self,$data) = @_;
    my $tname = $self->{NAME};
    delete $data->{Database}->{sql_ram_tables}->{uc $tname};
    return 1;
}
#####################################
# seek()
#####################################
sub seek {
    my($self, $data,$pos, $whence) = @_;
    return unless defined $self->{records};
    my($currentRow) = $self->{index};
    if ($whence == 0) {
        $currentRow = $pos;
    } elsif ($whence == 1) {
        $currentRow += $pos;
    } elsif ($whence == 2) {
        $currentRow = @{$self->{records}} + $pos;
    } else {
        die $self . "->seek: Illegal whence argument ($whence)";
    }
    if ($currentRow < 0) {
        die "Illegal row number: $currentRow";
    }
    $self->{index} = $currentRow;
}
############################################################################
1;
