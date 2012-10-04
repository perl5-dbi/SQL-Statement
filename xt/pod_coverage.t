use 5.006;

use Test::More;
eval "use Test::Pod::Coverage 1.00";
plan skip_all => "Test::Pod::Coverage 1.00 required for testing POD Coverage" if $@;

my @modules = all_modules();
plan tests => scalar @modules;
foreach my $module (@modules) {
   next if ($module eq 'SQL::Statement::Functions');
   pod_coverage_ok($module, {also_private => [ qr/^[A-Z0-9_]+$/ ], trustme => [qr/^new$/]} );  # Ignore all caps/digits
}

# Warp the namespace a bit, so that Pod::Coverage can recognize the subs
use SQL::Statement::Functions;
my @keys = keys %SQL::Statement::Functions::;
foreach my $subname (@keys) {
   my $short_name = $subname;
   $short_name =~ s/^SQL_FUNCTION_// || next;
   $SQL::Statement::Functions::{$short_name} = $SQL::Statement::Functions::{$subname};
   delete $SQL::Statement::Functions::{$subname};
}

### FIXME: This seems to always return true... ###
pod_coverage_ok( 'SQL::Statement::Functions', {private => [], also_private => [], trustme => []} );
