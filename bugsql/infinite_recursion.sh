perl -Iblib/lib -MSQL::Parser -MData::Dumper -MSQL::Statement -e 'my $stmt = SQL::Statement->new("SELECT * FROoM stuff WHERE asdf => 234 AND somsing > 45"); print Dumper($stmt->where);'
perl -Iblib/lib -MSQL::Parser -MData::Dumper -MSQL::Statement -e 'my $p = SQL::Parser->new; my $res = $p->parse("SELECT * FROoooM stuff WHERE asdf => 234 AND somshit > 45"); print "OK" if $res;'
# RT #29274
