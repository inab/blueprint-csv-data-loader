#!/usr/bin/perl -w

use strict;

use Carp;
use Config::IniFiles;
use Search::Elasticsearch 1.12;

use constant INI_SECTION	=>	'elasticsearch';

use constant DEFAULT_WP10_INDEX	=>	'meqtls';
use constant DEFAULT_WP10_TYPE	=>	'meqtls';

my @DEFAULTS = (
	['use_https' => 'false' ],
	['nodes' => [ 'localhost' ] ],
	['port' => '' ],
	['path_prefix' => '' ],
	['user' => '' ],
	['pass' => '' ],
	['request_timeout' => 300],
);

my %skipCol = (
	'chromosome_name'	=>	undef,
);

if(scalar(@ARGV)>=2) {
	# First, let's read the configuration
	my $iniFile = shift(@ARGV);
	
	my $ini = Config::IniFiles->new(-file => $iniFile, -default => 'main');
	
	my %confValues = ();
	if($ini->SectionExists(INI_SECTION)) {
		foreach my $param (@DEFAULTS) {
			my($key,$defval) = @{$param};
			
			if(defined($defval)) {
				my @values = $ini->val(INI_SECTION,$key,$defval);
				$confValues{$key} = (scalar(@values)>1)?\@values:$values[0];
			} elsif($ini->exists(INI_SECTION,$key)) {
				my @values = $ini->val(INI_SECTION,$key);
				$confValues{$key} = (scalar(@values)>1)?\@values:((scalar(@values)>0)?$values[0]:undef);
			} else {
				Carp::croak("ERROR: required parameter $key not found in section ".INI_SECTION);
			}
		}
		
		# Normalizing use_https
		if(exists($confValues{use_https}) && defined($confValues{use_https}) && $confValues{use_https} eq 'true') {
			$confValues{use_https} = 1;
		} else {
			delete($confValues{use_https});
		}
		
		# Normalizing userinfo
		if(exists($confValues{user}) && defined($confValues{user}) && length($confValues{user}) > 0 && exists($confValues{pass}) && defined($confValues{pass})) {
			$confValues{userinfo} = $confValues{user} . ':' . $confValues{pass};
		}
		
		# Normalizing nodes
		if(exists($confValues{nodes})) {
			unless(ref($confValues{nodes}) eq 'ARRAY') {
				$confValues{nodes} = [split(/ *, */,$confValues{nodes})];
			}
		}
	} else {
		Carp::croak("ERROR: Unable to read section ".INI_SECTION);
	}
	
	my @connParams = ();
	
	foreach my $key ('use_https','port','path_prefix','userinfo','request_timeout') {
		if(exists($confValues{$key}) && defined($confValues{$key}) && length($confValues{$key}) > 0) {
			push(@connParams,$key => $confValues{$key});
		}
	}
	
	# The connection
	my $es = Search::Elasticsearch->new(@connParams,'nodes' => $confValues{nodes});
	
	my $indexName = DEFAULT_WP10_INDEX;
	my $mappingName = DEFAULT_WP10_TYPE;
	$es->indices->delete('index' => $indexName)  if($es->indices->exists('index' => $indexName));
	$es->indices->create('index' => $indexName)  unless($es->indices->exists('index' => $indexName));
	
	my @bes_params = (
		index   => $indexName,
		type    => $mappingName,
	);
	push(@bes_params,'max_count' => $ini->val('mapper','batch-size'))  if($ini->exists('mapper','batch-size'));
	
	# The bulk helper (for massive insertions)
	my $bes = $es->bulk_helper(@bes_params);
	
	foreach my $file (@ARGV) {
		if(open(my $CSV,'<:encoding(UTF-8)',$file)) {
			print "* Processing $file\n";
			my $colsLine = <$CSV>;
			chomp($colsLine);
			my @cols = split(/\t/,$colsLine,-1);
			my @colSkip = map { exists($skipCol{$_}) } @cols;
			
			while(my $line=<$CSV>) {
				chomp($line);
				my @vals = split(/\t/,$line,-1);
				
				my $iVal = -1;
				foreach my $val (@vals) {
					$iVal++;
					if(defined($val)) {
						unless($colSkip[$iVal]) {
							if($val =~ /^-?[0-9]+(\.[0-9]+)?$/) {
								if(defined($1)) {
									$val = $val + 0E0;
								} else {
									$val = $val + 0;
								}
							} elsif($val eq 'NA') {
								$val = undef;
							}
						}
					}
				}
				
				my %entry = ();
				@entry{@cols} = @vals;
				
				$bes->index({ 'source' => \%entry });
				#use Data::Dumper;
				#print Dumper(\%entry),"\n";
			}
			
			$bes->flush();
			
			close($CSV);
		} else {
			Carp::croak("[ERROR] Unable to open $file. Reason: ".$!);
		}
	}
} else {
	print STDERR "Usage: $0 {ini file} {tab file}+\n";
}
