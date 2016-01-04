#!/usr/bin/perl -w

use strict;

use boolean 0.32;
use Carp;
use Config::IniFiles;
use File::Basename qw();
use Search::Elasticsearch 1.12;

use constant INI_SECTION	=>	'elasticsearch';

use constant DEFAULT_WP10_INDEX	=>	'wp10qtls';

use constant DEFAULT_WP10_TYPE	=>	'qtl';
use constant BULK_WP10_TYPE	=>	'bulkqtl';

use constant DATA_FILETYPE	=>	'data';
use constant SQLT_FILETYPE	=>	'sqtl';
use constant BULK_FILETYPE	=>	'bulk';

my %QTL_TYPES = (
	+DEFAULT_WP10_TYPE => {
		'_all'	=> {
			'enabled'	=>	boolean::true
		},
		'properties' => {
			# This is 'mono', 'neut' or 'tcel'
			'cell_type' => {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# This is 'cufflinks', 'K27AC', 'K4ME1', 'exon', 'gene', 'meth', 'psi', 'sj', 'sqtls'
			'qtl_source' => {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Common columns
			# 'geneID'
			'gene_id'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# 'rs', 'snpId'
			'snp_id'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'gene_chrom'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'gene_start'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'long',
			},
			'gene_end'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'long',
			},
			'pos'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'long',
			},
			'pv'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'double',
			},
			# 'qv_all', 'qv'
			'qv'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'double',
			},
			# Column from 'cufflinks', 'sqtls'
			# 'tr.first', 'tr.second'
			'ensemblTranscriptId'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Column from 'K27AC', 'K4ME1'
			'histone'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Column from 'gene', 'exon', 'sqtls'
			'ensemblGeneId'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Column from 'exon'
			'exonNumber'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'long',
			},
			# Column from 'meth'
			'probeId'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Column from 'psi', 'sj'
			'splice'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Column from 'sqtls'
			'F'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'double',
			},
			# The different metrics
			# 'beta', 'pv_bonf', 'pv_storey', 'nb.groups', 'md', 'F.svQTL', 'nb.perms', 'nb.perms.svQTL', 'pv.svQTL', 'qv.svQTL'
			'metrics'	=> {
				'dynamic'	=>	boolean::true,
				'type'	=>	'nested',
				'include_in_parent'	=>	boolean::true,
				'dynamic_templates'	=>	[
					{
						'template_metrics'	=> 	{
							'match'	=>	'*',
							'mapping'	=>	{
								'dynamic'	=>	boolean::false,
								'type'	=>	'double',
							},
						},
					},
				],
			},
		}
	},
	+BULK_WP10_TYPE => {
		'_all'	=> {
			'enabled'	=>	boolean::true
		},
		'properties' => {
			# This is 'mono', 'neut' or 'tcel'
			'cell_type' => {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# This is 'cufflinks', 'K27AC', 'K4ME1', 'exon', 'gene', 'meth', 'psi', 'sj', 'sqtls'
			'qtl_source' => {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# 'geneID'
			'gene_id'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# The bulk data from all the entries with the same geneId
			'qtl_data'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index'	=> 'no',
				'include_in_all' =>	boolean::false,
			},
		}
	},
);

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

my $doClean = undef;
if(scalar(@ARGV) > 0 && $ARGV[0] eq '-C') {
	$doClean = 1;
	shift(@ARGV);
}

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

	$es->indices->delete('index' => $indexName)  if($doClean && $es->indices->exists('index' => $indexName));
	unless($es->indices->exists('index' => $indexName)) {
		$es->indices->create('index' => $indexName);
		foreach my $mappingName (keys(%QTL_TYPES)) {
			$es->indices->put_mapping(
				'index' => $indexName,
				'type' => $mappingName,
				'body' => {
					$mappingName => $QTL_TYPES{$mappingName}
				}
			);
		}
	}
	
	foreach my $file (@ARGV) {
		my $basename = File::Basename::basename($file);
		
		# Three different types of files
		my $mappingName;
		my $colsLine;
		my $colSep;
		my $fileType;
		my $cell_type;
		my $qtl_source;
		my $geneIdKey;
		my $snpIdKey;
		
		if($basename =~ /^([^_]+)[_.]([^_])_(.+)_summary\.hdf5\.txt$/) {
			$mappingName = DEFAULT_WP10_TYPE;
			$cell_type = $1;
			$qtl_source = $2;
			$colSep = qr/\t/;
			$fileType = DATA_FILETYPE;
			$geneIdKey = 'geneID';
			$snpIdKey = 'rs';
		} elsif($basename =~ /^([^_]+)[_.]([^_])_(.+)_all_summary\.txt$/) {
			$mappingName = BULK_WP10_TYPE;
			$cell_type = $1;
			$qtl_source = $2;
			$colSep = ' ';
			$colsLine = 'geneID rs pos beta pv qv_all';
			$fileType = BULK_FILETYPE;
			$geneIdKey = 'geneID';
			$snpIdKey = 'rs';
		} elsif($basename =~ /^([^_]+)\.(sqtls)\.sig5\.tsv$/) {
			$mappingName = DEFAULT_WP10_TYPE;
			$cell_type = $1;
			$qtl_source = $2;
			$colSep = qr/\t/;
			$fileType = SQLT_FILETYPE;
			$geneIdKey = 'geneId';
			$snpIdKey = 'snpId';
		} else {
			print "[INFO] Discarding file $file (unknown type)\n";
			next;
		}
		
		if(open(my $CSV,'<:encoding(UTF-8)',$file)) {
			print "* Processing $file\n";
			
			my @bes_params = (
				index   => $indexName,
				type    => $mappingName,
			);
			push(@bes_params,'max_count' => $ini->val('mapper','batch-size'))  if($ini->exists('mapper','batch-size'));
			
			# The bulk helper (for massive insertions)
			my $bes = $es->bulk_helper(@bes_params);

			unless(defined($colsLine)) {
				my $colsLine = <$CSV>;
				chomp($colsLine);
			}
			my @cols = split($colSep,$colsLine,-1);
			#my @colSkip = map { exists($skipCol{$_}) } @cols;
			
			my $bulkGeneId='';
			my $bulkData;
			
			while(my $line=<$CSV>) {
				chomp($line);
				my @vals = split($colSep,$line,-1);
				
				#my $iVal = -1;
				#foreach my $val (@vals) {
				#	$iVal++;
				#	if(defined($val)) {
				#		unless($colSkip[$iVal]) {
				#			if($val =~ /^-?[0-9]+(\.[0-9]+)?$/) {
				#				if(defined($1)) {
				#					$val = $val + 0E0;
				#				} else {
				#					$val = $val + 0;
				#				}
				#			} elsif($val eq 'NA') {
				#				$val = undef;
				#			}
				#		}
				#	}
				#}
				
				if($fileType eq BULK_FILETYPE) {
					if($vals[0] ne $bulkGeneId) {
						if(defined($bulkData)) {
							my %entry = (
								'cell_type' => $cell_type,
								'qtl_source' => $qtl_source,
								'gene_id' => $bulkGeneId,
								'qtl_data' => $bulkData,
							);
							
							$bes->index({ 'source' => \%entry });
						}
						$bulkGeneId = $vals[0];
						$bulkData = '';
					}
					$bulkData .= join('\t',@vals[1..$#vals]) . "\n";
				} else {
					my %data = ();
					@data{@cols} = @vals;
					
					my %entry = (
						'cell_type' => $cell_type,
						'qtl_source' => $qtl_source,
						'gene_id' => $data{$geneIdKey},
						'snp_id' => $data{$snpIdKey},
					);
					
					if($fileType eq DATA_FILETYPE) {
						$entry{'gene_chrom'} = $data{'gene_chrom'};
						$entry{'gene_start'} = $data{'gene_start'}+0;
						$entry{'gene_end'} = $data{'gene_end'}+0;
						$entry{'pos'} = $data{'pos'}+0;
						$entry{'pv'} = $data{'pv'}+0E0;
						$entry{'qv'} = $data{'qv_all'}+0E0;
						$entry{'metrics'} = {
							'beta' => $data{'beta'}+0E0,
							'pv_bonf' => $data{'pv_bonf'}+0E0,
							'pv_storey' => $data{'pv_storey'}+0E0,
						};
						
						if($qtl_source eq 'cufflinks') {
							$entry{'ensemblTranscriptId'} = $data{$geneIdKey};
						} elsif($qtl_source eq 'gene') {
							$entry{'ensemblGeneId'} = $data{$geneIdKey};
						} elsif($qtl_source eq 'exon') {
							my $lastdot = rindex('.', $data{$geneIdKey});
							$entry{'ensemblGeneId'} = substr($data{$geneIdKey},0,$lastdot);
							$entry{'exonNumber'} = substr($data{$geneIdKey},$lastdot+1) + 0;
						} elsif($qtl_source eq 'meth') {
							$entry{'probeId'} = $data{$geneIdKey};
						} elsif($qtl_source eq 'psi') {
							$entry{'splice'} = [split('_', $data{$geneIdKey})];
						} elsif($qtl_source eq 'sj') {
							$entry{'splice'} = $data{$geneIdKey};
						} else {
							# Histones
							$entry{'histone'} = 'H3'.$data{$geneIdKey};
						}
						
					} elsif($fileType eq SQLT_FILETYPE) {
						$entry{'pv'} = $data{'pv'}+0E0;
						$entry{'qv'} = $data{'qv'}+0E0;
						$entry{'F'} = $data{'F'}+0E0;
						$entry{'ensemblGeneId'} = $data{$geneIdKey};
						$entry{'ensemblTranscriptId'} = [ $data{'tr.first'}, $data{'tr.second'} ];
						my %metrics = ();
						$entry{'metrics'} = \%metrics;
						foreach my $key ('nb.groups', 'md', 'F.svQTL', 'nb.perms', 'nb.perms.svQTL', 'pv.svQTL', 'qv.svQTL') {
							my $tKey = $key;
							$tKey =~ tr/./_/;
							$metrics{$tKey} = $data{$key} + 0E0;
						}
					}
					
					$bes->index({ 'source' => \%entry });
				}
				#use Data::Dumper;
				#print Dumper(\%entry),"\n";
			}
			# Bulk data special case
			if(defined($bulkData)) {
				my %entry = (
					'cell_type' => $cell_type,
					'qtl_source' => $qtl_source,
					'gene_id' => $bulkGeneId,
					'qtl_data' => $bulkData,
				);
				
				$bes->index({ 'source' => \%entry });
			}
			
			$bes->flush();
			
			close($CSV);
		} else {
			Carp::croak("[ERROR] Unable to open $file. Reason: ".$!);
		}
	}
} else {
	print STDERR "Usage: $0 [-C] {ini file} {tab file}+\n";
}
