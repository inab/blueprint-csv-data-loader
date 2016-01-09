#!/usr/bin/perl -w

use v5.12;
use warnings qw(all);
use strict;

use FindBin;
use lib $FindBin::Bin."/BLUEPRINT-dcc-loading-scripts/model/schema+tools/lib";
use lib $FindBin::Bin."/BLUEPRINT-dcc-loading-scripts/libs";

use boolean 0.32;
use Carp;
use Config::IniFiles;
use File::Basename qw();
use File::Spec;
use Log::Log4perl;
use Search::Elasticsearch 1.12;

use BP::Model;
use BP::Loader::Mapper;

use BP::DCCLoader::Parsers;
use BP::DCCLoader::WorkingDir;
use BP::DCCLoader::Parsers::GencodeGTFParser;

my $LOG;
BEGIN {
	Log::Log4perl->easy_init( { level => $Log::Log4perl::INFO, layout => "[%d{ISO8601}]%p %m%n" } );
	$LOG = Log::Log4perl->get_logger(__PACKAGE__);
};

use constant INI_SECTION	=>	'elasticsearch';

use constant DEFAULT_WP10_INDEX	=>	'wp10qtls';
use constant BULK_WP10_INDEX	=>	'wp10bulkqtls';

use constant DEFAULT_WP10_TYPE	=>	'qtl';
use constant BULK_WP10_TYPE	=>	'bulkqtl';

use constant DATA_FILETYPE	=>	'data';
use constant SQTLSEEKER_FILETYPE	=>	'sqtlseeker';
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
			'gene_name'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
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

my %QTL_INDEXES = (
	+DEFAULT_WP10_TYPE => DEFAULT_WP10_INDEX,
	+BULK_WP10_TYPE => BULK_WP10_INDEX,
);

my @DEFAULTS = (
	['use_https' => 'false' ],
	['nodes' => [ 'localhost' ] ],
	['port' => '' ],
	['path_prefix' => '' ],
	['user' => '' ],
	['pass' => '' ],
	['request_timeout' => 3600],
);

my %skipCol = (
	'chromosome_name'	=>	undef,
);

my $doClean = undef;
if(scalar(@ARGV) > 0 && $ARGV[0] eq '-C') {
	$doClean = 1;
	shift(@ARGV);
}

my $doSimul = undef;
if(scalar(@ARGV) > 0 && $ARGV[0] eq '-s') {
	$doSimul = 1;
	shift(@ARGV);
}

if(scalar(@ARGV)>=3) {
	# First, let's read the configuration
	my $iniFile = shift(@ARGV);
	# Defined outside
	my $cachingDir = shift(@ARGV);
	
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
	
	# Zeroth, load the data model
	
	# Let's parse the model
	my $modelFile = $ini->val($BP::Loader::Mapper::SECTION,'model');
	# Setting up the right path on relative cases
	$modelFile = File::Spec->catfile(File::Basename::dirname($iniFile),$modelFile)  unless(File::Spec->file_name_is_absolute($modelFile));

	$LOG->info("Parsing model $modelFile...");
	my $model = undef;
	eval {
		$model = BP::Model->new($modelFile);
	};
	
	if($@) {
		Carp::croak('ERROR: Model parsing and validation failed. Reason: '.$@);
	}
	$LOG->info("\tDONE!");
	
	# First, explicitly create the caching directory
	my $workingDir = BP::DCCLoader::WorkingDir->new($cachingDir);
	
	# Defined outside
	my($p_Gencode,$p_PAR,$p_GThash) = BP::DCCLoader::Parsers::GencodeGTFParser::getGencodeCoordinates($model,$workingDir,$ini);
	# Collapsing Gencode unique genes and transcripts into Ensembl's hash
	@{$p_GThash}{keys(%{$p_PAR})} = values(%{$p_PAR});
	
	# The elasticsearch database connection
	my $es = Search::Elasticsearch->new(@connParams,'nodes' => $confValues{nodes});
	# Setting up the parameters to the JSON serializer
	$es->transport->serializer->JSON->convert_blessed;
	
	if($doClean) {
		foreach my $indexName (values(%QTL_INDEXES)) {
			$es->indices->delete('index' => $indexName)  if($es->indices->exists('index' => $indexName));
		}
	}
	foreach my $mappingName (keys(%QTL_TYPES)) {
		my $indexName = $QTL_INDEXES{$mappingName};
		$es->indices->create('index' => $indexName)  unless($es->indices->exists('index' => $indexName));
		$es->indices->put_mapping(
			'index' => $indexName,
			'type' => $mappingName,
			'body' => {
				$mappingName => $QTL_TYPES{$mappingName}
			}
		);
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
		
		if($basename =~ /^([^_.]+)[_.]([^_]+)_(.+)_summary\.hdf5\.txt$/) {
			$mappingName = DEFAULT_WP10_TYPE;
			$cell_type = $1;
			$qtl_source = $2;
			$colSep = qr/\t/;
			$fileType = DATA_FILETYPE;
			$geneIdKey = 'geneID';
			$snpIdKey = 'rs';
		} elsif($basename =~ /^([^_.]+)[_.]([^_]+)_(.+)_all_summary\.txt$/) {
			$mappingName = BULK_WP10_TYPE;
			$cell_type = $1;
			$qtl_source = $2;
			$colSep = ' ';
			$colsLine = 'geneID rs pos beta pv qv_all';
			$fileType = BULK_FILETYPE;
			$geneIdKey = 'geneID';
			$snpIdKey = 'rs';
		} elsif($basename =~ /^([^.]+)\.([^.]+)\.sig5\.tsv$/) {
			$mappingName = DEFAULT_WP10_TYPE;
			$cell_type = $1;
			$qtl_source = $2;
			$colSep = qr/\t/;
			$fileType = SQTLSEEKER_FILETYPE;
			$geneIdKey = 'geneId';
			$snpIdKey = 'snpId';
		} else {
			$LOG->info("Discarding file $file (unknown type)");
			next;
		}
		my $indexName = $QTL_INDEXES{$mappingName};
		
		$LOG->info("Processing $file (cell type $cell_type, type $fileType, source $qtl_source)");
		if(open(my $CSV,'<:encoding(UTF-8)',$file)) {
			
			my @bes_params = (
				index   => $indexName,
				type    => $mappingName,
			);
			push(@bes_params,'max_count' => $ini->val('mapper','batch-size'))  if($ini->exists('mapper','batch-size'));
			
			# The bulk helper (for massive insertions)
			my $bes = $es->bulk_helper(@bes_params);

			unless(defined($colsLine)) {
				$colsLine = <$CSV>;
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
							
							$bes->index({ 'source' => \%entry })  unless($doSimul);
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
							my $lastdot = rindex($data{$geneIdKey},'.');
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
							$entry{'histone'} = 'H3'.$qtl_source;
						}
						
					} elsif($fileType eq SQTLSEEKER_FILETYPE) {
						$entry{'pv'} = $data{'pv'}+0E0;
						$entry{'qv'} = $data{'qv'}+0E0;
						$entry{'F'} = $data{'F'}+0E0;
						my $ensemblGeneId = $data{$geneIdKey};
						$entry{'ensemblGeneId'} = $ensemblGeneId;
						$entry{'ensemblTranscriptId'} = [ $data{'tr.first'}, $data{'tr.second'} ];
						
						# Fetching the gene coordinates
						if(exists($p_GThash->{$ensemblGeneId})) {
							my $p_data = $p_GThash->{$ensemblGeneId};
							my $p_coordinates = $p_data->{'coordinates'}[0];
							$entry{'gene_chrom'} = $p_coordinates->{'chromosome'};
							$entry{'gene_start'} = $p_coordinates->{'chromosome_start'};
							$entry{'gene_end'} = $p_coordinates->{'chromosome_end'};
						}
						
						my %metrics = ();
						$entry{'metrics'} = \%metrics;
						foreach my $key ('nb.groups', 'md', 'F.svQTL', 'nb.perms', 'nb.perms.svQTL', 'pv.svQTL', 'qv.svQTL') {
							my $tKey = $key;
							$tKey =~ tr/./_/;
							$metrics{$tKey} = $data{$key} + 0E0;
						}
					}
					
					$bes->index({ 'source' => \%entry })  unless($doSimul);
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
				
				$bes->index({ 'source' => \%entry })  unless($doSimul);
			}
			
			$bes->flush();
			
			close($CSV);
		} else {
			Carp::croak("[ERROR] Unable to open $file. Reason: ".$!);
		}
	}
} else {
	print STDERR "Usage: $0 [-C] [-s] {ini file} {caching dir} {tab file}+\n";
}
