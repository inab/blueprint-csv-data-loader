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
use Cwd;
use File::Basename qw();
use File::Spec;
use File::Temp qw();
use Log::Log4perl;
use MIME::Base64 qw();
use Net::FTP::AutoReconnect;
use Search::Elasticsearch 1.12;

use Set::IntervalTree;

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

use constant {
	DBSNP_BASE_TAG	=>	'dbsnp-ftp-base-uri',
	DBSNP_VCF_TAG	=>	'dbsnp-vcf',
	DBSNP_MERGED_TABLE_TAG	=>	'dbsnp-merged-table'
};

use constant DEFAULT_WP10_INDEX	=>	'wp10qtls';
use constant BULK_WP10_INDEX	=>	'wp10bulkqtls';
use constant VARIABILITY_WP10_INDEX	=>	'wp10qtls_variability';

use constant DEFAULT_WP10_TYPE	=>	'qtl';
use constant BULK_WP10_TYPE	=>	'bulkqtl';
use constant VARIABILITY_WP10_TYPE	=>	'qtl_variability';

use constant DATA_FILETYPE	=>	'data';
use constant SQTLSEEKER_FILETYPE	=>	'sqtlseeker';
use constant BULK_FILETYPE	=>	'bulk';
use constant VARIABILITY_FILETYPE	=>	'variability';

my %QTL_TYPES = (
	DEFAULT_WP10_TYPE() => {
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
			'rsId'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'dbSnpRef'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'dbSnpAlt'	=> {
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
			'MAF'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'double',
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
	BULK_WP10_TYPE() => {
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
	VARIABILITY_WP10_TYPE() => {
		'_all'	=> {
			'enabled'	=>	boolean::true
		},
		'properties' => {
			# This is 'mono', 'neut', 'tcel' or an array of 'mono', 'neut', 'tcel'
			'cell_type' => {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# This is 'gene', 'meth'
			'qtl_source' => {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			# Common columns
			# 'geneID' is really the QTL id
			'gene_id'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'gene_name'	=> {	# HGNC symbol
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
			},
			'gene_chrom'	=> {	# Chr
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'pos'	=> {	# Location
				'dynamic'	=>	boolean::false,
				'type'	=>	'long',
			},
			'arm'	=> {	# Arm
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'ensemblGeneId'	=> {	# Ensembl ID
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'feature'	=> {	# Feature
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'chromatin_state' => {	# Chromatin state *
				'dynamic'	=>	boolean::false,
				'type'	=>	'nested',
				'include_in_parent'	=>	boolean::true,
				'properties'	=> {
					'cell_type' => {	# This is 'mono', 'neut' or 'tcel'
						'dynamic'	=>	boolean::false,
						'type'	=>	'string',
						'index' => 'not_analyzed',
					},
					'state' => {
						'dynamic'	=>	boolean::false,
						'type'	=>	'string',
						'index' => 'not_analyzed',
					}
				}
			},
			'go_term'	=> {	# Feature
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
			},
			# The different ones for 'meth'
			# Genotype	Age	Sex	BMI	Alcohol	Monocyte count	Monocyte percentage	Analysis day
			# Genotype	Age	Sex	BMI	Alcohol	Neutrophil count	Neutrophil percentage	Neutrophil granularity	Neutrophil cellular content	Analysis day
			# Genotype	Age	Sex	BMI	Alcohol	Lymphocyte count	Lymphocyte percentage	Analysis day
			# The different ones for 'gene'
			# Genotype	Age	Sex	BMI	Alcohol	Monocyte count	Monocyte percentage	Analysis day
			# Genotype	Age	Sex	BMI	Alcohol	Neutrophil count	Neutrophil percentage	Neutrophil granularity	Neutrophil cellular count	Analysis day
			# Genotype	Age	Sex	BMI	Alcohol	Lymphocyte count	Lymphocyte percentage	Analysis day
			'variability'	=> {	# Feature
				'dynamic'	=>	boolean::false,
				'type'	=>	'string',
				'index' => 'not_analyzed',
			},
			'associated_chart'	=> {
				'dynamic'	=>	boolean::false,
				'type'	=>	'binary',
			},
		}
	},
);

my @variability_keys = (
	'Genotype',
	'Age',
	'Sex',
	'BMI',
	'Alcohol',
	'Monocyte count',
	'Monocyte percentage',
	'Neutrophil count',
	'Neutrophil percentage',
	'Neutrophil granularity',
	'Neutrophil cellular content',
	'Lymphocyte count',
	'Lymphocyte percentage',
	'Analysis day'
#	'Alysis day'
);

my %chromatin_keys = (
	'Chromatin state monocytes' => {
		'cell_type'	=> 'mono'
	},
	'Chromatin state neutrophils' => {
		'cell_type'	=> 'neut'
	},
	'Chromatin state T cells' => {
		'cell_type'	=> 'tcel'
	}
);

my %QTL_INDEXES = (
	DEFAULT_WP10_TYPE() => DEFAULT_WP10_INDEX,
	BULK_WP10_TYPE() => BULK_WP10_INDEX,
	VARIABILITY_WP10_TYPE() => VARIABILITY_WP10_INDEX,
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

# Global variables
my $doClean = undef;
my $doSimul = undef;
my $J = undef;


sub getMainSymbol($) {
	my($p_data) = @_;
	unless(exists($p_data->{'main_symbol'})) {
		my $main_symbol;
		foreach my $symbol (@{$p_data->{'symbol'}}) {
			next  if(substr($symbol,0,3) eq 'ENS' || $symbol =~ /^\d+$/);
			$main_symbol = $symbol;
			last;
		}
		$main_symbol = $p_data->{'symbol'}[0]  unless(defined($main_symbol));
		$p_data->{'main_symbol'} = $main_symbol;
	}

	return $p_data->{'main_symbol'};
}

sub rsIdRemapper($$\@) {
	my($localDbSnpVCFFile,$localDbSnpMergedTableFile,$p_files) = @_;
	
	# First pass, dbSNP rsId extraction for recognized files
	my %rsIdMapping = ();
	my %coordMapping = ();
	
	$LOG->info("dbSNP rsId and coordinate extraction from recognized files");
	my $RSIDFILE = File::Temp->new(TEMPLATE => 'WP10-rsid-XXXXXX',SUFFIX => '.txt',TMPDIR => 1);
	my $rsidFilename = $RSIDFILE->filename();
	my $rsidCount = 0;
	my $COORDFILE = File::Temp->new(TEMPLATE => 'WP10-coord-XXXXXX',SUFFIX => '.txt',TMPDIR => 1);
	my $coordFilename = $COORDFILE->filename();
	my $coordCount = 0;
	if(open(my $COORDFH,'>',$coordFilename)) {
		foreach my $file (@{$p_files}) {
			my $basename = File::Basename::basename($file);
			
			# Three different types of files
			my $mappingName;
			my $colsLine;
			my $colSep;
			my $fileType;
			my $cell_type;
			my @cell_types = ();
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
			} elsif($basename =~ /^([^.]+)\.([^.]+)\.sig5\.tsv$/) {
				$mappingName = DEFAULT_WP10_TYPE;
				$cell_type = $1;
				$qtl_source = $2;
				$colSep = qr/\t/;
				$fileType = SQTLSEEKER_FILETYPE;
				$geneIdKey = 'geneId';
				$snpIdKey = 'snpId';
			} else {
				$LOG->info("SKipping file $file");
				next;
			}
			
			$LOG->info("Extracting rsIds from $file (cell type $cell_type, type $fileType, source $qtl_source)");
			if(open(my $CSV,'<:encoding(UTF-8)',$file)) {
				unless(defined($colsLine)) {
					$colsLine = <$CSV>;
					chomp($colsLine);
				}
				my @cols = split($colSep,$colsLine,-1);
				
				while(my $line=<$CSV>) {
					chomp($line);
					my @vals = split($colSep,$line,-1);
					
					my %data = ();
					@data{@cols} = @vals;
					
					my $snp_id = $data{$snpIdKey};
					if($snp_id =~ /^rs([0-9]+)$/) {
						if(!exists($rsIdMapping{$snp_id})) {
							$rsidCount++;
							$rsIdMapping{$snp_id} = undef;
						}
					} elsif($snp_id =~ /^snp([0-9]+)_chr(.+)$/) {
						if(!exists($coordMapping{$2}) || !exists($coordMapping{$2}{$1})) {
							$coordMapping{$2} = {}  unless(exists($coordMapping{$2}));
							$coordMapping{$2}{$1} = undef;
							$coordCount ++;
						}
					} elsif($snp_id =~ /^([^:]+):([0-9]+)$/) {
						if(!exists($coordMapping{$1}) || !exists($coordMapping{$1}{$2})) {
							$coordMapping{$1} = {}  unless(exists($coordMapping{$1}));
							$coordMapping{$1}{$2} = undef;
							$coordCount ++;
						}
					}
				}
				
				close($CSV);
			} else {
				$LOG->logcroak("[ERROR] Unable to open $file. Reason: ".$!);
			}
		}
		
		# Tabix wants its input query file ordered by coordinates
		foreach my $chromosome (keys(%coordMapping)) {
			my @coords = sort { $a <=> $b } keys(%{$coordMapping{$chromosome}});
			foreach my $coord (@coords) {
				print $COORDFH $chromosome,"\t",$coord,"\n";
			}
		}
		close($COORDFH);
	} else {
		$LOG->logcroak("[ERROR] Unable to create temp $coordFilename. Reason: ".$!);
	}
	
	if($rsidCount > 0) {
		$LOG->info("dbSNP coordinates and MAF mapping from rsId ($rsidCount) using vcftools");
		# We need to get the merged rsIds
		# grep did not work because the large pattern file ate lots of memory
		# http://www.ncbi.nlm.nih.gov/books/NBK279185/#FTP.do_you_have_a_table_of_merged_snps_s
		if(open(my $SNPIDFH,'>',$rsidFilename) && open(my $MERGED,'-|','gunzip','-c',$localDbSnpMergedTableFile)) {
			# First add the merged ids
			while(my $line=<$MERGED>) {
				chomp($line);
				my($orig,$merged,undef) = split(/\t/,$line,3);
				my $rsId = 'rs'.$orig;
				
				if(exists($rsIdMapping{$rsId})) {
					my $rsIdMerged = 'rs'.$merged;
					
					unless(defined($rsIdMapping{$rsIdMerged})) {
						$rsIdMapping{$rsIdMerged} = {};
						print $SNPIDFH $rsIdMerged,"\n";
					}
					$rsIdMapping{$rsId} = $rsIdMapping{$rsIdMerged};
				}
			}
			
			close($MERGED);
			
			# Now add those rsId which were not merged
			foreach my $rsId (keys(%rsIdMapping)) {
				unless(defined($rsIdMapping{$rsId})) {
					$rsIdMapping{$rsId} = {};
					print $SNPIDFH $rsId,"\n";
				}
			}
			
			close($SNPIDFH);
		} else {
			$LOG->logcroak("[ERROR] Unable to either create temp $rsidFilename or grepping for merged rsIds. Reason: ".$!);
		}
		
		# This is needed due the unwanted behavior of vcftools, which always creates a log file
		my $curdir = getcwd();
		my $ND = File::Temp->newdir(TMPDIR => 1);
		chdir($ND->dirname);
		my $mappedRsidCount = 0;
		if(open(my $VCF,'-|','vcftools','--gzvcf',$localDbSnpVCFFile,'--snps',$rsidFilename,'--recode','--recode-INFO','CAF','--stdout')) {
			while(my $line=<$VCF>) {
				next  if(substr($line,0,1) eq '#');
				chomp($line);
				
				my($chromosome,$pos,$rsId,$REF,$ALT,undef,undef,$INFO) = split(/\t/,$line,-1);
				my $p_data = $rsIdMapping{$rsId};
				
				$p_data->{'chromosome'} = $chromosome;
				$p_data->{'pos'} = $pos+0;
				$p_data->{'rsId'} = [ $rsId ];
				$p_data->{'dbSnpRef'} = [ $REF ];
				$p_data->{'dbSnpAlt'} = [ $ALT ];
				
				my $MAF = 2.0;
				if($INFO =~ /CAF=([^;]+)/) {
					my @alleleFreqs = split(/,/,$1,-1);
					foreach my $alleleFreq (@alleleFreqs) {
						next  if($alleleFreq eq '.');
						
						# Number normalization
						$alleleFreq += 0e0;
						
						$MAF = $alleleFreq  if($MAF > $alleleFreq);
					}
				}
				
				# Assigning the minor allele frequency when the impossible value is not there
				$p_data->{'MAF'} = [ ($MAF < 2.0) ? $MAF : undef ];
				
				$mappedRsidCount++;
			}
			close($VCF);
		} else {
			$LOG->logcroak("[ERROR] Unable to run vcftools on $rsidFilename. Reason: ".$!);
		}
		$LOG->info("Mapped $mappedRsidCount different dbSNP rsId to coordinates\n");
		# Going back to the original directory
		chdir($curdir);
	} else {
		$LOG->info("Skipping dbSNP coordinates and MAF mapping from rsId");
	}
	
	if($coordCount > 0) {
		$LOG->info("dbSNP rsId and MAF mapping from coordinates ($coordCount) using tabix");
		my $mappedCoordCount = 0;
		if(open(my $TABIX,'-|','tabix','-p','vcf','-R',$coordFilename,$localDbSnpVCFFile)) {
			while(my $line=<$TABIX>) {
				next  if(substr($line,0,1) eq '#');
				chomp($line);
				
				my($chromosome,$pos,$rsId,$REF,$ALT,undef,undef,$INFO) = split(/\t/,$line,-1);
				
				my $p_data;
				if(defined($coordMapping{$chromosome}{$pos})) {
					$p_data = $coordMapping{$chromosome}{$pos};
				} else {
					$p_data = {
						'chromosome'	=> $chromosome,
						'pos'	=> $pos+0,
						'rsId'	=> [],
						'dbSnpRef'	=> [],
						'dbSnpAlt'	=> [],
						'MAF'	=> []
					};
					
					$coordMapping{$chromosome}{$pos} = $p_data;
					$mappedCoordCount++;
				}
				
				push(@{$p_data->{'rsId'}},$rsId);
				push(@{$p_data->{'dbSnpRef'}},$REF);
				push(@{$p_data->{'dbSnpAlt'}},$ALT);
				
				my $MAF = 2.0;
				if($INFO =~ /CAF=([^;]+)/) {
					my @alleleFreqs = split(/,/,$1,-1);
					foreach my $alleleFreq (@alleleFreqs) {
						next  if($alleleFreq eq '.');
						
						# Number normalization
						$alleleFreq += 0e0;
						
						$MAF = $alleleFreq  if($MAF > $alleleFreq);
					}
				}
				
				# Assigning the minor allele frequency when the impossible value is not there
				push(@{$p_data->{'MAF'}},($MAF < 2.0) ? $MAF : undef);
			}
			
			close($TABIX);
		} else {
			$LOG->logcroak("[ERROR] Unable to run tabix on $coordFilename. Reason: ".$!);
		}
		$LOG->info("Mapped $mappedCoordCount different dbSNP coordinates to rsId\n");
	} else {
		$LOG->info("Skipping dbSNP rsId and MAF mapping from coordinates");
	}
	
	return (\%rsIdMapping,\%coordMapping);
}

sub bulkInsertion($\%\%\%\%\%\@) {
	my($ini,$p_GThash,$p_trees,$p_rsIdMapping,$p_coordMapping,$p_chartMapping,$p_files) = @_;
	
	# Database connection setup
	
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
				$LOG->logcroak("ERROR: required parameter $key not found in section ".INI_SECTION);
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
		$LOG->logcroak("ERROR: Unable to read section ".INI_SECTION);
	}
	
	my @connParams = ();
	
	foreach my $key ('use_https','port','path_prefix','userinfo','request_timeout') {
		if(exists($confValues{$key}) && defined($confValues{$key}) && length($confValues{$key}) > 0) {
			push(@connParams,$key => $confValues{$key});
		}
	}
	
	# The elasticsearch database connection
	my $es;
	
	unless($doSimul) {
		$es = Search::Elasticsearch->new(@connParams,'nodes' => $confValues{nodes});
		# Setting up the parameters to the JSON serializer
		$es->transport->serializer->JSON->convert_blessed;
	}
	
	my %alreadyCleansed = ();
	my %alreadyTypeCreated = ();
	
	$LOG->info("File insertion");
	foreach my $file (@{$p_files}) {
		my $basename = File::Basename::basename($file);
		
		# Three different types of files
		my $mappingName;
		my $colsLine;
		my $colSep;
		my $fileType;
		my $cell_type;
		my @cell_types = ();
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
		} elsif($basename =~ /^([^_]+)_([^_]+)_hypervar_S[13]\.txt\.csv$/) {
			$mappingName = VARIABILITY_WP10_TYPE;
			$cell_type = $1;
			@cell_types = split(/\+/,$cell_type);
			$qtl_source = $2;
			$colSep = qr/\t/;
			$fileType = VARIABILITY_FILETYPE;
			$geneIdKey = $qtl_source eq 'meth' ? 'Probe ID' : 'Ensembl ID';
		} else {
			$LOG->info("Discarding file $file (unknown type)");
			next;
		}
		my $indexName = $QTL_INDEXES{$mappingName};
		
		unless($doSimul) {
			if($doClean && !exists($alreadyCleansed{$indexName})) {
				if($es->indices->exists('index' => $indexName)) {
					$LOG->info("Removing index $indexName (and its type mappings)");
					$es->indices->delete('index' => $indexName);
				}
				
				$alreadyCleansed{$indexName} = undef;
			}
			
			unless(exists($alreadyTypeCreated{$mappingName})) {
				unless($es->indices->exists('index' => $indexName)) {
					$LOG->info("Creating index $indexName");
					$es->indices->create('index' => $indexName);
				}
			
				$LOG->info("Assuring type mapping $mappingName exists on index $indexName");
				$es->indices->put_mapping(
					'index' => $indexName,
					'type' => $mappingName,
					'body' => {
						$mappingName => $QTL_TYPES{$mappingName}
					}
				);
				$alreadyTypeCreated{$mappingName} = undef;
			}
		}
		
		$LOG->info("Processing $file (cell type $cell_type, type $fileType, source $qtl_source)");
		if(open(my $CSV,'<:encoding(UTF-8)',$file)) {
			my $bes;
			
			# The bulk helper (for massive insertions)
			unless($doSimul) {
				my @bes_params = (
					index   => $indexName,
					type    => $mappingName,
				);
				push(@bes_params,'max_count' => $ini->val($BP::Loader::Mapper::SECTION,BP::Loader::Mapper::BATCH_SIZE_KEY))  if($ini->exists($BP::Loader::Mapper::SECTION,BP::Loader::Mapper::BATCH_SIZE_KEY));
				
				$bes = $es->bulk_helper(@bes_params);
			}

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
							
							if($doSimul) {
								print STDERR $J->encode(\%entry),"\n";
							} else {
								$bes->index({ 'source' => \%entry });
							}
						}
						$bulkGeneId = $vals[0];
						$bulkData = '';
					}
					$bulkData .= join('\t',@vals[1..$#vals]) . "\n";
				} elsif($fileType eq VARIABILITY_FILETYPE) {
					my %data = ();
					@data{@cols} = @vals;
					
					my $qtl_id = $data{$geneIdKey};
					my %entry = (
						'cell_type' => \@cell_types,
						'qtl_source' => $qtl_source,
						'gene_id' => $qtl_id,
					);
					
					$entry{'gene_name'} = $data{'HGNC symbol'}  if(exists($data{'HGNC symbol'}));
					$entry{'gene_chrom'} = $data{'Chr'}  if(exists($data{'Chr'}));
					$entry{'pos'} = $data{'Location'}  if(exists($data{'Location'}));
					$entry{'arm'} = $data{'Arm'}  if(exists($data{'Arm'}));
					$entry{'ensemblGeneId'} = $data{'Ensembl ID'}  if(exists($data{'Ensembl ID'}));
					$entry{'feature'} = $data{'Feature'}  if(exists($data{'Feature'}));
					
					if(exists($data{'Chromatin state'})) {
						$entry{'chromatin_state'} = {
							'cell_type' => $cell_type,
							'state' => $data{'Chromatin state'}
						};
					} else {
						my @chromatin_states = ();
						my $someExisted = undef;
						foreach my $chroKey (keys(%chromatin_keys)) {
							if(exists($data{$chroKey})) {
								$someExisted = 1;
								push(@chromatin_states, {
									'cell_type' => $chromatin_keys{$chroKey}{'cell_type'},
									'state' => $data{$chroKey}
								});
							}
						}
						$entry{'chromatin_state'} = \@chromatin_states  if($someExisted);
					}
					
					if(exists($data{'GO term'})) {
						my @go = split(/ *, */,$data{'GO term'});
						$entry{'go_term'} = \@go;
					}
					
					my @variabilities = ();
					foreach my $variability (@variability_keys) {
						if(exists($data{$variability}) && $data{$variability} ne '0') {
							push(@variabilities,$variability);
						}
					}
					$entry{'variability'} = \@variabilities;
					
					# And now, the associated chart
					if(exists($p_chartMapping->{$qtl_id})) {
						if(open(my $CH,'<:bytes',$p_chartMapping->{$qtl_id})) {
							local $/;
							binmode($CH);
							my $pngChart = <$CH>;
							close($CH);
							
							$entry{'associated_chart'} = MIME::Base64::encode($pngChart);
						} else {
							$LOG->warn("Unable to read chart for $qtl_id from ".$p_chartMapping->{$qtl_id});
						}
					} else {
						$LOG->warn("Missing chart for $qtl_id");
					}
					
					if($doSimul) {
						print STDERR $J->encode(\%entry),"\n";
					} else {
						$bes->index({ 'source' => \%entry });
					}
				} else {
					my %data = ();
					@data{@cols} = @vals;
					
					my %entry = (
						'cell_type' => $cell_type,
						'qtl_source' => $qtl_source,
						'gene_id' => $data{$geneIdKey},
						'snp_id' => $data{$snpIdKey},
					);
					
					# Now, let's set the dbSNP additional data
					my $doCoordMapping = undef;
					my $doRsIdMapping = undef;
					if($entry{'snp_id'} =~ /^rs[0-9]+$/) {
						if(exists($p_rsIdMapping->{$entry{'snp_id'}})) {
							$doCoordMapping = 1;
						} else {
							$entry{'rsId'} = [ $entry{'snp_id'} ];
						}
					} elsif($entry{'snp_id'} =~ /^snp([0-9]+)_chr(.+)$/) {
						# Curating the format of anonymous SNPs
						$entry{'pos'} = $1;
						
						# We need the chromosome from this perspective
						$doRsIdMapping = $2;
					} elsif($entry{'snp_id'} =~ /^([^:]+):([0-9]+)$/) {
						$entry{'pos'} = $2  unless(exists($entry{'pos'}));
						
						# We need the chromosome from this perspective
						$doRsIdMapping = $1;
					}
					
					if(defined($doCoordMapping)) {
						my $p_mapping = $p_rsIdMapping->{$entry{'snp_id'}};
						foreach my $key ('pos','rsId','dbSnpRef','dbSnpAlt','MAF') {
							$entry{$key} = $p_mapping->{$key};
						}
					} elsif(defined($doRsIdMapping) && exists($p_coordMapping->{$doRsIdMapping}) && exists($p_coordMapping->{$doRsIdMapping}{$entry{'pos'}}) && defined($p_coordMapping->{$doRsIdMapping}{$entry{'pos'}})) {
						my $p_mapping = $p_coordMapping->{$doRsIdMapping}{$entry{'pos'}};
						foreach my $key ('rsId','dbSnpRef','dbSnpAlt','MAF') {
							$entry{$key} = $p_mapping->{$key};
						}
					}
					
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
						
						my %metrics = ();
						$entry{'metrics'} = \%metrics;
						foreach my $key ('nb.groups', 'md', 'F.svQTL', 'nb.perms', 'nb.perms.svQTL', 'pv.svQTL', 'qv.svQTL') {
							my $tKey = $key;
							$tKey =~ tr/./_/;
							$metrics{$tKey} = $data{$key} + 0E0;
						}
					}
					
					# Now, let's complement this
					if(exists($entry{'ensemblTranscriptId'}) && !exists($entry{'ensemblGeneId'})) {
						my $ensemblTranscriptId = $entry{'ensemblTranscriptId'};
						# Fetching the gene coordinates
						$ensemblTranscriptId = substr($ensemblTranscriptId,0,rindex($ensemblTranscriptId,'.'));
						if(exists($p_GThash->{$ensemblTranscriptId})) {
							my $p_data = $p_GThash->{$ensemblTranscriptId};
							$entry{'ensemblGeneId'} = $p_data->{'feature_cluster_id'};
						}
					}

					if(exists($entry{'ensemblGeneId'})) {
						my $ensemblGeneId = $entry{'ensemblGeneId'};
						# Fetching the gene coordinates
						$ensemblGeneId = substr($ensemblGeneId,0,rindex($ensemblGeneId,'.'));
						
						if(exists($p_GThash->{$ensemblGeneId})) {
							my $p_data = $p_GThash->{$ensemblGeneId};
							unless(exists($entry{'gene_chrom'})) {
								my $p_coordinates = $p_data->{'coordinates'}[0];
								$entry{'gene_chrom'} = $p_coordinates->{'chromosome'};
								$entry{'gene_start'} = $p_coordinates->{'chromosome_start'};
								$entry{'gene_end'} = $p_coordinates->{'chromosome_end'};
							}
							
							$entry{'gene_name'} = getMainSymbol($p_data);
						} else {
							print STDERR "$qtl_source ENSID NOT FOUND $ensemblGeneId\n";
						}
					} elsif(exists($p_trees->{$entry{'gene_chrom'}})) {
						my $tree = $p_trees->{$entry{'gene_chrom'}};
						
						# Fetching the genes overlapping these coordinates
						my @geneNames = ();
						my @ensemblGeneIds = ();
						
						my $p_p_data = $tree->fetch($entry{'gene_start'},$entry{'gene_end'}+1);
						foreach my $p_data (@{$p_p_data}) {
							my $p_coordinates = $p_data->{'coordinates'}[0];
							
							push(@ensemblGeneIds,$p_coordinates->{'feature_id'});
							push(@geneNames,getMainSymbol($p_data));
						}
						
						if(scalar(@ensemblGeneIds)>0) {
							if(scalar(@ensemblGeneIds)>1) {
								$entry{'ensemblGeneId'} = \@ensemblGeneIds;
								$entry{'gene_name'} = \@geneNames;
							} else {
								$entry{'ensemblGeneId'} = $ensemblGeneIds[0];
								$entry{'gene_name'} = $geneNames[0];
							}
						}
					} else {
						$LOG->info("Mira cell_type => $cell_type, qtl_source => $qtl_source, gene_id => $entry{gene_id}");
					}
					
					if($doSimul) {
						print STDERR $J->encode(\%entry),"\n";
					} else {
						$bes->index({ 'source' => \%entry });
					}
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
				
				if($doSimul) {
					print STDERR $J->encode(\%entry),"\n";
				} else {
					$bes->index({ 'source' => \%entry });
				}
			}
			
			$bes->flush()  unless($doSimul);
			
			close($CSV);
		} else {
			$LOG->logcroak("[ERROR] Unable to open $file. Reason: ".$!);
		}
	}
}

if(scalar(@ARGV) > 0 && $ARGV[0] eq '-C') {
	$doClean = 1;
	shift(@ARGV);
}

if(scalar(@ARGV) > 0 && $ARGV[0] eq '-s') {
	shift(@ARGV);
	$doSimul = 1;
	
	use JSON qw();
	$J = JSON->new()->pretty(1)->convert_blessed(1)
}

if(scalar(@ARGV)>=3) {
	# First, let's read the configuration
	my $iniFile = shift(@ARGV);
	# Defined outside
	my $cachingDir = shift(@ARGV);
	
	my $ini = Config::IniFiles->new(-file => $iniFile, -default => 'main');
	
	# Getting the path to dbSNP file
	my $dbsnp_ftp_base = undef;
	my $dbsnp_vcf_file = undef;
	my $dbsnp_vcf_tbi_file = undef;
	my $dbsnp_merged_table_uri = undef;
	if($ini->exists(BP::DCCLoader::Parsers::DCC_LOADER_SECTION,DBSNP_BASE_TAG)) {
		$dbsnp_ftp_base = $ini->val(BP::DCCLoader::Parsers::DCC_LOADER_SECTION,DBSNP_BASE_TAG);
	} else {
		$LOG->logcroak("Configuration file $iniFile must have '".DBSNP_BASE_TAG."'");
	}
	if($ini->exists(BP::DCCLoader::Parsers::DCC_LOADER_SECTION,DBSNP_VCF_TAG)) {
		$dbsnp_vcf_file = $ini->val(BP::DCCLoader::Parsers::DCC_LOADER_SECTION,DBSNP_VCF_TAG);
	} else {
		$LOG->logcroak("Configuration file $iniFile must have '".DBSNP_VCF_TAG."'");
	}
	if($ini->exists(BP::DCCLoader::Parsers::DCC_LOADER_SECTION,DBSNP_MERGED_TABLE_TAG)) {
		$dbsnp_merged_table_uri = $ini->val(BP::DCCLoader::Parsers::DCC_LOADER_SECTION,DBSNP_MERGED_TABLE_TAG);
	} else {
		$LOG->logcroak("Configuration file $iniFile must have '".DBSNP_MERGED_TABLE_TAG."'");
	}
	
	
	# Filtering images directories from the files
	my @files = ();
	my %chartMapping = ();
	foreach my $file (@ARGV) {
		if(-d $file) {
			# Let's gather all the PNG files from the directory
			if(opendir(my $IMGDIR,$file)) {
				while(my $entry = readdir($IMGDIR)) {
					if($entry =~ /^(.+)\.png$/) {
						my $qtl_id = $1;
						my $fullEntry = File::Spec->catfile($file,$entry);
						
						if(-f $fullEntry && -r $fullEntry) {
							# Storing the full path for later processing
							$chartMapping{$qtl_id} = $fullEntry;
						}
					}
				}
				
				closedir($IMGDIR);
			} else {
				$LOG->logcroak("ERROR: Unable to open charts directory $file. Reason: ".$!);
			}
		} else {
			push(@files,$file);
		}
	}
	
	if(scalar(@files) == 0) {
		$LOG->logdie("This program needs at least one data file to insert.");
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
		$LOG->logcroak('ERROR: Model parsing and validation failed. Reason: '.$@);
	}
	$LOG->info("\tDONE!");
	
	# Now, let's patch the properies of the different remote resources, using the properties inside the model
	eval {
		$model->annotations->applyAnnotations(\($dbsnp_ftp_base,$dbsnp_vcf_file,$dbsnp_merged_table_uri));
		$dbsnp_vcf_tbi_file = $dbsnp_vcf_file . '.tbi';
	};
	
	# First, explicitly create the caching directory
	my $workingDir = BP::DCCLoader::WorkingDir->new($cachingDir);
	
	# And translate dbSNP uri to URI objects
	$dbsnp_ftp_base = URI->new($dbsnp_ftp_base);
	$dbsnp_merged_table_uri = URI->new($dbsnp_merged_table_uri);
	
	# Fetching dbSNP file in compressed VCF format, as well as its index
	my $localDbSnpVCFFile;
	my $localDbSnpVCFtbiFile;
	my $localDbSnpMergedTableFile;
	
	{
		# Defined outside
		my $ftpServer = undef;
		
		# Fetching FTP resources
		$LOG->info("Connecting to $dbsnp_ftp_base...");
		
		my $dbSnpHost = $dbsnp_ftp_base->host();
		$ftpServer = Net::FTP::AutoReconnect->new($dbSnpHost,Debug=>0) || $LOG->logcroak("FTP connection to server ".$dbSnpHost." failed: ".$@);
		$ftpServer->login(BP::DCCLoader::WorkingDir::ANONYMOUS_USER,BP::DCCLoader::WorkingDir::ANONYMOUS_PASS) || $LOG->logcroak("FTP login to server $dbSnpHost failed: ".$ftpServer->message());
		$ftpServer->binary();
		
		my $dbSnpPath = $dbsnp_ftp_base->path;
		
		$localDbSnpVCFFile = $workingDir->cachedGet($ftpServer,$dbSnpPath.'/'.$dbsnp_vcf_file);
		$LOG->logcroak("FATAL ERROR: Unable to fetch file $dbsnp_vcf_file from $dbSnpPath (host $dbSnpHost)")  unless(defined($localDbSnpVCFFile));
		
		$localDbSnpVCFtbiFile = $workingDir->cachedGet($ftpServer,$dbSnpPath.'/'.$dbsnp_vcf_tbi_file);
		$LOG->logcroak("FATAL ERROR: Unable to fetch file $dbsnp_vcf_tbi_file from $dbSnpPath (host $dbSnpHost)")  unless(defined($localDbSnpVCFtbiFile));
		
		my $dbSnpMergedTablePath = $dbsnp_merged_table_uri->path;
		$localDbSnpMergedTableFile = $workingDir->cachedGet($ftpServer,$dbSnpMergedTablePath);
		$LOG->logcroak("FATAL ERROR: Unable to fetch $dbSnpMergedTablePath (host $dbSnpHost)")  unless(defined($localDbSnpMergedTableFile));
		
		$ftpServer->disconnect()  if($ftpServer->can('disconnect'));
		$ftpServer->quit()  if($ftpServer->can('quit'));
		$ftpServer = undef;
	}
	
	# Defined outside
	my($p_Gencode,$p_PAR,$p_GThash) = BP::DCCLoader::Parsers::GencodeGTFParser::getGencodeCoordinates($model,$workingDir,$ini);
	# Collapsing Gencode unique genes and transcripts into Ensembl's hash
	@{$p_GThash}{keys(%{$p_PAR})} = values(%{$p_PAR});
	
	# Now, we are going to have a forest, where each interval tree is a chromosome
	$LOG->info("Generating interval forest");
	my %trees = ();
	foreach my $node (values(%{$p_GThash})) {
		if($node->{feature} eq 'gene') {
			my $p_coord = $node->{coordinates}[0];
			my $tree;
			if(exists($trees{$p_coord->{chromosome}})) {
				$tree = $trees{$p_coord->{chromosome}};
			} else {
				$tree = Set::IntervalTree->new();
				$trees{$p_coord->{chromosome}} = $tree;
			}
			
			# As ranges are half-open, take it into account
			$tree->insert($node,$p_coord->{chromosome_start},$p_coord->{chromosome_end}+1);
		}
	}
	$LOG->info("Interval forest ready!");
	
	# First pass, dbSNP rsId extraction for recognized files
	my($p_rsIdMapping,$p_coordMapping) = rsIdRemapper($localDbSnpVCFFile,$localDbSnpMergedTableFile,@files);
	
	# Second pass, file insertion
	bulkInsertion($ini,%{$p_GThash},%trees,%{$p_rsIdMapping},%{$p_coordMapping},%chartMapping,@files);
	$LOG->info("Insertions finished");
} else {
	print STDERR "Usage: $0 [-C] [-s] {ini file} {caching dir} {images_dirs}* {tab file}+\n";
}
