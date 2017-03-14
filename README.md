BLUEPRINT WP10 data loader
=======================

This data loader takes the data available at ftp://ftp.ebi.ac.uk/pub/databases/blueprint/blueprint_Epivar/ (files at `Pheno_Matrix` and `qtl_as`), and it stores the contents into the Elasticsearch database instance used by BLUEPRINT WP10 data portal.

This Perl script depends on the dependencies listed in [cpanfile](cpanfile), which can be installed using [cpanm](http://search.cpan.org/~miyagawa/App-cpanminus-1.7042/bin/cpanm) using a sentence like:

```
cpanm --installdeps .
```

* The data loader fetches a dbSNP copy in VCF format, as well as its index, based on the setting in the WP10 data model. See [blueprint-setup-wp10_template.ini](blueprint-setup-wp10_template.ini) for the settings.
* The data loader depends on [vcftools](https://vcftools.github.io/), which must be found through `PATH`.
* It also depends on `tabix`, available in [HTSlib / Samtools](http://www.htslib.org/) package.
