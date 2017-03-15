#!/bin/bash

biocManifURL=https://bioconductor.org/packages/release/data/annotation/src/contrib/IlluminaHumanMethylation450k.db_2.0.9.tar.gz

if [ $# -gt 0 ] ; then
	outfile="$1"
	
	tempD="$(mktemp -d)"
	trap "rm -rf -- '$tempD'" EXIT
	wget -nv -P "$tempD" "${biocManifURL}"
	biocManif="${tempD}"/"$(basename "${biocManifURL}")"
	sqliteRelTarPath="$(tar tzf "${biocManif}" | grep sqlite)"
	sqliteRelPath="$(basename "${sqliteRelTarPath}")"
	tar -x -z --transform 's|^.*/||' -C "${tempD}" -f "${biocManif}" "${sqliteRelTarPath}"
	
	sqliteManifPath="${tempD}"/"${sqliteRelPath}"
	
	sqlite3 -batch -separator $'\t' "${sqliteManifPath}" 'select probe_id, Chromosome_37, Coordinate_37 from probedesign;' > "${outfile}"
fi
