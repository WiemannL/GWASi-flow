#!/usr/bin/env nextflow

/*
 * MTAG-flow - A Nextflow pipeline for GWAS data ingestion and MTAG processing
 */

// Default parameters
params.samples = params.samples ?: "samples.csv"
params.outdir  = params.outdir  ?: "results"
params.mtag_script = params.mtag_script ?: "mtag.py"

params.snp = "SNP"
params.a1  = "A1"
params.a2  = "A2"
params.z   = "Z"
params.n   = "N"
params.maf_min = 0.01

log.info """\
         MTAG-flow - pairwise MTAG PIPELINE
         ===================================
         GWAS CSV     : ${params.samples}
         Output dir   : ${params.outdir}
         """
         .stripIndent()

// Validate inputs
if (params.gwas_csv == null) {
    error "Please provide a CSV file with GWAS information using --gwas_csv"
}

//Read CSV file
Channel
    .fromPath(params.samples)
    .splitCsv(header:true)
    .set { sample_rows }


// Create tuple for each MTAG pairing
pairwise_inputs = sample_rows.map { row ->

    def brain_name = row.brain_gwas.replaceAll(/.*\//,'').replace('.txt','')

    tuple(
        row.inflammation_gwas,
        row.brain_gwas,
        "${params.outdir}/${brain_name}_MTAG"
    )
}


process RUN_MTAG {

    tag { outprefix }

    input:
    tuple val(infl_gwas),
          val(brain_gwas),
          val(outprefix)

    output:
    file("${outprefix}*") into mtag_results

    script:
    """
    run_mtag.sh \
        ${params.mtag_script} \
        "${infl_gwas},${brain_gwas}" \
        ${outprefix} \
        ${params.snp} \
        ${params.a1} \
        ${params.a2} \
        ${params.z} \
        ${params.n} \
        ${params.maf_min}
    """
}

workflow {
    RUN_MTAG(pairwise_inputs)
}




