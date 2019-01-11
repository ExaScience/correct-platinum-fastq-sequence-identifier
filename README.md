# correct-platinum-fastq-sequence-identifier
## Script for correcting sequence identifiers in Platinum sequences

The fastq files at the [European Nucleotide Archive](https://www.ebi.ac.uk/ena/data/view/PRJEB3381) provide the Illumina sequence identifiers only as comments. However, for optical duplicate marking to work correctly in elPrep, GATK, and Picard, they need to be the actual sequence identifiers in the fastq files before they are aligned with bwa mem. This script ensures that this is the case.
