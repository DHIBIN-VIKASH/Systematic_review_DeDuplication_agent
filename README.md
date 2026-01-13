# SMR Deduplication Agent

This agent automates the deduplication of bibliographic records from multiple academic databases (PubMed, Embase, Cochrane, Scopus, Web of Science, etc.). It uses a hierarchical matching logic (DOI, PMID, Exact Title, and Fuzzy Title Similarity) to identify and remove duplicates across different file formats.

## Features
- **General Format Support**: Automatically detects and handles multiple formats:
    - **PubMed**: `.txt`, `.nbib`
    - **RIS**: `.ris` (Standard, Embase, Cochrane, Scopus, etc.)
    - **BibTeX**: `.bib` (Web of Science, Scopus)
    - **CSV/Excel-exported CSV**: Handles various column naming conventions.
    - **Tab-Delimited**: Web of Science `.txt` exports.
- **Automatic Discovery**: Scans the directory for all supported files—no need for specific filenames.
- **Cross-Database Deduplication**: Removes duplicates across all provided search results.
- **Hierarchical Matching**: 
  1. **DOI Match** (Highest Priority, normalized for many variations)
  2. **PMID Match**
  3. **Exact Title Match** (Case and character normalized)
  4. **Fuzzy Title Similarity** (95% similarity or 85% with year match)

## How to Use

### 1. Prepare Your Input Files
Place your exported search results in this folder. You can keep the original filenames exported from the databases. Supported extensions: `.txt`, `.bib`, `.ris`, `.csv`, `.nbib`, `.ciw`, `.enw`.

### 2. Run the Program
Ensure you have Python and `pandas` installed. Run the deduplication script:
```powershell
python deduplicate_files.py
```

### 3. Get the Output
The program will generate deduplicated files for each input, appending `_deduplicated` to the filename (e.g., `scopus_export_deduplicated.bib`).

### 4. Verify Counts
You can run the counting scripts to see how many unique records were found:
```powershell
python count_records.py   # Counts records in input files
python verify_clean.py    # Counts records in output files
```

## Matching Logic Details
- **Normalization**: Titles are stripped of special characters and whitespace for exact matching.
- **DOIs**: Automatically extracts and normalizes DOIs from various prefixes and formats.
- **Stability**: Files are processed in alphabetical order to ensure consistent results across runs.
- **Intra-file & Inter-file**: Deduplicates within each file and then checks against all previously processed records.
