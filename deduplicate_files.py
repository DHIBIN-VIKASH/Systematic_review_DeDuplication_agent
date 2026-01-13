import re
import difflib
import os
import glob
import pandas as pd
import csv
import json

def normalize_text(text):
    if not text:
        return ""
    # Remove non-alphanumeric characters and lowercase
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).lower()

def title_similarity(a, b):
    if not a or not b: return 0
    # Quick length check
    if abs(len(a) - len(b)) > max(len(a), len(b)) * 0.2:
        return 0
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

class Record:
    def __init__(self, source_file, original_text, pmid=None, doi=None, title=None, authors=None, year=None, extra_data=None):
        self.source_file = source_file
        self.original_text = original_text
        self.pmid = str(pmid).strip() if pmid and str(pmid).strip().lower() != 'nan' else None
        
        # Normalize DOI
        self.doi = str(doi).lower().strip() if doi and str(doi).strip().lower() != 'nan' else None
        if self.doi:
            # Remove "http://doi.org/" or "https://doi.org/" or "doi:"
            self.doi = re.sub(r'https?://(dx\.)?doi\.org/', '', self.doi)
            self.doi = re.sub(r'^doi:\s*', '', self.doi)
            self.doi = self.doi.split(' ')[0] # Handle cases like "10.1001/jama.201.1 [doi]"
            
        self.title = str(title).strip() if title and str(title).strip().lower() != 'nan' else ""
        self.normalized_title = normalize_text(self.title)
        
        if isinstance(authors, list):
            self.authors = [str(a) for a in authors]
        elif authors and str(authors).lower() != 'nan':
            self.authors = [str(authors)]
        else:
            self.authors = []
            
        self.year = str(year).strip() if year and str(year).strip().lower() != 'nan' else None
        self.extra_data = extra_data or {}

    def is_duplicate_of(self, other):
        # 1. DOI Match (Strongest)
        if self.doi and other.doi and self.doi == other.doi:
            return True
        
        # 2. PMID Match
        if self.pmid and other.pmid and self.pmid == other.pmid:
            return True

        # 3. Exact Normalized Title Match (if title is long enough)
        if self.normalized_title and other.normalized_title and len(self.normalized_title) > 30: 
            if self.normalized_title == other.normalized_title:
                return True

        # 4. Title Similarity (95%+)
        if self.title and other.title and abs(len(self.title) - len(other.title)) < 40: 
            sim = title_similarity(self.title, other.title)
            if sim >= 0.95:
                return True
            # Relaxed match if year also matches
            if sim >= 0.85 and self.year and other.year and self.year == other.year:
                return True
        
        return False

def parse_pubmed(filename):
    records = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    blocks = re.split(r'\n(?=PMID- )', content)
    for block in blocks:
        if not block.strip(): continue
        
        pmid = re.search(r'^PMID- (.*)', block, re.M)
        doi = re.search(r'^LID - (.*) \[doi\]', block, re.M) or \
              re.search(r'^AID - (.*) \[doi\]', block, re.M) or \
              re.search(r'^SO  - .*?doi: (.*?)\.', block, re.M)
        title = re.search(r'^TI  - (.*?)(?=\n[A-Z]{2,4} - |\n\n|$)', block, re.S | re.M)
        year = re.search(r'^DP  - (\d{4})', block, re.M)
        authors = re.findall(r'^FAU - (.*)', block, re.M)
        
        t_str = ""
        if title:
            t_str = " ".join(line.strip() for line in title.group(1).split('\n'))

        records.append(Record(
            source_file=filename,
            original_text=block,
            pmid=pmid.group(1).strip() if pmid else None,
            doi=doi.group(1).strip() if doi else None,
            title=t_str,
            authors=authors,
            year=year.group(1).strip() if year else None
        ))
    return records

def parse_bib(filename):
    records = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    entries = re.findall(r'@\w+\s*\{.*?\n\}', content, re.S)
    for entry in entries:
        title_match = re.search(r'title\s*=\s*[\{"](.*?)[}\"],', entry, re.S | re.I) or \
                      re.search(r'title\s*=\s*\{(.*)\}', entry, re.S | re.I)
        doi_match = re.search(r'doi\s*=\s*[\{"](.*?)[}\"]', entry, re.S | re.I)
        year_match = re.search(r'year\s*=\s*[\{"]?(\d{4})[\"\}]?', entry, re.S | re.I)
        author_match = re.search(r'author\s*=\s*[\{"](.*?)[}\"]', entry, re.S | re.I)
        
        t_str = ""
        if title_match:
            t_str = " ".join(line.strip() for line in title_match.group(1).split('\n'))
            t_str = re.sub(r'[\{\}]', '', t_str)

        records.append(Record(
            source_file=filename,
            original_text=entry,
            doi=doi_match.group(1).strip() if doi_match else None,
            title=t_str,
            authors=author_match.group(1).split(' and ') if author_match else [],
            year=year_match.group(1).strip() if year_match else None
        ))
    return records

def parse_ris(filename):
    records = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    entries = re.split(r'\nER\s+-', content)
    for entry in entries:
        if not entry.strip(): continue
        
        title_match = re.search(r'^(?:TI|T1)\s+-\s+(.*)', entry, re.M | re.I)
        doi_match = re.search(r'^DO\s+-\s+(.*)', entry, re.M | re.I)
        year_match = re.search(r'^(?:PY|Y1)\s+-\s+(\d{4})', entry, re.M | re.I)
        authors = re.findall(r'^AU\s+-\s+(.*)', entry, re.M | re.I)
        
        t_str = title_match.group(1).strip() if title_match else ""

        records.append(Record(
            source_file=filename,
            original_text=entry + "\nER  -",
            doi=doi_match.group(1).strip() if doi_match else None,
            title=t_str,
            authors=authors,
            year=year_match.group(1).strip() if year_match else None
        ))
    return records

def parse_csv(filename):
    records = []
    try:
        # Detect delimiter
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.readline() + f.readline()
            dialect = csv.Sniffer().sniff(sample)
            f.seek(0)
            df = pd.read_csv(f, sep=dialect.delimiter)
    except Exception as e:
        try:
            df = pd.read_csv(filename, encoding='latin1')
        except:
            print(f"Error reading CSV {filename}: {e}")
            return []

    # Map headers
    cols = df.columns
    title_col = next((c for c in cols if any(x in c.lower() for x in ['title', 'ti', 'document name'])), None)
    doi_col = next((c for c in cols if any(x in c.lower() for x in ['doi', 'do', 'digital object identifier'])), None)
    pmid_col = next((c for c in cols if any(x in c.lower() for x in ['pmid', 'pubmed id', 'pm'])), None)
    author_col = next((c for c in cols if any(x in c.lower() for x in ['author', 'au', 'contributor'])), None)
    year_col = next((c for c in cols if any(x in c.lower() for x in ['year', 'py', 'publication date'])), None)

    for _, row in df.iterrows():
        title = row[title_col] if title_col else ""
        doi = row[doi_col] if doi_col else None
        pmid = row[pmid_col] if pmid_col else None
        authors = row[author_col] if author_col else ""
        year = row[year_col] if year_col else ""
        
        # Original text for CSV is the JSON of the row
        original_text = row.to_json()

        records.append(Record(
            source_file=filename,
            original_text=original_text,
            pmid=pmid,
            doi=doi,
            title=title,
            authors=str(authors).split(';') if authors else [],
            year=year,
            extra_data=row.to_dict()
        ))
    return records

def detect_and_parse(filename):
    ext = os.path.splitext(filename)[1].lower()
    
    with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
        head = f.read(2048)
    
    if 'PMID-' in head or ext == '.nbib':
        return parse_pubmed(filename), "PubMed"
    elif '@' in head and '{' in head:
        return parse_bib(filename), "BibTeX"
    elif 'TY  -' in head or 'ER  -' in head or ext == '.ris':
        return parse_ris(filename), "RIS"
    elif ext == '.csv':
        return parse_csv(filename), "CSV"
    elif 'PT ' in head and 'AU ' in head: # WoS Tab delimited
        return parse_csv(filename), "WoS-Tab"
    else:
        # Fallback to extension
        if ext == '.ris': return parse_ris(filename), "RIS"
        if ext == '.bib': return parse_bib(filename), "BibTeX"
        if ext == '.csv': return parse_csv(filename), "CSV"
        if ext == '.txt':
            # Could be anything, try RIS then PubMed
            if 'TY  -' in head: return parse_ris(filename), "RIS"
            if 'PMID-' in head: return parse_pubmed(filename), "PubMed"
    
    return [], None

def process_file(records, label, master_seen_dois, master_seen_titles, master_unique_list):
    print(f"Deduplicating {label}...")
    local_unique = []
    skipped = 0
    for r in records:
        # Check against master first
        if r.doi and r.doi in master_seen_dois:
            skipped += 1
            continue
        if r.normalized_title and r.normalized_title in master_seen_titles and len(r.normalized_title) > 30:
            skipped += 1
            continue
            
        is_dup = False
        for u in master_unique_list:
            if r.is_duplicate_of(u):
                is_dup = True
                break
        
        if is_dup:
            skipped += 1
            continue
        
        local_unique.append(r)
        master_unique_list.append(r)
        if r.doi: master_seen_dois.add(r.doi)
        if r.normalized_title: master_seen_titles.add(r.normalized_title)
            
    print(f"  - Kept {len(local_unique)} records, removed {skipped} duplicates.")
    return local_unique

def save_records(records, original_filename, format_label):
    if not records:
        print(f"No records to save for {original_filename}")
        return

    name, ext = os.path.splitext(original_filename)
    out_name = f"{name}_deduplicated{ext}"
    
    if format_label == "CSV" or format_label == "WoS-Tab":
        # Reconstruct DataFrame from extra_data
        data = [r.extra_data for r in records]
        df = pd.DataFrame(data)
        df.to_csv(out_name, index=False)
    elif format_label == "PubMed":
        with open(out_name, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(r.original_text.strip() for r in records))
    elif format_label == "BibTeX":
        with open(out_name, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(r.original_text.strip() for r in records))
    elif format_label == "RIS":
        with open(out_name, 'w', encoding='utf-8') as f:
            # Ensure each record has ER - if missing
            text = ""
            for r in records:
                t = r.original_text.strip()
                if not t.endswith("ER  -"):
                    t += "\nER  -"
                text += t + "\n\n"
            f.write(text)
    else:
        # Default fallback
        with open(out_name, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(str(r.original_text).strip() for r in records))
    
    print(f"Saved to {out_name}")

def main():
    # Find all potential files
    extensions = ['*.txt', '*.bib', '*.ris', '*.csv', '*.nbib', '*.ciw', '*.enw']
    files = []
    for ext in extensions:
        files.extend(glob.glob(ext))
    
    # Exclude script and deduplicated files
    files = [f for f in files if '_deduplicated' not in f and f not in ['deduplicate_files.py', 'count_records.py', 'verify_clean.py']]
    
    if not files:
        print("No input files found in the current directory.")
        print(f"Supported extensions: {', '.join(extensions)}")
        return

    print(f"Found {len(files)} files to process: {', '.join(files)}")

    master_seen_dois = set()
    master_seen_titles = set()
    master_unique_list = []

    all_processed = []

    # Process in a stable order (alphabetical) or maybe prioritized?
    # Usually PubMed/Cochrane are higher quality.
    files.sort()

    for f in files:
        records, format_label = detect_and_parse(f)
        if not format_label:
            print(f"Could not detect format for {f}, skipping.")
            continue
        
        print(f"Detected format: {format_label} for {f}")
        deduped = process_file(records, f, master_seen_dois, master_seen_titles, master_unique_list)
        all_processed.append((deduped, f, format_label))

    print("\n" + "="*30)
    print("DEDUPLICATION SUMMARY")
    print("="*30)
    total_in = 0
    total_out = 0
    for deduped, f, _ in all_processed:
        # Re-calc original count (this is a bit lazy but works)
        # We'll just trust the process_file output
        print(f"{f}: {len(deduped)} records kept")
        total_out += len(deduped)

    # Save results
    print("\nSaving files...")
    for deduped, f, format_label in all_processed:
        save_records(deduped, f, format_label)

    print("\nAll tasks completed.")

if __name__ == "__main__":
    main()
