import re
import difflib
import os
import glob
import pandas as pd
import csv
import json
import logging
import argparse
import time
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
from rich.text import Text
from rich.live import Live
from rich.columns import Columns
from rich import box

console = Console()

# Structured logging — file only (rich handles console display)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("dedup_run.log", mode='a', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

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
    def __init__(self, source_file, original_text, pmid=None, doi=None, title=None, authors=None, year=None, abstract=None, extra_data=None):
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
        self.abstract = str(abstract).strip() if abstract and str(abstract).strip().lower() != 'nan' else ""
        
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
            # Relaxed match if year also matches + author confirmation
            if sim >= 0.85 and self.year and other.year and self.year == other.year:
                # Additional author confirmation to reduce false positives
                if self.authors and other.authors:
                    a1 = self.authors[0].split(',')[0].strip().lower()
                    a2 = other.authors[0].split(',')[0].strip().lower()
                    if a1 and a2 and (a1 in a2 or a2 in a1):
                        return True
                else:
                    # No author data — fall back to title+year match
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
        abstract = re.search(r'^AB  - (.*?)(?=\n[A-Z]{2,4} - |\n\n|$)', block, re.S | re.M)
        year = re.search(r'^DP  - (\d{4})', block, re.M)
        authors = re.findall(r'^FAU - (.*)', block, re.M)
        
        t_str = ""
        if title:
            t_str = " ".join(line.strip() for line in title.group(1).split('\n'))

        ab_str = ""
        if abstract:
            ab_str = " ".join(line.strip() for line in abstract.group(1).split('\n'))

        records.append(Record(
            source_file=filename,
            original_text=block,
            pmid=pmid.group(1).strip() if pmid else None,
            doi=doi.group(1).strip() if doi else None,
            title=t_str,
            abstract=ab_str,
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
        title_match = re.search(r'title\s*=\s*[\{"](.*?)[}\\"],', entry, re.S | re.I) or \
                      re.search(r'title\s*=\s*\{(.*)\}', entry, re.S | re.I)
        abstract_match = re.search(r'abstract\s*=\s*[\{"](.*?)[}\\"]', entry, re.S | re.I)
        doi_match = re.search(r'doi\s*=\s*[\{"](.*?)[}\\"]', entry, re.S | re.I)
        year_match = re.search(r'year\s*=\s*[\{"]?(\d{4})[\\"\}]?', entry, re.S | re.I)
        author_match = re.search(r'author\s*=\s*[\{"](.*?)[}\\"]', entry, re.S | re.I)
        
        t_str = ""
        if title_match:
            t_str = " ".join(line.strip() for line in title_match.group(1).split('\n'))
            t_str = re.sub(r'[\{\}]', '', t_str)

        ab_str = ""
        if abstract_match:
            ab_str = " ".join(line.strip() for line in abstract_match.group(1).split('\n'))
            ab_str = re.sub(r'[\{\}]', '', ab_str)

        records.append(Record(
            source_file=filename,
            original_text=entry,
            doi=doi_match.group(1).strip() if doi_match else None,
            title=t_str,
            abstract=ab_str,
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
        abstract_match = re.search(r'^(?:AB|N2)\s+-\s+(.*?)(?=\n[A-Z][A-Z0-9]\s+-|$)', entry, re.S | re.M | re.I)
        doi_match = re.search(r'^DO\s+-\s+(.*)', entry, re.M | re.I)
        year_match = re.search(r'^(?:PY|Y1)\s+-\s+(\d{4})', entry, re.M | re.I)
        authors = re.findall(r'^AU\s+-\s+(.*)', entry, re.M | re.I)
        
        t_str = title_match.group(1).strip() if title_match else ""
        ab_str = ""
        if abstract_match:
            ab_str = " ".join(line.strip() for line in abstract_match.group(1).split('\n'))

        records.append(Record(
            source_file=filename,
            original_text=entry + "\nER  -",
            doi=doi_match.group(1).strip() if doi_match else None,
            title=t_str,
            abstract=ab_str,
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
    abstract_col = next((c for c in cols if any(x in c.lower() for x in ['abstract', 'ab'])), None)
    doi_col = next((c for c in cols if any(x in c.lower() for x in ['doi', 'do', 'digital object identifier'])), None)
    pmid_col = next((c for c in cols if any(x in c.lower() for x in ['pmid', 'pubmed id', 'pm'])), None)
    author_col = next((c for c in cols if any(x in c.lower() for x in ['author', 'au', 'contributor'])), None)
    year_col = next((c for c in cols if any(x in c.lower() for x in ['year', 'py', 'publication date'])), None)

    for _, row in df.iterrows():
        title = row[title_col] if title_col else ""
        abstract = row[abstract_col] if abstract_col else ""
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
            abstract=abstract,
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
    logger.info(f"Deduplicating {label} ({len(records)} input records)...")
    local_unique = []
    skipped = 0
    skipped_by = {'doi': 0, 'title_exact': 0, 'fuzzy': 0}
    for r in records:
        # Check against master first
        if r.doi and r.doi in master_seen_dois:
            skipped += 1
            skipped_by['doi'] += 1
            continue
        if r.normalized_title and r.normalized_title in master_seen_titles and len(r.normalized_title) > 30:
            skipped += 1
            skipped_by['title_exact'] += 1
            continue
            
        is_dup = False
        for u in master_unique_list:
            if r.is_duplicate_of(u):
                is_dup = True
                break
        
        if is_dup:
            skipped += 1
            skipped_by['fuzzy'] += 1
            continue
        
        local_unique.append(r)
        master_unique_list.append(r)
        if r.doi: master_seen_dois.add(r.doi)
        if r.normalized_title: master_seen_titles.add(r.normalized_title)
            
    logger.info(f"  → Kept {len(local_unique)}, removed {skipped} duplicates "
                f"(DOI: {skipped_by['doi']}, Title-exact: {skipped_by['title_exact']}, Fuzzy: {skipped_by['fuzzy']})")
    return local_unique, len(records)

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

FORMAT_COLORS = {
    "PubMed": "cyan",
    "BibTeX": "yellow",
    "RIS": "green",
    "CSV": "magenta",
    "WoS-Tab": "blue",
}

def build_summary_table(all_processed, title="Deduplication Results"):
    """Build a rich Table showing per-file dedup results."""
    table = Table(
        title=title,
        box=box.ROUNDED,
        title_style="bold bright_white",
        header_style="bold bright_cyan",
        border_style="bright_blue",
        show_lines=True,
        padding=(0, 1),
    )
    table.add_column("#", style="dim", justify="right", width=3)
    table.add_column("File", style="bold white", max_width=50)
    table.add_column("Format", justify="center", width=8)
    table.add_column("Input", justify="right", style="bright_white", width=7)
    table.add_column("Kept", justify="right", style="bold bright_green", width=7)
    table.add_column("Removed", justify="right", style="bold bright_red", width=9)
    table.add_column("% Dup", justify="right", width=7)

    total_in = 0
    total_out = 0
    for i, (deduped, f, fmt, original_count) in enumerate(all_processed, 1):
        removed = original_count - len(deduped)
        pct = f"{removed / original_count * 100:.1f}%" if original_count > 0 else "0%"
        color = FORMAT_COLORS.get(fmt, "white")
        table.add_row(
            str(i),
            os.path.basename(f),
            f"[{color}]{fmt}[/{color}]",
            str(original_count),
            str(len(deduped)),
            str(removed),
            pct,
        )
        total_in += original_count
        total_out += len(deduped)

    total_removed = total_in - total_out
    total_pct = f"{total_removed / total_in * 100:.1f}%" if total_in > 0 else "0%"
    table.add_row(
        "",
        "[bold bright_white]TOTAL[/]",
        "",
        f"[bold]{total_in}[/]",
        f"[bold bright_green]{total_out}[/]",
        f"[bold bright_red]{total_removed}[/]",
        f"[bold]{total_pct}[/]",
        style="on grey15",
    )
    return table, total_in, total_out


def main():
    parser = argparse.ArgumentParser(description="Deduplicate bibliographic files.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and report counts without saving output files.")
    args = parser.parse_args()

    logger.info(f"=== Deduplication Run Started at {datetime.now().isoformat()} ===")

    # ── Banner ──────────────────────────────────────────────────────────
    banner = Text()
    banner.append("\n  📚 Systematic Review ", style="bold bright_white")
    banner.append("DeDuplication Agent", style="bold bright_cyan")
    banner.append("  \n", style="bold bright_white")
    console.print(Panel(
        banner,
        border_style="bright_blue",
        box=box.DOUBLE_EDGE,
        padding=(0, 2),
    ))
    console.print(f"  [dim]Run started:[/] [bright_white]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/]")
    console.print()

    # ── Discover files ──────────────────────────────────────────────────
    extensions = ['*.txt', '*.bib', '*.ris', '*.csv', '*.nbib', '*.ciw', '*.enw']
    files = []
    for ext in extensions:
        files.extend(glob.glob(ext))

    files = [f for f in files if '_deduplicated' not in f and f not in ['deduplicate_files.py', 'count_records.py', 'verify_clean.py']]

    if not files:
        console.print("[bold red]  ✗ No input files found in the current directory.[/]")
        console.print(f"  [dim]Supported extensions: {', '.join(extensions)}[/]")
        logger.warning("No input files found.")
        return

    files.sort()
    logger.info(f"Found {len(files)} files to process: {', '.join(files)}")

    console.print(f"  [bright_white]Found [bold bright_cyan]{len(files)}[/bold bright_cyan] files to process[/]")
    for f in files:
        console.print(f"    [dim]•[/] [white]{f}[/]")
    console.print()

    # ── Process files with live progress ─────────────────────────────────
    master_seen_dois = set()
    master_seen_titles = set()
    master_unique_list = []
    all_processed = []

    with Progress(
        SpinnerColumn("dots", style="bright_cyan"),
        TextColumn("[bold bright_white]{task.description}"),
        BarColumn(bar_width=30, style="bright_blue", complete_style="bright_green", finished_style="bold bright_green"),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:

        overall_task = progress.add_task("Overall", total=len(files))

        for f in files:
            short = os.path.basename(f)
            progress.update(overall_task, description=f"Processing [bright_cyan]{short}[/]")

            records, format_label = detect_and_parse(f)
            if not format_label:
                console.print(f"  [yellow]⚠  Skipped[/] [dim]{short}[/] [yellow](unknown format)[/]")
                logger.warning(f"Could not detect format for {f}, skipping.")
                progress.advance(overall_task)
                continue

            logger.info(f"Detected format: {format_label} for {f}")

            # Sub-task for this file's record-level dedup
            file_task = progress.add_task(
                f"  [dim]↳ {short}[/]",
                total=len(records),
            )

            # Inline dedup with per-record progress
            local_unique = []
            skipped = 0
            skipped_by = {'doi': 0, 'title_exact': 0, 'fuzzy': 0}

            for r in records:
                if r.doi and r.doi in master_seen_dois:
                    skipped += 1; skipped_by['doi'] += 1
                    progress.advance(file_task)
                    continue
                if r.normalized_title and r.normalized_title in master_seen_titles and len(r.normalized_title) > 30:
                    skipped += 1; skipped_by['title_exact'] += 1
                    progress.advance(file_task)
                    continue

                is_dup = False
                for u in master_unique_list:
                    if r.is_duplicate_of(u):
                        is_dup = True
                        break

                if is_dup:
                    skipped += 1; skipped_by['fuzzy'] += 1
                    progress.advance(file_task)
                    continue

                local_unique.append(r)
                master_unique_list.append(r)
                if r.doi: master_seen_dois.add(r.doi)
                if r.normalized_title: master_seen_titles.add(r.normalized_title)
                progress.advance(file_task)

            color = FORMAT_COLORS.get(format_label, "white")
            kept = len(local_unique)
            console.print(
                f"  [bright_green]✓[/]  [{color}]{format_label:6s}[/{color}] "
                f"[white]{short}[/] → "
                f"[bright_green]{kept}[/] kept, "
                f"[bright_red]{skipped}[/] removed "
                f"[dim](DOI:{skipped_by['doi']} Title:{skipped_by['title_exact']} Fuzzy:{skipped_by['fuzzy']})[/]"
            )

            logger.info(f"  → Kept {kept}, removed {skipped} duplicates "
                        f"(DOI: {skipped_by['doi']}, Title-exact: {skipped_by['title_exact']}, Fuzzy: {skipped_by['fuzzy']})")

            all_processed.append((local_unique, f, format_label, len(records)))
            progress.advance(overall_task)

        progress.update(overall_task, description="[bold bright_green]✓ All files processed[/]")

    console.print()

    # ── Summary Table ───────────────────────────────────────────────────
    table, total_in, total_out = build_summary_table(all_processed)
    console.print(table)
    console.print()

    # Log summary
    logger.info("=" * 50)
    logger.info("DEDUPLICATION SUMMARY")
    logger.info("=" * 50)
    for deduped, f, _, original_count in all_processed:
        removed = original_count - len(deduped)
        logger.info(f"  {f}: {original_count} in → {len(deduped)} kept ({removed} duplicates removed)")
    logger.info(f"  TOTAL: {total_in} in → {total_out} kept ({total_in - total_out} duplicates removed)")

    if args.dry_run:
        console.print(Panel(
            "[bold yellow]DRY RUN — No files were saved.[/]",
            border_style="yellow",
            box=box.ROUNDED,
        ))
        logger.info("[DRY RUN] No files were saved.")
        return

    # ── Save outputs ────────────────────────────────────────────────────
    console.print("  [bold bright_white]Saving outputs…[/]")
    saved_files = []

    for deduped, f, format_label, _ in all_processed:
        save_records(deduped, f, format_label)
        name, ext = os.path.splitext(f)
        saved_files.append(f"{name}_deduplicated{ext}")

    # Master CSV
    all_unique = [r for deduped, _, _, _ in all_processed for r in deduped]
    if all_unique:
        master_df = pd.DataFrame([{
            'title': r.title,
            'abstract': r.abstract or '',
            'doi': r.doi or '',
            'pmid': r.pmid or '',
            'year': r.year or '',
            'authors': '; '.join(r.authors) if r.authors else '',
            'source_file': r.source_file
        } for r in all_unique])
        master_df.to_csv('master_deduplicated.csv', index=False)
        saved_files.append('master_deduplicated.csv')
        logger.info(f"Master CSV saved: master_deduplicated.csv ({len(master_df)} unique records)")

        missing_title = master_df['title'].eq('').sum()
        missing_abstract = master_df['abstract'].eq('').sum()
        if missing_title > 0:
            logger.warning(f"  {missing_title} records have no title")
        if missing_abstract > 0:
            logger.warning(f"  {missing_abstract} records have no abstract (may need manual review)")

    # Audit JSON
    summary = {
        'timestamp': datetime.now().isoformat(),
        'files_processed': len(all_processed),
        'total_input_records': total_in,
        'total_unique_records': total_out,
        'total_duplicates_removed': total_in - total_out,
        'per_file': [
            {'file': f, 'format': fmt, 'input': orig, 'output': len(d), 'removed': orig - len(d)}
            for d, f, fmt, orig in all_processed
        ]
    }
    with open('dedup_summary.json', 'w', encoding='utf-8') as jf:
        json.dump(summary, jf, indent=2)
    saved_files.append('dedup_summary.json')
    logger.info("Run summary saved: dedup_summary.json")

    # List saved files
    for sf in saved_files:
        console.print(f"    [bright_green]→[/] [white]{sf}[/]")
    console.print()

    # ── Quality warnings ────────────────────────────────────────────────
    warnings = []
    if all_unique:
        if missing_title > 0:
            warnings.append(f"[yellow]⚠  {missing_title} records have no title[/]")
        if missing_abstract > 0:
            warnings.append(f"[yellow]⚠  {missing_abstract} records have no abstract (may need manual review)[/]")
    if warnings:
        console.print(Panel(
            "\n".join(warnings),
            title="Quality Checks",
            title_align="left",
            border_style="yellow",
            box=box.ROUNDED,
            padding=(0, 1),
        ))
        console.print()

    # ── Final panel ─────────────────────────────────────────────────────
    total_removed = total_in - total_out
    final_msg = Text()
    final_msg.append(f"  {total_in}", style="bright_white bold")
    final_msg.append(" records scanned  →  ", style="dim")
    final_msg.append(f"{total_out}", style="bold bright_green")
    final_msg.append(" unique  •  ", style="dim")
    final_msg.append(f"{total_removed}", style="bold bright_red")
    final_msg.append(" duplicates removed  ", style="dim")

    console.print(Panel(
        final_msg,
        title="✅ Deduplication Complete",
        title_align="left",
        border_style="bright_green",
        box=box.DOUBLE_EDGE,
        padding=(0, 1),
    ))

    logger.info("All tasks completed.")


if __name__ == "__main__":
    main()
