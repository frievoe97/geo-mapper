## Geo Mapper – CSV to NUTS/LAU Geodata

This project provides a small, interactive command‑line tool to map values from a
single CSV/Excel file (e.g. German regions, districts, municipalities) to official
NUTS/LAU geodata. The tool guides you through:

- loading a source file
- selecting ID/name/value columns
- choosing the desired geodata level and vintage (NUTS 0–3 or LAU, by year)
- running several robust, conservative matching strategies
- optionally fixing remaining rows via an interactive manual mapping UI
- exporting the results into a structured `results/` folder

The focus is on transparency and safety: each matching step is logged, and the
output clearly records which mapper produced each match.

---

## Installation and Setup

### 1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
```

On Windows, use:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

You can deactivate the environment at any time with:

```bash
deactivate
```

### 3. Optional: use the Makefile helpers

The repository contains a small `Makefile` with convenience targets:

```bash
make install   # create venv + install dependencies
make run       # run the main script with default test data (if configured)
make clean     # remove generated outputs under results/
```

---

## Basic Usage

Once the environment is set up and dependencies are installed:

```bash
python main.py --data path/to/your_file.csv
```

Supported input formats:

- CSV (`.csv`)
- Excel (`.xlsx`, `.xlsm`, `.xls`)

On startup the tool will:

1. Load the input file and drop completely empty rows.
2. Ask you which columns contain IDs, names and additional value columns (unless
   they are specified through a JSON meta file, see below).
3. Let you choose the geodata level and year (NUTS 0–3 or LAU).
4. Load the matching geodata CSV files from `geodata_clean/csv`.
5. Let you choose which mapping steps (mappers) to run, or select them
   automatically.
6. Optionally ask you to choose a single geodata source for export/manual
   mapping.
7. Offer an interactive manual mapping step for remaining unmapped rows.
8. Export multiple CSVs into `results/…` with mapped, unmapped and unused
   entries.

### Command-line options

```bash
python main.py --data DATA_FILE [--json META_JSON]
               [--auto-mappers true|false]
               [--auto-export-source true|false]
```

- `--data/-d` (required)  
  Path to the input CSV/Excel file.

- `--json/-j` (optional)  
  Path to a JSON file that is:

  - copied into the export folder, and
  - used as meta configuration (e.g. to predefine column names, level, year).

- `--auto-mappers` (default: `true`)  
  If `true`, skip the interactive mapper selection and automatically choose
  a sensible set of mappers based on the selected ID/name columns.

- `--auto-export-source` (default: `false`)  
  If `true`, automatically select the “best” geodata source for export/manual
  mapping (based on coverage statistics) instead of asking.

Boolean flags accept typical truthy values like `1`, `true`, `yes`, `y`
case‑insensitively.

---

## Data Flow and Pipeline

The processing pipeline is orchestrated in `main.py` via a sequence of steps
defined in the `pipeline` package:

1. **Load CSV/Excel** (`pipeline.load_csv.load_csv_step`)  
   Detects CSV delimiter (`,`, `;`, tab, `|`), loads the file and drops rows that
   are completely empty. Remembers the input file name for use in result paths.

2. **Select ID/name/value columns** (`pipeline.select_column.narrow_to_single_column_step`)  
   Interactively lets you pick:

   - an optional ID column,
   - an optional name column,
   - optional additional value columns to carry through to the export.  
     At least one of ID or name must be selected. Choices can also be provided
     via meta JSON.

3. **Normalize source column** (`pipeline.normalize.normalize_source_step`)  
   Adds a `normalized_source` column created from the name/source column using a
   robust normalization (casefold, umlaut handling, strip diacritics, remove
   digits and punctuation, collapse whitespace). All name‑based mappers build
   on this.

4. **Select geodata level & year** (`pipeline.geodata_selection.select_geodata_step`)  
   Lets you choose between `NUTS 0–3` and `LAU`, and then a year/version based
   on available directories under `geodata_clean/csv`. Selections can also
   come from meta JSON.

5. **Load matching geodata CSVs** (`pipeline.geodata_loader.load_geodata_files_step`)  
   Loads all geodata CSV files that match the chosen type/level/year and
   stores them in memory.

6. **Select mappers** (`pipeline.mapping.selection.select_mappers_step`)  
   Determines which mapping strategies to run. By default, the selection
   depends on whether an ID and/or name column is available; you can override
   this interactively unless `--auto-mappers=true` is set.

7. **Run mapping** (`pipeline.mapping.mapping_step`)  
   Applies the selected mappers in sequence. For each geodata CSV, a dedicated
   mapping table is maintained with the columns:

   - `mapped_by` – mapper that produced the mapping
   - `mapped_value` – target geodata ID
   - `mapped_source` – geodata CSV path
   - `mapped_label` – name from geodata
   - `mapped_param` – optional mapper‑specific parameter (e.g. matched variant)  
     Mappers are conservative: they only assign a mapping when the corresponding
     key (ID/name) can be resolved unambiguously. Within a given geodata CSV, an
     ID is never used for more than one input row.

8. **Choose export geodata source** (`pipeline.export_selection.select_export_geodata_step`)  
   Based on how many rows each geodata source has mapped (and how many of its
   own rows are used), the tool either asks you to pick one source, or chooses
   the best one automatically if `--auto-export-source=true`.

9. **Manual mapping** (`pipeline.manual_mapping.manual_mapping_step`)  
   For the selected geodata source, you can manually map remaining unmapped
   input values:

   - Preferred: a curses‑based two‑pane UI showing unmapped inputs on the left
     and unused geodata entries on the right.
   - Fallback: a dialog‑based workflow with `questionary` where you pick one
     input row and then search/select a geodata entry.  
     Manual mappings are written into the same per‑source mapping tables used by
     the automatic mappers.

10. **Export results** (`pipeline.export_results.export_results_step`)  
    Writes several CSV files under `results/<input_name>/<dataset>/<level>/<year>/`:
    - `mapped_pairs.csv` – input IDs/names plus geodata IDs/names and mapper
      info, sorted by mapper priority.
    - `unmapped_orginal.csv` – input IDs/names that could not be mapped by any
      selected geodata source.
    - `unmapped_geodata.csv` – geodata rows that were never used in mappings.  
      If a meta JSON was provided on input, an enriched `meta.json` is written
      alongside the exports, containing the effective column and geodata choices.

---

## Mapper Overview

The mapping from input values to geodata is done by several specialized
“mappers”. Each mapper writes (on success) into the columns described above.
The orchestrator enforces that within one geodata CSV each geodata ID is used
at most once.

- **`id_exact`**  
  Normalizes IDs (uppercase, strips spaces and punctuation) and searches across
  all loaded geodata frames. An ID is mapped when its normalized form resolves
  unambiguously (either only once overall or always to the same geodata ID
  across versions).

- **`unique_name`**  
  Normalizes the source value (lowercase, handle German umlauts, remove digits
  and punctuation) and only maps when the normalized name occurs exactly once
  across all loaded geodata (or always points to the same ID across versions).
  This is very conservative but very precise.

- **`sorted_tokens`** (available but not enabled by default in the main
  pipeline)  
  Normalizes names, splits them into tokens, sorts the tokens alphabetically
  and compares these “token keys”. This still maps values where the word order
  differs, e.g. “Kreisfreie Stadt Köln” vs. “Köln, kreisfreie Stadt”. It only
  maps when the token key uniquely identifies a single ID.

- **`token_permutation`**  
  Splits the source value into tokens, generates all permutations up to a
  limited length, joins tokens without spaces and normalizes these variants.
  It then matches against normalized geodata names without spaces. A mapping is
  created when one of these variants uniquely matches exactly one geodata
  record.

- **`suffix_variants`**  
  Builds variants from the original value by adding typical title words like
  “Stadt”, “Landkreis”, “Kreisfreie Stadt”, “DE” and similar either at the
  beginning or the end. All variants are normalized and compared with geodata;
  a mapping is created if exactly one variant matches a single geodata record.
  Already‑used geodata IDs are excluded so that a remaining row can still be
  mapped to a different variant of the same place.

- **`regex_replace`**  
  Creates variants using predefined regex replacement rules (for example remove
  “Landeshauptstadt”, replace “Stadt” with “Kreisfreie Stadt” or “Stadtkreis”,
  or switch between “in der” and “i. d.”, “an der” and “a. d.”). Each variant
  is normalized; if this leads to exactly one matching geodata record (after
  excluding IDs already used by earlier mappers), that ID is used.

- **`fuzzy_confident`**  
  Performs conservative fuzzy matching using `difflib`. Besides similarity of
  normalized names, it uses simple heuristics about types like “Kreisfreie
  Stadt” vs. “Landkreis” and structural bonuses. A match is only accepted if
  score thresholds and a safety margin to the second‑best candidate are
  satisfied.

---

## Repository Layout

- `main.py` – CLI entry point and pipeline definition.
- `pipeline/`
  - `load_csv/` – input loading (CSV/Excel).
  - `select_column/` – interactive column selection.
  - `normalize/` – normalization of source strings.
  - `geodata_selection/` – selection of geodata level and year.
  - `geodata_loader/` – loading of matching geodata CSVs.
  - `mapping/` – orchestration and implementations of the mapping mappers.
  - `export_selection.py` – choice of export geodata source.
  - `manual_mapping.py` – curses/dialog‑based manual mapping UI.
  - `export_results.py` – CSV/meta export.
  - `storage.py` – central state storage for pipeline selections and statistics.
  - `constants.py` – shared paths, prompts and mapping configuration.
  - `utils/text.py` – text and ID normalization utilities.
- `geodata_raw/` – raw geodata (GeoJSON) used to prepare the cleaned CSV files.
- `geodata_clean/` – cleaned geodata; especially `csv/` is used by the pipeline.
- `results/` – output directory for exports.
- `test_data/` – example input files for experimentation.
