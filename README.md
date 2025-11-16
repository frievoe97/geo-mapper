## Project setup

1. Create a virtual environment

```bash
python3 -m venv .venv
```

2. Activate the environment (macOS/Linux)

```bash
source .venv/bin/activate
```

3. Install dependencies

```bash
pip install -r requirements.txt
```

4. Deactivate the environment (if needed)

```bash
deactivate
```

Optionally, the `Makefile` provides some helpers:

```bash
make install   # create venv + install dependencies
make run       # run the main script
make clean     # remove generated outputs
```

## Mapper overview

The mapping from input values to geodata is done by several specialized mappers.  
Each mapper writes (on success) into the columns `mapped_by`, `mapped_value`, `mapped_source` and `mapped_label`.

- `unique_name`  
  Normalizes the source value (lowercase, handle German umlauts, remove digits and punctuation) and only maps when the normalized name occurs exactly once across all loaded geodata (or always points to the same ID across versions). This is very conservative but very precise.

- `sorted_tokens`  
  Normalizes names, splits them into tokens, sorts the tokens alphabetically and compares these “token keys”. This still maps values where the word order differs, e.g. “Kreisfreie Stadt Köln” vs. “Köln, kreisfreie Stadt”. It only maps when the token key uniquely identifies a single ID.

- `suffix_variants`  
  Builds variants from the original value by adding typical title words like “Stadt”, “Landkreis”, “Kreisfreie Stadt”, “DE” and similar either at the beginning or the end. All variants are normalized and compared with geodata; a mapping is created if exactly one variant matches a single geodata record.

- `regex_replace`  
  Creates variants using predefined regex replacement rules (for example remove “Landeshauptstadt”, replace “Stadt” with “Kreisfreie Stadt” or “Stadtkreis”, or switch between “in der” and “i. d.”, “an der” and “a. d.”). Each variant is normalized; if this leads to exactly one matching geodata record, that ID is used.

- `fuzzy_confident`  
  Performs conservative fuzzy matching using `difflib`. Besides similarity of normalized names, it uses simple heuristics about types like “Kreisfreie Stadt” vs. “Landkreis” and structural bonuses. A match is only accepted if score thresholds and a safety margin to the second-best candidate are satisfied.

- `token_permutation`  
  Splits the source value into tokens, generates all permutations up to a limited length, joins tokens without spaces and normalizes these variants. It then matches against normalized geodata names without spaces. A mapping is created when one of these variants uniquely matches exactly one geodata record.
