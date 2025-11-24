# geo-mapper

Interaktives Mapping von CSV/Excel-Daten auf amtliche NUTS-/LAU-Geodaten für Deutschland.

---

## Motivation und Ziel

Bei der Auswertung von sozioökonomischen Kennzahlen oder anderen Statistiken mit Raumbezug taucht immer wieder dasselbe Problem auf:

- Namen von Regionen sind unterschiedlich geschrieben (z. B. „Mühldorf am Inn“ vs. „Mühldorf a. Inn“).
- Offizielle Codes (NUTS, Amtlicher Gemeindeschlüssel/ARS, LAU) fehlen oder sind unvollständig.
- Geodaten liegen in verschiedenen Jahrgängen und Versionen vor.

`geo-mapper` löst dieses Problem für Deutschland, indem es Werte aus einer einzelnen CSV- oder Excel-Datei interaktiv auf offizielle Referenz-Geodaten (NUTS 0–3 und LAU) mapped. Das Tool:

- erkennt und lädt Ihre Eingabedatei,
- lässt Sie ID-, Namens- und Wertespalten auswählen,
- wählt passende NUTS-/LAU-Geodatensätze (Ebene und Jahr),
- wendet mehrere robuste, konservative Matching-Strategien („Mapper“) nacheinander an,
- ermöglicht anschließend ein komfortables manuelles Mapping der verbleibenden Restfälle,
- exportiert alle Ergebnisse in strukturierter Form in einen `results_<input>`-Ordner.

Der Fokus liegt auf Transparenz und Sicherheit: jeder Zuordnung ist im Export anzusehen, welcher Mapper sie erzeugt hat und auf welche Variante er sich gestützt hat.

---

## Funktionsweise in Kürze

- **Eingabe:** eine einzelne Tabelle (`.csv`, `.xlsx`, `.xlsm`, `.xls`) mit mindestens einer ID- oder Namensspalte.
- **Zwischenschritte:** interaktive Auswahl von Spalten, geographischer Ebene (NUTS 0–3 oder LAU) und Jahr/Version,
  anschließend Ausführung mehrerer automatisch arbeitender Mapper.
- **Manuelles Mapping:** verbleibende nicht zugeordnete Zeilen können im Terminal komfortabel
  manuell einer Geodaten-Zeile zugeordnet oder bewusst ungemappt gelassen werden.
- **Ausgabe:** mehrere CSV-Dateien in `results_<dateiname>/` neben Ihrer Eingabedatei mit
  gemappten Werten, nicht gemappten Eingaben, ungenutzten Geodaten sowie optional exportierten Geodaten (CSV/GeoJSON) und einer `meta.json` mit allen gewählten Einstellungen.

Die Referenz-Geodaten werden mit dem Skript `prepare/clean_geojson_data.py` aus Roh-GeoJSONs unter `geo_mapper/geodata_raw` in ein sauberes Layout unter `geo_mapper/geodata_clean` überführt (siehe Abschnitt „Eigene Geodaten vorbereiten“).

---

## Installation

### Über PyPI (empfohlen)

`geo-mapper` ist als Paket auf PyPI veröffentlicht:

```bash
pip install geo-mapper
```

Voraussetzungen:

- Python ≥ 3.10

Nach der Installation können Sie das CLI von überall starten:

```bash
geo-mapper --data path/to/your_file.csv
# alternativ:
python -m geo_mapper --data path/to/your_file.csv
```

Es ist empfehlenswert, mit einer virtuellen Umgebung zu arbeiten:

```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# Windows: .venv\Scripts\Activate.ps1
pip install geo-mapper
```

### Lokale Installation aus dem Quellcode

Wenn Sie das Repository lokal auschecken und am Code arbeiten wollen:

```bash
cd /pfad/zum/geo-mapper
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -e .
```

Optionale Makefile-Hilfen (aus dem Projektverzeichnis):

```bash
make install   # venv anlegen + geo-mapper im Editiermodus installieren
make run       # CLI-Hilfe anzeigen (Schnelltest)
make clean     # ggf. alten lokalen results/-Ordner entfernen
```

Anschließend steht das CLI als `geo-mapper` bzw. `python -m geo_mapper` zur Verfügung.

---

## Schnellstart: typischer Workflow

1. **Programm starten**

   `bash    geo-mapper --data path/to/file.xlsx    # oder    geo-mapper -d path/to/file.csv    `

   Unterstützte Formate:

   - CSV (`.csv`) – Trennzeichen wird automatisch erkannt (`,`, `;`, Tab, `|`). - Excel (`.xlsx`, `.xlsm`, `.xls`).

2. **Arbeitsblatt wählen (nur bei Excel)**  
   Bei Excel-Dateien werden die vorhandenen Sheets angezeigt; Sie wählen eines aus. Bei CSV-Dateien entfällt dieser Schritt.

3. **ID-Spalte(n) wählen**  
   Sie wählen die Spalte(n), in denen IDs stehen (z. B. NUTS, ARS, eigene Schlüssel). Es können mehrere ID-Spalten gewählt werden. Alternativ können Sie explizit „`<Do not use an ID column>`“ wählen, wenn Sie ausschließlich über Namen mappen wollen.

4. **Namensspalte wählen**  
   Sie wählen die Spalte mit den Gebietsnamen (z. B. „Landkreis“, „Gemeinde“). Auch hier kann optional „`<Do not use a name column>`“ gewählt werden.

   Wichtig: mindestens eine ID- oder eine Namensspalte muss ausgewählt sein, sonst wird der Lauf abgebrochen.

5. **Wertespalten wählen (optional)**  
   Sie können beliebig viele Spalten markieren, die als Nutzdaten mit in den Export übernommen werden sollen (z. B. „Bevölkerung“, „Arbeitslosenquote“). Für jede dieser Spalten können Sie bei Bedarf direkt einen neuen Spaltennamen für den Export vergeben.

6. **Geodaten-Ebene auswählen (NUTS/LAU)**  
   Sie entscheiden, auf welcher Verwaltungsebene gemappt werden soll:

   - `NUTS 0 (Deutschland gesamt)` - `NUTS 1 (Bundesländer)` - `NUTS 2 (Regionen / Regierungsbezirke)` - `NUTS 3 (Landkreise / kreisfreie Städte)` - `LAU (Gemeinden)` - `unknown` – keine Einschränkung (alle NUTS- und LAU-Datensätze).

7. **Jahr/Version auswählen**  
   Für die gewählte Ebene werden alle vorhandenen Jahrgänge angezeigt (z. B. 2013, 2016, 2021). Sie können:

   - einen konkreten Jahrgang wählen (dann wird nur dieser Datensatz verwendet), oder - `unknown` wählen: - wenn Ebene gesetzt ist (z. B. NUTS 3), werden alle Jahrgänge dieser Ebene geladen, - wenn auch die Ebene `unknown` ist, werden alle verfügbaren NUTS- und LAU-Datensätze geladen.

8. **Mapper auswählen (optional)**  
   Standardmäßig wählt `geo-mapper` eine sinnvolle Kombination von Mappern abhängig davon, ob ID- und/oder Namensspalten vorhanden sind. Wenn Sie `--auto-mappers false` übergeben, können Sie im Terminal gezielt aktivieren, welche Mapper verwendet werden sollen (siehe Abschnitt „Automatische Mapper“).

9. **Automatisches Mapping**  
   Für alle passenden Geodaten-CSV-Dateien werden die ausgewählten Mapper in einer fest definierten Reihenfolge ausgeführt. Pro Geodaten-Datei entsteht eine eigene Mapping-Tabelle.

10. **Geodaten-Quelle für den Export wählen**  
    Nach dem automatischen Mapping zeigt das Programm an, wie viele Zeilen pro Geodatenquelle erfolgreich gemappt wurden und wie viele Geodatensätze dabei genutzt wurden. Sie wählen einen Datensatz für den Export aus – oder überlassen die Auswahl dem Programm, wenn Sie `--auto-export-source true` gesetzt haben (es wird dann die Quelle mit der besten Abdeckung gewählt).

11. **Manuelles Mapping**  
    Für die gewählte Geodatenquelle können Sie verbleibende nicht gemappte Einträge manuell zuordnen:

    - bevorzugt in einer curses-basierten Zweifenster-Ansicht im Terminal: - linke Seite: nicht gemappte Eingabezeilen, - rechte Seite: noch ungenutzte Geodatenzeilen, - Navigation mit Pfeiltasten bzw. `j`/`k`, Wechsel der Seite mit `TAB`, Suche mit `/`, Zuordnung mit `ENTER`, Rückgängig machen der letzten Zuordnung mit `u`, Beenden mit `q`. - falls curses nicht verfügbar ist, über eine dialogbasierte Variante mit `questionary`.

12. **Export der Ergebnisse**  
    Am Ende werden im Ordner `results_<dateiname>/` neben Ihrer Eingabedatei mehrere CSV-Dateien und optional Geodaten-Dateien geschrieben (siehe Abschnitt „Exportierte Dateien“).

---

## Kommandozeilen-Referenz

Grundaufruf:

```bash
geo-mapper --data DATA_FILE [OPTIONEN]
```

Verfügbare Optionen:

- `-h`, `--help`  
  Zeigt die integrierte Hilfe und beendet das Programm.

- `-d PATH`, `--data PATH` (Pflichtargument)  
  Pfad zur Eingabedatei (CSV oder Excel).

- `-j PATH`, `--json PATH` (optional)  
  Pfad zu einer JSON-Konfigurationsdatei. Diese wird

  - in den Ergebnisordner kopiert und - als Meta-Konfiguration verwendet (z. B. zur Vorgabe von Spalten, Ebene, Jahr). Details zur Struktur siehe Abschnitt „JSON-Konfiguration“.

- `--auto-mappers true|false` (optional, Standard: `true`)  
  Steuert, ob die Mapper-Auswahl interaktiv erfolgt:

  - `true`: keine Rückfrage, Mapper werden automatisch anhand der vorhandenen ID-/Namensspalten gewählt (empfohlen für wiederkehrende Läufe), - `false`: Sie wählen im Terminal explizit aus, welche Mapper verwendet werden.

- `--auto-export-source true|false` (optional, Standard: `false`)  
  Steuert, wie die Geodatenquelle für den Export ausgewählt wird:

  - `false`: Sie wählen interaktiv die gewünschte Quelle, - `true`: es wird automatisch die Quelle mit der besten Abdeckung (meiste gemappte Zeilen, hoher Anteil genutzter Geodatensätze) verwendet. Die Möglichkeit des manuellen Mappings bleibt bestehen; nur die Auswahl des Datensatzes selbst erfolgt automatisch.

- `--export-geodata no|csv|geojson|both` (optional, Standard: `no`)  
  Legt fest, ob und in welcher Form die ausgewählte Geodatenquelle zusätzlich in den Ergebnisordner exportiert wird: - `no` – kein Geodaten-Export, - `csv` – die zugrunde liegende Geodaten-CSV wird mitgeschrieben, - `geojson` – die passende GeoJSON-Datei wird mitgeschrieben, - `both` – sowohl CSV als auch GeoJSON werden exportiert.

Boolesche Flags (`--auto-mappers`, `--auto-export-source`) akzeptieren typische Angaben wie `true`, `false`, `1`, `0`, `yes`, `no` (ohne Beachtung von Groß-/Kleinschreibung).

---

## Exportierte Dateien

Für eine Eingabedatei `path/to/file.xlsx` wird standardmäßig im selben Ordner ein Unterordner `results_file/` angelegt. Darin finden sich typischerweise:

- `mapped_pairs.csv`  
  Eine Zeile pro erfolgreich gemappter Kombination (pro gewähltem Geodatensatz), mit u. a.:

  - `original_id` – Wert aus der gewählten ID-Spalte (falls vorhanden), - `original_name` – Wert aus der gewählten Namensspalte (falls vorhanden), - `geodata_id` – ID aus den Geodaten (falls relevant), - `geodata_name` – Name aus den Geodaten, - `geodaten_id`, `geodaten_id_nuts`, `geodaten_id_ars` – je nach Datensatzfamilie (NUTS/LAU), - alle ausgewählten Nutzdaten-Spalten aus der Eingabe, - `mapper` – welcher Mapper die Zuordnung erzeugt hat, - `parameter` – mapper-spezifische Zusatzinformation (z. B. genutzter Token-Key oder Variante). Die Zeilen sind so sortiert, dass frühere Mapper (strengere Regeln) oben stehen.

- `unmapped_orginal.csv`  
  Alle Eingabezeilen, die von keinem der berücksichtigten Geodatensätze gemappt werden konnten (nur ID/Name, keine Werte).

- `unmapped_geodata.csv`  
  Alle Geodaten-Zeilen (für die gewählte Geodatenquelle), die in keiner Zuordnung verwendet wurden.

- `meta.json`  
  Eine JSON-Datei mit den effektiv verwendeten Einstellungen: Spaltennamen, `geodata_level`, `geodata_year` usw. Wenn beim Aufruf bereits eine JSON- Konfiguration übergeben wurde, werden deren Inhalte hier ergänzt/vereinheitlicht.

- ggf. zusätzlich Geodaten-Dateien (abhängig von `--export-geodata`):
  - eine Kopie der verwendeten Geodaten-CSV, - die passende GeoJSON-Datei mit Geometrien.

---

## Automatische Mapper

Die eigentliche Zuordnung von Eingabewerten zu Geodaten erfolgt über spezialisierte Mapper. Für jede Geodaten-CSV wird eine separate Mapping-Tabelle aufgebaut; innerhalb eines Datensatzes wird jede Geodaten-ID höchstens einmal verwendet (1:1-Beziehung).

Standardmäßig werden (in dieser Reihenfolge) folgende Mapper verwendet:

1. **`exact_id`**  
   Wird verwendet, wenn mindestens eine ID-Spalte ausgewählt wurde. Es werden ausschließlich die ID-Spalten betrachtet (ohne zusätzliche Normalisierung). Auf Geodatenseite werden alle Spalten berücksichtigt, deren Name mit `id` beginnt (z. B. `id`, `id_nuts`, `id_ars`). Für jede Eingabezeile wird nur dann gemappt, wenn alle gefundenen ID-Treffer eindeutig auf eine einzige Geodaten-ID zeigen.

2. **`id_without_leading_zero`**  
   Entspricht `exact_id`, entfernt aber vor dem Vergleich führende Nullen sowohl auf Eingabe- als auch auf Geodatenseite (z. B. `09679` → `9679`). So lassen sich IDs matchen, die in einem Datensatz mit führender Null, im anderen ohne gespeichert sind.

3. **`unique_name`**  
   Nutzt eine normalisierte Namensspalte (kleingeschrieben, deutsche Umlaute behandelt, Ziffern und Satzzeichen entfernt). Diese Normalisierung wird über alle geladenen Geodaten hinweg verwendet. Eine Zuordnung erfolgt nur, wenn der normalisierte Name insgesamt genau einmal vorkommt oder in allen Jahrgängen auf dieselbe ID verweist. Mehrdeutigkeiten werden bewusst nicht gemappt.

4. **`regex_replace`**  
   Erzeugt Varianten des Originalnamens mithilfe vordefinierter regulärer Ausdrücke, z. B.:

   - Entfernen von Zusätzen wie „Landeshauptstadt“, „Wissenschaftsstadt“,
   - Ersetzen von Abkürzungen („i. d.“ / „i. d. Opf“, „a. d.“ / „an der“),
   - Harmonisierung von Schreibweisen („Burgenlandkreis“ → „Burgenland (D)“, „Salzlandkreis“ → „Salzland“).

   Jede Variante wird normalisiert, und es wird nur gemappt, wenn die daraus resultierende Normalform eindeutig auf eine Geodaten-ID zeigt. IDs, die bereits von früheren Mappern verwendet wurden, werden dabei ausgeschlossen.

   Die wichtigsten aktuell hinterlegten Regex-Regeln sind:

   | Regex-Regel                     | Ersetzung          | Beispiel (vorher → nachher)                                            |
   | ------------------------------- | ------------------ | ---------------------------------------------------------------------- |
   | `\ba\.\s*`                      | `am `              | a. Bodensee → am Bodensee                                              |
   | `\bam\b`                        | `a.`               | am Main → a. Main                                                      |
   | `\ban der\b`                    | `a.d.`             | Neustadt an der Weinstraße → Neustadt a.d. Weinstraße                  |
   | `\bBL\b`                        | ``                 | Musterstadt BL → Musterstadt                                           |
   | `\bdocumenta-Stadt\b`           | ``                 | documenta-Stadt Kassel → Kassel                                        |
   | `\bEifelkreis\b`                | ``                 | Eifelkreis Bitburg-Prüm → Bitburg-Prüm                                 |
   | `\bHansestadt\b`                | `Kreisfreie Stadt` | Hansestadt Lübeck → Kreisfreie Stadt Lübeck                            |
   | `\bHansestadt\b`                | ``                 | Freie und Hansestadt Hamburg → Freie und  Hamburg                      |
   | `\bim\b`                        | `i.`               | im Taunus → i. Taunus                                                  |
   | `\bin der Oberpfalz\b`          | `i. d. Opf`        | Amberg in der Oberpfalz → Amberg i. d. Opf                             |
   | `\bkreisfreie Stadt\b`          | ``                 | Kreisfreie Stadt Köln → Köln                                           |
   | `\bkreisfreie Stadt\b`          | `Stadtkreis`       | Kreisfreie Stadt Augsburg → Stadtkreis Augsburg                        |
   | `\bKreis\b`                     | ``                 | Rhein-Kreis Neuss → Rhein-Neuss                                        |
   | `(?<=\s)Kreis\b`                | ``                 | Mettmann Kreis → Mettmann                                              |
   | `\bLandeshauptstadt\b`          | ``                 | Landeshauptstadt Dresden → Dresden                                     |
   | `\bLandeshauptstadt\b`          | `Stadtkreis`       | Landeshauptstadt Stuttgart → Stadtkreis Stuttgart                      |
   | `\bLandeshauptstadt\b`          | `Kreisfreie Stadt` | Landeshauptstadt Mainz → Kreisfreie Stadt Mainz                        |
   | `\bLandkreis\b`                 | ``                 | Landkreis Leipzig → Leipzig                                            |
   | `\bLandkreis\b`                 | `(DE)`             | Landkreis Uckermark → Uckermark (DE)                                   |
   | `\bSt\.`                        | `Kreisfreie Stadt` | St. Ingbert → Kreisfreie Stadt Ingbert                                 |
   | `\bStadt\b`                     | `Kreisfreie Stadt` | Stadt Karlsruhe → Kreisfreie Stadt Karlsruhe                           |
   | `\bStadt\b`                     | `Stadtkreis`       | Stadt Heidelberg → Stadtkreis Heidelberg                               |
   | `\bStadt\b`                     | ``                 | Stadt Dessau-Roßlau → Dessau-Roßlau                                    |
   | `\bUniversitätsstadt\b`         | `Stadtkreis`       | Universitätsstadt Tübingen → Stadtkreis Tübingen                       |
   | `\bUniversitätsstadt\b`         | ``                 | Universitätsstadt Göttingen → Göttingen                                |
   | `\bWissenschaftsstadt\b`        | ``                 | Wissenschaftsstadt Darmstadt → Darmstadt                               |
   | `\bWissenschaftsstadt\b`        | `Kreisfreie Stadt` | Wissenschaftsstadt Darmstadt → Kreisfreie Stadt Darmstadt              |
   | `\bdocumenta-Stadt\b`           | `Kreisfreie Stadt` | documenta-Stadt Kassel → Kreisfreie Stadt Kassel                       |
   | `\bkr\.f\. St\.`                | `Kreisfreie Stadt` | Frankenthal (Pfalz), kr.f. St. → Frankenthal (Pfalz), Kreisfreie Stadt |
   | `\bkreisfr\.\s*Stadt\b`         | `Kreisfreie Stadt` | kreisfr. Stadt Cottbus → Kreisfreie Stadt Cottbus                      |
   | `\bRegionalverband\b`           | `Stadtverband`     | Regionalverband Saarbrücken → Stadtverband Saarbrücken                 |
   | `\bSalzlandkreis\b`             | `Salzland`         | Salzlandkreis → Salzland                                               |
   | `\bBurgenlandkreis\b`           | `Burgenland (D)`   | Burgenlandkreis → Burgenland (D)                                       |
   | `\bSächs\.`                     | `Sächsische`       | Sächs. Schweiz-Osterzgebirge → Sächsische Schweiz-Osterzgebirge        |
   | `\bZwickau\b`                   | `Zwichau`          | Landkreis Zwickau → Landkreis Zwichau                                  |
   | `\bStadt der FernUniversität\b` | `Kreisfreie Stadt` | Stadt der FernUniversität Hagen → Kreisfreie Stadt Hagen               |
   | `\bKlingenstadt\b`              | `Kreisfreie Stadt` | Klingenstadt Solingen → Kreisfreie Stadt Solingen                      |
   | `\bFreie und Hansestadt\b`      | ``                 | Freie und Hansestadt Hamburg → Hamburg                                 |

5. **`token_permutation`**  
   Kombinierter Mapper für Suffix-Varianten und tokenbasierte Normalisierung: - ausgehend vom Originalnamen werden Varianten gebildet, indem typische Titelwörter wie „Landkreis“, „Kreisfreie Stadt“, „DE“ usw. angehängt werden (einschließlich der Variante ohne Zusatz), - alle Varianten werden normalisiert, in Tokens zerlegt, alphabetisch sortiert und zu einem Schlüssel wieder zusammengesetzt, - für alle Geodaten-Namen wird derselbe sortierte Token-Key gebildet, - eine Zuordnung erfolgt nur, wenn über alle Varianten hinweg genau eine verwendbare Geodaten-ID übrig bleibt (unter Ausschluss bereits verwendeter IDs).

Darüber hinaus existiert ein weiterer Mapper **`sorted_tokens`**, der ähnlich wie `token_permutation` arbeitet, aber ohne Suffix-Varianten. Dieser ist für eigene Pipelines nutzbar, wird jedoch nicht standardmäßig im CLI ausgeführt.

Die Reihenfolge der Mapper ist bewusst konservativ gewählt: exakte ID-Treffer haben Vorrang, danach folgen immer „weichere“ Namens-Mapper.

---

## Verarbeitungs-Pipeline im Detail

Die CLI (`geo_mapper/cli.py`) orchestriert folgende Schritte, die nacheinander aufgerufen werden:

1. **Eingabedatei laden**

   - CSV: Trennzeichen-Erkennung und Einlesen mit `pandas`. - Excel: interaktive Auswahl des Arbeitsblatts. - Vollständig leere Zeilen werden verworfen. - Der Basispfad für den Ergebnisordner `results_<dateiname>/` wird aus dem Speicherort der Eingabedatei abgeleitet.

2. **Spaltenauswahl (ID/Name/Werte)**

   - Anzeige der vorhandenen Spalten inkl. Beispielwerten, - Auswahl einer oder mehrerer ID-Spalten (optional), - Auswahl einer Namensspalte (optional), - Auswahl beliebiger Wertespalten für den Export (optional) mit der Möglichkeit, diese direkt umzubenennen. Mindestens eine ID- oder eine Namensspalte muss gewählt werden. Alternativ können diese Angaben vollständig aus einer JSON-Konfiguration stammen.

3. **Normalisierung der Quellnamen**  
   Auf Basis der gewählten Namensspalte wird eine zusätzliche Spalte `normalized_source` erzeugt, in der alle Namen nach klar definierten Regeln normalisiert werden. Diese Spalte wird von den Namens-Mappern wiederverwendet.

4. **Auswahl der Geodaten (Ebene & Jahr)**

   - Auswahl der Ebene: NUTS 0–3 oder LAU oder `unknown` (alle), - Auswahl des Jahrgangs/der Version (konkretes Jahr oder `unknown`), - darauf basierend werden alle passenden CSV-Dateien aus `geo_mapper/geodata_clean/csv` ermittelt. Wird sowohl Ebene als auch Jahr auf `unknown` gesetzt, werden alle verfügbaren NUTS- und LAU-Datensätze geladen; wird nur das Jahr auf `unknown` gesetzt, aber z. B. „NUTS 3“ gewählt, werden alle NUTS-3-Jahrgänge geladen.

5. **Geodaten laden**  
   Alle passenden CSV-Dateien werden als `pandas`-DataFrames geladen; für NUTS- Datensätze wird zusätzlich eine einheitliche `id`-Spalte erzeugt, die je nach Verfügbarkeit auf `id_nuts` oder `id_ars` basiert.

6. **Mapper-Auswahl**  
   Je nach vorhandenen ID-/Namensspalten schlägt das Programm eine Standard- Kombination von Mappern vor. Bei deaktiviertem `--auto-mappers` können Sie diese Auswahl interaktiv anpassen.

7. **Ausführung der Mapper**  
   Für jede Geodaten-CSV und jeden ausgewählten Mapper wird eine Mapping-Tabelle aufgebaut. Pro CSV wird festgehalten:

   - welche Eingabezeilen gemappt wurden, - welche Geodaten-IDs verwendet wurden, - wie hoch die Abdeckung ist (Anteil der gemappten Eingabezeilen und genutzten Geodatensätze). Innerhalb einer Geodaten-CSV wird jede ID höchstens einmal verwendet.

8. **Auswahl der Export-Geodatenquelle**  
   Basierend auf den obigen Statistiken können Sie einen Datensatz zur weiteren Verwendung (Export, manuelles Mapping) auswählen oder diese Auswahl mit `--auto-export-source true` automatisieren.

9. **Manuelles Mapping**  
   Für die gewählte Geodatenquelle werden alle noch ungemappten Eingabezeilen dargestellt. Sie können diese Zeilen interaktiv Geodaten-Einträgen zuordnen; diese manuellen Zuordnungen werden in dieselbe Mapping-Struktur geschrieben wie die automatischen Mapper.

10. **Export der Ergebnisse**  
    Zum Schluss werden alle Ergebnisdateien (siehe Abschnitt „Exportierte Dateien“) geschrieben und optional die verwendeten Geodaten (CSV/GeoJSON) kopiert.

---

## JSON-Konfiguration (`--json`)

Mit der Option `--json` können Sie eine JSON-Datei übergeben, die den Lauf konfiguriert und bei Bedarf wiederverwendet werden kann. Typischer Workflow:

1. Erste Datei interaktiv mappen (ohne `--json`).
2. Den entstandenen `meta.json`-Export im Ergebnisordner als Konfiguration für
   weitere Dateien gleichen Aufbaus verwenden:

   `bash    geo-mapper --data neue_daten.xlsx --json results_alte_daten/meta.json    `

Unterstützte Felder in einer JSON-Konfiguration sind u. a.:

- Spalten:

  - `id_column` – einzelne ID-Spalte (Kompatibilitätsfeld), - `id_columns` – Liste von ID-Spalten, - `name_column` – Namensspalte, - `value_columns` – Liste (oder einzelner String) mit Wertespalten.

- Geodaten-Auswahl:
  - `geodata_level` – `"NUTS 3"`, `"NUTS 2"`, `"NUTS 1"`, `"NUTS 0"` oder `"LAU"` (auch Varianten wie `"NUTS_3"` werden erkannt), - `geodata_year` – Jahr als String oder Zahl (z. B. `"2021"`), - alternativ historische Felder `level` und `year`.

Wenn eine gültige Kombination aus Level und Jahr angegeben ist und die entsprechenden Geodaten im Paket vorhanden sind, werden diese Angaben ohne weitere Nachfragen verwendet. Fehlende Felder ergänzt `geo-mapper` beim Export in der erzeugten `meta.json`.

---

## Geodaten im Paket und eigene Geodaten vorbereiten

Im Python-Paket werden bereits bereinigte Geodaten ausgeliefert:

- `geo_mapper/geodata_clean/csv`  
  CSV-Dateien mit IDs und Namen, nach Ebene und Jahr strukturiert:

  - `LAU/<jahr>/lau_<jahr>_level_0.csv` - `NUTS_<level>/<jahr>/nuts_<jahr>_level_<level>.csv`

- `geo_mapper/geodata_clean/geojson`  
  Die gleichen Datensätze als GeoJSON mit WGS84-Koordinaten (EPSG:4326).

Diese Dateien werden aus Roh-GeoJSONs unter `geo_mapper/geodata_raw` erzeugt. Das Skript `prepare/clean_geojson_data.py`:

- liest LAU- und NUTS-GeoJSONs ein (erwartet GISCO-kompatible Struktur,
  Koordinaten in EPSG:3035),
- filtert auf Deutschland (`CNTR_CODE == "DE"`),
- extrahiert IDs und Namen (`GISCO_ID`/`LAU_NAME` für LAU,
  `NUTS_ID`/`NUTS_NAME` für NUTS),
- reprojiziert die Geometrien nach WGS84,
- schreibt pro Ebene/Jahr GeoJSON- und CSV-Dateien nach `geodata_clean`,
- ergänzt für NUTS-Daten optional den amtlichen Regionalschlüssel (`id_ars`)
  anhand der Excel-Quellen in `geo_mapper/geodata_raw/ags_nuts` (siehe `prepare/regionalschluessel.py`).

### Eigene bzw. aktualisierte Geodaten einspielen

Fortgeschrittene Nutzer können eigene oder aktualisierte Geodaten verwenden:

1. Rohdaten ablegen

   - LAU-GeoJSONs in `geo_mapper/geodata_raw/lau/`, - NUTS-GeoJSONs in `geo_mapper/geodata_raw/nuts/`. Die Dateinamen sollten das Jahr (vierstellige Jahreszahl) enthalten, damit das Skript den Jahrgang erkennen kann.

2. Geodaten bereinigen und neu erzeugen  
   Im Projektverzeichnis (idealerweise in einer virtuellen Umgebung) ausführen:

   `bash    python prepare/clean_geojson_data.py    `

   Die bereinigten Daten werden dann in `geo_mapper/geodata_clean/csv` und `geo_mapper/geodata_clean/geojson` neu erzeugt und stehen dem CLI sofort zur Verfügung.

3. (Optional) Regionalschlüssel-Quellen anpassen  
   Falls Sie eigene Zuordnungen von NUTS-IDs zu Regionalschlüsseln verwenden möchten, passen Sie die Excel-Quellen und Pfade in `prepare/regionalschluessel.py` sowie die entsprechenden Dateien unter `geo_mapper/geodata_raw/ags_nuts` an und führen anschließend erneut `clean_geojson_data.py` aus.

---

## Hinweise und Einschränkungen

- Der Fokus liegt ausschließlich auf Deutschland; Geodaten anderer Länder
  werden verworfen.
- Pro Lauf wird genau eine Eingabedatei verarbeitet; für mehrere Dateien mit
  gleichem Layout empfiehlt sich die Verwendung einer gemeinsamen `meta.json`.
- Die interaktiven Schritte setzen ein terminalbasiertes Umfeld voraus
  (`questionary`/curses); ein vollständig nicht-interaktiver Batchbetrieb ist derzeit nicht vorgesehen (abgesehen von der Konfiguration über JSON).

Für typische Anwendungsfälle – Harmonisierung von Kreis- und Gemeindedaten aus Statistik-Excel-Tabellen auf amtliche NUTS-/LAU-Geodaten – bietet `geo-mapper` damit einen transparenten, wiederholbaren und gut nachvollziehbaren Workflow.
