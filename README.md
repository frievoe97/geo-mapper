# geo-mapper

Hinweis: `geo-mapper` funktioniert derzeit nur mit Geodaten für Deutschland.

Interaktives Mapping von CSV/Excel-Daten auf amtliche NUTS-/LAU-Geodaten für Deutschland.

---

## Motivation und Ziel

Bei der Auswertung von sozioökonomischen Kennzahlen oder anderen Statistiken mit Raumbezug taucht immer wieder dasselbe Problem auf:

- Namen von Regionen sind unterschiedlich geschrieben (z. B. „Mühldorf am Inn“ vs. „Mühldorf a. Inn“).
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

## Schnellstart: typischer Workflow

1. **Programm starten und Datei laden**  
   Sie starten `geo-mapper` mit der Option `--data` und dem Pfad zu einer CSV- oder Excel-Datei. Das Programm erkennt das Dateiformat, ermittelt bei CSV-Dateien das Trennzeichen und lädt bei Excel-Dateien die vorhandenen Arbeitsblätter, sodass Sie das gewünschte Sheet auswählen können. Leerzeilen werden entfernt, die Tabelle wird intern als `pandas` DataFrame weiterverarbeitet und der Name der Eingabedatei bestimmt den Namen des Ergebnisordners `results_<dateiname>/`.

2. **Spalten für IDs, Namen und Werte wählen**  
   Im nächsten Schritt legen Sie fest, welche Spalten als IDs und welche als Namen dienen sollen. Sie können mehrere ID-Spalten angeben, zum Beispiel NUTS, ARS oder eigene Schlüssel, und zusätzlich eine Namensspalte mit Kreis- oder Gemeindenamen. Wenn Ihre Daten ausschließlich Namen enthalten, können Sie auf eine ID-Spalte verzichten und nur eine Namensspalte nutzen. Wichtig ist, dass zumindest eine ID- oder eine Namensspalte gewählt wird, sonst bricht der Lauf ab, damit keine inhaltlich leeren Zuordnungen entstehen.

3. **Wertespalten für den Export auswählen**  
   Anschließend entscheiden Sie, welche Wertespalten in die Ergebnisse übernommen werden sollen. Hier können Sie beliebig viele Spalten markieren, etwa Bevölkerung, Flächenangaben oder Kennziffern, und bei Bedarf neue Spaltennamen für die Ausgabe vergeben. Diese Werte erscheinen später zusammen mit den verknüpften Geodaten-IDs und Geodaten-Namen in den Exportdateien und lassen sich direkt in weiteren Auswertungen verwenden.

4. **Geodaten-Ebene und Jahr auswählen**  
   Wenn die Struktur der Eingangsdaten feststeht, wählen Sie die Zielgeodaten. Zuerst bestimmen Sie die Ebene, also ob auf NUTS 0, NUTS 1, NUTS 2, NUTS 3 oder LAU gemappt werden soll, es ist aber auch eine Einstellung ohne Einschränkung möglich. Danach wählen Sie einen konkreten Jahrgang, etwa 2016 oder 2021, oder lassen die Jahresauswahl offen. In diesem Fall lädt `geo-mapper` alle passenden Jahrgänge der gewählten Ebene oder sogar alle verfügbaren NUTS- und LAU-Datensätze, wenn weder Ebene noch Jahr eingeschränkt werden.

5. **Automatische Mapper ausführen**  
   Auf Basis dieser Auswahl beginnt das eigentliche Mapping. Für jede Kombination aus Ebene und Jahr werden die passenden Geodaten-CSV-Dateien geladen und die aktivierten Mapper in einer festen Reihenfolge ausgeführt. Die Schritte reichen von strengen ID-Vergleichen bis hin zu Namensvarianten, bei denen Schreibweisen vereinheitlicht, Zusätze entfernt, typische Titelwörter angefügt und Wortreihenfolgen ignoriert werden. Standardmäßig entscheidet `geo-mapper` automatisch, welche Mapper für Ihre Kombination aus ID- und Namensspalten sinnvoll sind, bei Bedarf können Sie diese Auswahl über Optionen wie `--auto-mappers false` selbst steuern.

6. **Beste Geodatenquelle auswählen**  
   Nach Abschluss des automatischen Mappings zeigt das Programm für jede Geodatendatei an, wie viele Eingabezeilen erfolgreich zugeordnet wurden und wie viele der vorhandenen Geodaten-IDs tatsächlich genutzt wurden. Auf dieser Basis wählen Sie eine Geodatenquelle als Grundlage für den Export oder überlassen die Auswahl dem Programm, indem Sie die automatische Entscheidung aktivieren. `geo-mapper` wählt dann die Quelle mit der besten Abdeckung, damit möglichst viele Eingabezeilen abgedeckt sind und die Geodaten effizient genutzt werden.

7. **Manuelles Mapping durchführen**  
   Im nächsten Schritt können Sie verbleibende nicht zugeordnete Zeilen manuell mappen. In der Standardvariante öffnet sich eine zweigeteilte Ansicht im Terminal, in der links die offenen Eingabezeilen und rechts die verfügbaren Geodatenzeilen angezeigt werden. Sie können darin blättern, nach Namen suchen und mit einer Taste Zuordnungen vornehmen oder bei Bedarf den letzten Schritt zurücknehmen. Wenn die Terminaloberfläche nicht zur Verfügung steht, können Sie alternativ eine dialogbasierte Variante mit Textmenüs und Auswahlfragen nutzen, die dieselben Entscheidungen ermöglicht.

8. **Ergebnisdateien schreiben**  
   Zum Schluss schreibt `geo-mapper` alle Ergebnisse in den Ergebnisordner neben Ihrer Eingabedatei. Dort finden Sie eine Datei mit allen gemappten Zeilen und eine Datei mit den nicht gemappten Einträgen, außerdem eine Übersicht der ungenutzten Geodaten und, je nach gewählter Option, exportierte Geodatensätze als CSV oder GeoJSON. Ergänzt wird dies durch eine Meta-Datei, die alle getroffenen Einstellungen zu Spaltenwahl, Geodatenebene, Jahrgang und verwendeten Mappern festhält.

---

## Installation

### 1. Installation über PyPI mit `pip`

Sobald das Paket auf PyPI veröffentlicht ist:

```bash
pip install geo-mapper
```

Voraussetzung:

- Python ab Version 3.10

Nach der Installation können Sie das Programm von überall starten:

```bash
geo-mapper --data path/to/your_file.csv
```

oder

```bash
python -m geo_mapper --data path/to/your_file.csv
```

### 2. Lokale Installation mit `pip`, systemweit nutzbar

Wenn Sie das Repository lokal ausgecheckt haben und das Kommando überall im System verwenden möchten:

```bash
cd /pfad/zum/geo-mapper
pip install .
```

Falls mehrere Python-Versionen installiert sind, kann stattdessen zum Beispiel `python3 -m pip install .` genutzt werden.

Anschließend steht das Kommando `geo-mapper` systemweit zur Verfügung, solange das jeweilige Python-Umfeld aktiv ist.

### 3. Lokale Nutzung in einer virtuellen Umgebung

Wenn Sie `geo-mapper` nur in einem Projektverzeichnis verwenden möchten, ohne es global zu installieren, empfiehlt sich eine virtuelle Umgebung:

```bash
cd /pfad/zum/geo-mapper
python3 -m venv .venv
source .venv/bin/activate  # macOS und Linux
# Windows: .venv\Scripts\Activate.ps1
pip install .
```

Nach der Aktivierung der virtuellen Umgebung können Sie das Programm innerhalb dieses Verzeichnisses verwenden:

```bash
geo-mapper --data path/to/your_file.csv
```

oder

```bash
python -m geo_mapper --data path/to/your_file.csv
```

Optionale Makefile-Hilfen im Projektverzeichnis:

```bash
make install
make run
make clean
```

`make install` legt eine virtuelle Umgebung im Projekt an und installiert `geo-mapper` im Editiermodus, `make run` startet einen kurzen Test über die Kommandozeile und `make clean` entfernt den lokalen Ergebnisordner.

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

  - in den Ergebnisordner kopiert und - als Meta-Konfiguration verwendet (z. B. zur Vorgabe von Spalten, Ebene, Jahr). Details zur Struktur siehe Abschnitt „JSON-Konfiguration“.

- `--auto-mappers true|false` (optional, Standard: `true`)  
  Steuert, ob die Mapper-Auswahl interaktiv erfolgt:

  - `true`: keine Rückfrage, Mapper werden automatisch anhand der vorhandenen ID-/Namensspalten gewählt (empfohlen für wiederkehrende Läufe), - `false`: Sie wählen im Terminal explizit aus, welche Mapper verwendet werden.

- `--auto-export-source true|false` (optional, Standard: `false`)  
  Steuert, wie die Geodatenquelle für den Export ausgewählt wird.

  - `false`: Sie wählen interaktiv die gewünschte Quelle.  
  - `true`: das Programm wählt automatisch die Quelle mit der besten Abdeckung mit möglichst vielen gemappten Zeilen und einem hohen Anteil genutzter Geodatensätze. Die Möglichkeit des manuellen Mappings bleibt bestehen, nur die Auswahl des Datensatzes erfolgt automatisch.

- `--export-geodata no|csv|geojson|both` (optional, Standard: `no`)  
  Legt fest, ob und in welcher Form die ausgewählte Geodatenquelle zusätzlich in den Ergebnisordner exportiert wird.  
  - `no`: kein Geodaten-Export.  
  - `csv`: die zugrunde liegende Geodaten-CSV wird mitgeschrieben.  
  - `geojson`: die passende GeoJSON-Datei wird mitgeschrieben.  
  - `both`: sowohl CSV als auch GeoJSON werden exportiert.

Boolesche Flags (`--auto-mappers`, `--auto-export-source`) akzeptieren typische Angaben wie `true`, `false`, `1`, `0`, `yes`, `no` (ohne Beachtung von Groß-/Kleinschreibung).

---

## Exportierte Dateien

Für eine Eingabedatei `path/to/file.xlsx` wird standardmäßig im selben Ordner ein Unterordner `results_file/` angelegt. Darin finden sich typischerweise:

- `mapped_pairs.csv`  
  Eine Zeile pro erfolgreich gemappter Kombination (pro gewähltem Geodatensatz), mit u. a.:

  - `original_id` – Wert aus der gewählten ID-Spalte (falls vorhanden), - `original_name` – Wert aus der gewählten Namensspalte (falls vorhanden), - `geodata_id` – ID aus den Geodaten (falls relevant), - `geodata_name` – Name aus den Geodaten, - `geodaten_id`, `geodaten_id_nuts`, `geodaten_id_ars` – je nach Datensatzfamilie (NUTS/LAU), - alle ausgewählten Nutzdaten-Spalten aus der Eingabe, - `mapper` – welcher Mapper die Zuordnung erzeugt hat, - `parameter` – mapper-spezifische Zusatzinformation (z. B. genutzter Token-Key oder Variante). Die Zeilen sind so sortiert, dass frühere Mapper (strengere Regeln) oben stehen.

- `unmapped_orginal.csv`  
  Alle Eingabezeilen, die von keinem der berücksichtigten Geodatensätze gemappt werden konnten (nur ID/Name, keine Werte).

- `unmapped_geodata.csv`  
  Alle Geodaten-Zeilen (für die gewählte Geodatenquelle), die in keiner Zuordnung verwendet wurden.

- `meta.yaml`  
  Eine YAML-Datei mit den effektiv verwendeten Einstellungen: Spaltennamen, `geodata_level`, `geodata_year` usw. Wenn beim Aufruf bereits eine YAML-Konfiguration übergeben wurde, werden deren Inhalte hier ergänzt/vereinheitlicht.

- ggf. zusätzlich Geodaten-Dateien (abhängig von `--export-geodata`):
  - eine Kopie der verwendeten Geodaten-CSV, - die passende GeoJSON-Datei mit Geometrien.

---

## Automatische Mapper

Die eigentliche Zuordnung von Eingabewerten zu Geodaten erfolgt über spezialisierte Mapper. Für jede Geodaten-CSV wird eine separate Mapping-Tabelle aufgebaut; innerhalb eines Datensatzes wird jede Geodaten-ID höchstens einmal verwendet (1:1-Beziehung).

Standardmäßig werden (in dieser Reihenfolge) folgende **automatische** Mapper verwendet; das anschließende manuelle Mapping kommt zusätzlich oben drauf:

1. **`exact_id`** – exakter ID-Vergleich  
   - Wird verwendet, wenn mindestens eine ID-Spalte ausgewählt wurde.  
   - Auf Eingabeseite können mehrere ID-Spalten (z. B. `ARS`, `NUTS`, eigene IDs) gleichzeitig genutzt werden; auf Geodatenseite werden alle Spalten berücksichtigt, deren Name mit `id` beginnt (z. B. `id`, `id_nuts`, `id_ars`).  
   - Zellwerte werden robust in Strings umgewandelt (`12345`, `12345.0` → `12345`), NaN/Leereinträge werden ignoriert.  
   - Für jede Zeile werden alle vorhandenen ID-Spalten gegen ein Lookup aller Geodaten-IDs geprüft. Ein Treffer zählt nur, wenn:  
     - für jede ID-Spalte alle gefundenen Kandidaten auf **dieselbe** Geodaten-ID verweisen und  
     - über alle beteiligten ID-Spalten hinweg auch nur eine einzige Geodaten-ID übrig bleibt.  
   - Die Zuordnung ist damit bewusst konservativ: sobald in irgendeiner Spalte mehrere unterschiedliche Kandidaten auftauchen, wird **nicht** gemappt. In `mapped_param` wird festgehalten, über welche Geodaten-ID-Spalte (z. B. `id_ars`) gemappt wurde.

2. **`id_without_leading_zero`** – ID-Vergleich ohne führende Nullen  
   - Arbeitet identisch zu `exact_id`, zieht aber vorher führende Nullen sowohl bei den Eingabe-IDs als auch bei den Geodaten-IDs ab (z. B. `09679` → `9679`, leere Strings bleiben leer).  
   - Damit lassen sich Unterschiede in der Speicherung von Regionalschlüsseln/NUTS-Codes ausgleichen (einmal `08311`, einmal `8311`).  
   - Auch hier gilt: eine Zuordnung erfolgt nur, wenn alle beteiligten ID-Spalten eindeutig auf **eine** Geodaten-ID zeigen; bei Mehrdeutigkeiten bleibt die Zeile ungemappt. Im Export ist `mapped_by = "id_without_leading_zero"`.

3. **`unique_name`** – eindeutig normalisierte Namen  
   - Nutzt eine stark normalisierte Namensform: Kleinschreibung, deutsche Umlaute (`ä` → `ae`, `ö` → `oe`, `ü` → `ue`), Entfernen von Ziffern, Ersetzen von Satzzeichen/Sonderzeichen durch Leerzeichen, Zusammenziehen mehrfacher Leerzeichen.  
   - Für **jeden Geodatensatz (CSV)** wird getrennt gezählt, wie oft ein normalisierter Name vorkommt; ein Eingabe-Eintrag kann dadurch mehreren Jahrgängen/Versionen zugeordnet werden, weil jeder Datensatz separat analysiert wird.  
   - Innerhalb eines einzelnen Geodatensatzes wird nur dann gemappt, wenn der normalisierte Name eindeutig auf **eine** Geodaten-ID zeigt (keine konkurrierenden IDs in diesem Datensatz).  
   - In allen anderen Fällen (Name fehlt in diesem Datensatz vollständig oder ist dort mit unterschiedlichen IDs belegt) wird **nicht** gemappt; solche Fälle werden intern als „no match“ bzw. „ambiguous“ gezählt.  
   - Der Mapper ist damit gut geeignet für „saubere“ Kreis-/Gemeindenamen, bleibt aber bewusst vorsichtig bei Mehrdeutigkeiten innerhalb eines Datensatzes.

4. **`regex_replace`** – Varianten über reguläre Ausdrücke  
   - Erzeugt aus dem Originalnamen systematisch Varianten mithilfe einer Liste von `(Regex, Ersetzung)`-Regeln.  
   - Dazu gehören unter anderem:  
     - Entfernen von Zusätzen wie „Landeshauptstadt“, „Wissenschaftsstadt“, „Hansestadt“,  
     - Ersetzen von Abkürzungen und Formulierungen, zum Beispiel „im“ wird zu „i.“, „in der Oberpfalz“ wird zu „i. d. Opf“, „kreisfr. Stadt“ wird zu „Kreisfreie Stadt“,  
     - Harmonisierung von Suffixen und Kreisbezeichnungen wie „Landkreis“, „Kreisfreie Stadt“, „Regionalverband“,  
     - Korrektur einzelner Tippfehler aus historischen NUTS-Daten, zum Beispiel „Zwickau“ wird zu „Zwichau“.  
   - Für jede erzeugte Variante wird der Name wie beim `unique_name`-Mapper normalisiert und gegen ein Lookup aller Geodaten-Namen geprüft.  
   - IDs, die bereits von früheren Mappern (`exact_id`, `id_without_leading_zero`, `unique_name`) verwendet wurden, sind gesperrt – so kann z. B. erst die kreisfreie Stadt und anschließend der zugehörige Landkreis gemappt werden, ohne dieselbe Geodaten-ID doppelt zu vergeben.  
   - Gemappt wird nur, wenn nach Anwendung aller Regeln **genau eine** noch freie Geodaten-ID übrig bleibt. In `mapped_param` wird die Normalform derjenigen Variante hinterlegt, die zum Treffer geführt hat.

   Die aktuell hinterlegten Regex-Regeln sind:

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

5. **`token_permutation`** – Suffix-Varianten und tokenbasierte Normalisierung  
   - Ausgehend vom Originalnamen werden Varianten gebildet, indem typische Titel- und Suffixwörter aus einer vordefinierten Liste angehängt werden, zum Beispiel „Stadtkreis“, „Landkreis“, „Kreisfreie Stadt“, „DE“, „D“, „Eifelkreis“ und „Kreis“, zusammen mit der Variante ohne Zusatz.  
   - Für jede Variante werden die Buchstaben vereinheitlicht, in Wörter zerlegt und diese Wörter in eine feste Reihenfolge gebracht, sodass unterschiedliche Schreibweisen mit identischen Bestandteilen zu demselben Schlüssel zusammengefasst werden.  
   - Für alle Geodaten-Namen wird derselbe Schlüssel gebildet.  
   - IDs, die bereits von früheren Mappern vergeben wurden, werden ausgeschlossen; eine Zuordnung erfolgt nur, wenn über alle Varianten hinweg genau **eine** verwendbare Geodaten-ID übrig bleibt.  
   - Die Methode ist unempfindlich gegenüber Wortreihenfolge („Stadtkreis Karlsruhe“ vs. „Karlsruhe Stadtkreis“) und gewissen Suffix-Unterschieden. Im Export zeigt `mapped_param`, welcher Token-Key zum Treffer geführt hat.

   Die in diesem Mapper verwendeten Titel- und Suffixwörter sind:

   | Wert             | Beispiel (vorher → nachher)                 |
   |------------------|----------------------------------------------|
   | Stadtkreis       | Stadt Karlsruhe → Stadtkreis Karlsruhe      |
   | Landkreis        | Uckermark → Landkreis Uckermark             |
   | Kreisfreie Stadt | Stadt Augsburg → Kreisfreie Stadt Augsburg  |
   | DE               | Burgenlandkreis → Burgenland (DE)          |
   | D                | Burgenlandkreis → Burgenland (D)           |
   | Eifelkreis       | Bitburg-Prüm → Eifelkreis Bitburg-Prüm      |
   | Kreis            | Rhein-Neuss → Rhein-Kreis Neuss             |

Die Reihenfolge der Mapper ist bewusst konservativ gewählt: exakte ID-Treffer haben Vorrang, danach folgen immer „weichere“ Namens-Mapper. Pro Geodaten-CSV wird jede ID höchstens einmal vergeben; verbleibende Fälle können anschließend im manuellen Mapping gezielt nachbearbeitet werden.

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

   - Auswahl der Ebene: NUTS 0–3 oder LAU oder `unknown` (alle), - Auswahl des Jahrgangs/der Version (konkretes Jahr oder `unknown`), - darauf basierend werden alle passenden CSV-Dateien aus `geo_mapper/geodata_clean/csv` ermittelt. Wird sowohl Ebene als auch Jahr auf `unknown` gesetzt, werden alle verfügbaren NUTS- und LAU-Datensätze geladen; wird nur das Jahr auf `unknown` gesetzt, aber z. B. „NUTS 3“ gewählt, werden alle NUTS-3-Jahrgänge geladen.

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

## YAML-Konfiguration (`--yaml`)

Mit der Option `--yaml` können Sie eine YAML-Datei übergeben, die den Lauf konfiguriert und bei Bedarf wiederverwendet werden kann. Typischer Workflow:

1. Erste Datei interaktiv mappen (ohne `--yaml`).
2. Den entstandenen `meta.yaml`-Export im Ergebnisordner als Konfiguration für
   weitere Dateien gleichen Aufbaus verwenden:

   `bash    geo-mapper --data neue_daten.xlsx --yaml results_alte_daten/meta.yaml    `

Unterstützte Felder in einer YAML-Konfiguration sind u. a.:

- Spalten:

  - `id_column` – einzelne ID-Spalte (Kompatibilitätsfeld), - `id_columns` – Liste von ID-Spalten, - `name_column` – Namensspalte, - `value_columns` – Liste (oder einzelner String) mit Wertespalten.

– Geodaten-Auswahl:
  - `geodata_level` – `"NUTS 3"`, `"NUTS 2"`, `"NUTS 1"`, `"NUTS 0"` oder `"LAU"` (auch Varianten wie `"NUTS_3"` werden erkannt), - `geodata_year` – Jahr als String oder Zahl (z. B. `"2021"`), - alternativ historische Felder `level` und `year`.

Wenn eine gültige Kombination aus Level und Jahr angegeben ist und die entsprechenden Geodaten im Paket vorhanden sind, werden diese Angaben ohne weitere Nachfragen verwendet. Fehlende Felder ergänzt `geo-mapper` beim Export in der erzeugten `meta.yaml`.

---

## Geodaten im Paket und eigene Geodaten vorbereiten

Im Python-Paket werden bereits bereinigte Geodaten ausgeliefert:

- `geo_mapper/geodata_clean/csv`  
  CSV-Dateien mit IDs und Namen, nach Ebene und Jahr strukturiert:

  - `LAU/<jahr>/lau_<jahr>_level_0.csv` - `NUTS_<level>/<jahr>/nuts_<jahr>_level_<level>.csv`

- `geo_mapper/geodata_clean/geojson`  
  Die gleichen Datensätze als GeoJSON mit WGS84-Koordinaten (EPSG:4326).

### Verfügbare NUTS- und LAU-Versionen

Im Paket sind aktuell die folgenden Kombinationen aus Ebene und Jahrgang enthalten:

| Ebene  | Verfügbare Jahrgänge                                       |
|--------|------------------------------------------------------------|
| NUTS 0 | 2003, 2006, 2010, 2013, 2016, 2021, 2024                  |
| NUTS 1 | 2003, 2006, 2010, 2013, 2016, 2021, 2024                  |
| NUTS 2 | 2003, 2006, 2010, 2013, 2016, 2021, 2024                  |
| NUTS 3 | 2003, 2006, 2010, 2013, 2016, 2021, 2024                  |
| LAU    | 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024 |

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

### Datenquellen für NUTS-, LAU- und Regionalschlüssel

Die im Paket enthaltenen NUTS- und LAU-Geodaten basieren auf den offiziellen GISCO-Daten von Eurostat:

- NUTS: https://ec.europa.eu/eurostat/de/web/gisco/geodata/statistical-units/territorial-units-statistics
- LAU: https://ec.europa.eu/eurostat/de/web/gisco/geodata/statistical-units/local-administrative-units

Die dort bereitgestellten GeoJSON-Dateien werden unverändert unter `geo_mapper/geodata_raw` abgelegt und dienen als Ausgangsbasis für die Aufbereitung. Für das Mapping werden insbesondere die IDs und Namen verwendet:

- NUTS: `NUTS_ID`, `NUTS_NAME`
- LAU: `GISCO_ID`, `LAU_NAME`

Für NUTS-Daten ist in den Eurostat-Rohdaten kein deutscher amtlicher Regionalschlüssel (ARS) enthalten. Um diesen dennoch soweit möglich zu ergänzen, werden zusätzliche Quellen genutzt, die NUTS-IDs explizit den Regionalschlüsseln zuordnen. Insbesondere:

- Kreisfreie Städte und Landkreise nach Fläche, Bevölkerung und Bevölkerungsdichte  
  https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/Administrativ/04-kreise.html

- Bruttoinlandsprodukt, Bruttowertschöpfung in den kreisfreien Städten und Landkreisen  
  https://www.statistikportal.de/de/vgrdl/ergebnisse-kreisebene/bruttoinlandsprodukt-bruttowertschoepfung-kreise

Aus diesen Dateien werden die für Deutschland relevanten Zuordnungen NUTS ↔ Regionalschlüssel extrahiert und in `prepare/regionalschluessel.py` bzw. in den Dateien unter `geo_mapper/geodata_raw/ags_nuts` hinterlegt. Besonders bei älteren NUTS-Versionen ist diese Zuordnung nicht vollständig für alle Kreise möglich, deckt aber den Großteil der Regionen ab.

### Beispielhafte Testdatensätze für die Namensnormalisierung

Die im Abschnitt „Automatische Mapper“ beschriebenen regulären Ausdrücke, Namensvarianten und Suffix-Strategien wurden nicht frei erfunden, sondern anhand typischer amtlicher Veröffentlichungen für Kreise und kreisfreie Städte abgeleitet und getestet. Dazu gehören u. a.:

- Kreisfreie Städte und Landkreise nach Fläche, Bevölkerung und Bevölkerungsdichte am 31.12.2024  
  https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/Administrativ/04-kreise.html

- Bruttoinlandsprodukt, Bruttowertschöpfung in den kreisfreien Städten und Landkreisen der Bundesrepublik Deutschland 1992 und 1994 bis 2022 (Reihe 2 Band 1), Berechnungsstand: August 2023  
  https://www.statistikportal.de/de/vgrdl/ergebnisse-kreisebene/bruttoinlandsprodukt-bruttowertschoepfung-kreise

- Kindertagesbetreuung regional 2018 – Ein Vergleich aller Kreise in Deutschland – Anhangtabellen A1 und A2  
  https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Soziales/Kindertagesbetreuung/_inhalt.html#_iw8t5s9wx

- SGB II-Kennzahlen für Kreise und kreisfreie Städte (Kennzahlen und Grunddaten)  
  https://statistik.arbeitsagentur.de/DE/Navigation/Statistiken/Fachstatistiken/Grundsicherung-fuer-Arbeitsuchende-SGBII/Produkte/Kennzahlen-nach-48a/SGB-II-Kennzahlen-Archiv/2010/Juni/Zu-den-Daten-Nav.html

Aus diesen Tabellen wurden insbesondere unterschiedliche Schreibweisen von Kreisnamen, Zusätze (z. B. „Landeshauptstadt“, „kreisfreie Stadt“) sowie typische Abkürzungen gesammelt, um robuste Regex-Regeln und Suffixvarianten zu entwickeln und ihre Wirkung in realistischen Datenlayouts zu testen.
