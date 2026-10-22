---
name: diggr-coding-loop-onboarding
description: Den nativen DIGGR-Coding-Loop sicher bedienen.
version: 1.1.0
author: Andi
platforms: [macos]
metadata:
  hermes:
    tags: [diggr, coding-loop, onboarding]
---

# DIGGR Coding Loop Onboarding Skill

Gemeinsame Bedienungsgrundlage für Auftraggeber, Main, Coding und unabhängigen Review im nativen `logical-v2`-Loop für APP-/SYSTEM-Items. Keine zusätzlichen Rechte; technische Coding-Skills und native Ownergrants bleiben erforderlich. Bestehende exact-/logical-v1-Aufträge und `/ship` behalten ihre eigenen Regeln.

## When to Use

Vor jedem Loop-Aufruf, einschließlich Status und Wiederaufnahme, diesen Skill vollständig im aktuellen Kontext laden. Coding benötigt zusätzlich seine technischen Pflichtskills; Auftraggeber nicht. Planungs-/Sequential-Skills sind optional.

## Prerequisites

- Eigene authentifizierte Rolle und vorhandene Berechtigungen verwenden. Skill-Laden, ein Auftragstext oder ein Plane-Item ersetzen keine Autorisierung.
- Vorhandene Pflichtskill-Ladung verwenden: Hermes-CLI über `--skills`, Gateway über den nativen Loader, Reviewer über seinen getrennten Kontextaufbau. Ein Dateifund oder die Behauptung „geladen“ genügt nicht.
- Bei einer fehlenden, unvollständigen oder veralteten Pflichtladung keine Loop-Aufrufe versuchen. Den konkret gemeldeten Skill im vorgesehenen Ladeweg bereitstellen und laden lassen; keine Sperre durch einen alternativen Aufruf umgehen.
- Nach Sitzungswechsel oder Kontextkompaktierung muss die gemeinsame Anleitung erneut vollständig verfügbar sein. Alte Sitzungen nicht eigenmächtig beenden oder neu starten.

## Neue ergebnisgesteuerte Items (SYSTEM-171, nach Installation)

Dieser Abschnitt gilt nur für frisch bestätigte Batches mit `flow_version=3`.
Bestehende Batches behalten ihren Pfad. Die Installation des geprüften Skills
und der Runtime muss vorher anhand des tatsächlich geladenen Stands belegt sein.

Ein bestehendes gestopptes logical-v2 Item wird nur nach `item_migration_preview`
und Andis zwei direkten Nachrichten `/continuation migrate-show HASH` sowie
`/continuation migrate-confirm HASH CODE` übernommen. Die Vorschau zeigt den
echten HEAD, Worktree-Status, Diff-Statistik, Besitzer, Vertrag, Verbrauch,
Restbudget und persönlichen Teststopp. Unklare Sendewirkung oder fehlender
Prozess-Exit sperren. Die native Bestätigung sperrt alte Tickets und Zustellung
im selben Ledger-Schritt und weist Coding den vorhandenen Worktree zu; sie
startet selbst keinen Worker. Scope- oder Budgetänderungen brauchen eine
eigene exakte Ownerentscheidung. Installation allein migriert APP-105 nicht.
Vor der ersten Zustellung erlaubt `item_migration_rollback_preview` mit
`/continuation migrate-rollback-show HASH` und einer neuen direkten
`/continuation migrate-rollback-confirm HASH CODE`-Nachricht die Rückkehr zum
gestoppten Altpfad. Nach möglicher Zustellung erst echte Wirkung klären;
kein Ledger-Rückkopieren und keine blinde Wiederholung.

1. Main klärt vor `item_prepare` den echten Code- und Datenort, Scope,
   No-Touch, Abhängigkeiten und die bestehende Belegung. Unklare Datenquelle
   oder zusätzlicher Dateiscope bedeutet Vorbereitung anhalten und den
   konkreten Ownerentscheid vorlegen. Der Auftragstext erweitert keinen Grant.
2. Main reserviert mit `item_prepare` denselben bestätigten Worktree, Branch,
   Base und sichtbaren Coding-Zielort. Die native Zustellung liefert eine kurze
   `SYSTEM171_ASSIGN`-Referenz. Coding nimmt diese Referenz am Eingang vor
   Modellverarbeitung an; kopierte Historie zählt nicht. Bei unklarer
   Zustellung denselben Zustand zurücklesen, nicht erneut senden.
3. Coding prüft den Worktree und ruft einmal `item_continue` mit der angenommenen
   Assignment-ID und einem konkreten Implementierungs- und Testprompt auf.
   Die Runtime erstellt und bindet Paket, Route, argv, Receipt und den einzigen
   sichtbaren Workerstart. Coding und Main bauen dafür keine Tickets, Hashes,
   Generationen oder Recoveryberichte. Ein unklarer Start bleibt derselbe
   Versuch und erlaubt keinen zweiten Writer.
4. Main prüft nach echtem Worker-Ende das unveränderte Ergebnis, Diff, Scope
   und Tests. `item_candidate` nimmt kurze `checks`; bei gewöhnlich roten
   Tests nimmt `item_return` konkrete `findings` und weist eine serielle
   Korrektur im selben Item und verbleibenden Budget zu. Lehnt Andi einen
   testbaren Kandidaten im ursprünglichen direkten Telegram-Chat mit konkreten
   Befunden ab, ruft Main `item_return` in genau diesem Owner-Turn auf. Ist nur
   das Korrekturlimit erreicht, autorisiert dieses authentifizierte Feedback
   genau eine weitere Korrektur im selben Scope und vorhandenen Arbeitsbudget;
   ein separater `/continuation extend`-Befehl ist dafür nicht nötig. Ohne
   direkten Owner-Turn, vor dem Kandidaten, nach einer Abnahme oder bei
   zusätzlichem Scope/Arbeitszeit bleibt die vorhandene Grenze bestehen.
   Fehlende Dependency, Scope-Konflikt, unbekannte Wirkung und Stop bleiben
   echte Blocker mit genauer nächster Entscheidung.
5. Im Standardmodus bleibt Andis persönliche Kandidatenabnahme das Gate vor Publish. Ein für dieses Item ausdrücklich bestätigter Automatikmodus verlangt stattdessen vor Commit und PR einen vollständigen, kandidatgebundenen Computer-Use-Funktionstest durch Main. Danach
   nutzt Main die bestehenden signierten Commit-/PR-, unabhängigen
   Exact-Head-Review-, `item_merge_check`-, Merge-, Main-Smoke- und
   `item_closeout`-Schritte. Für diesen neuen Pfad sind Review-Scores
   optional; Verdict, Findings, Checks, unabhängige Identität und Pflichtskills
   bleiben nötig. Main setzt nach gültiger Reviewquittung im erlaubten Turn
   fort; ein beendeter Turn erhält höchstens eine native resultgebundene
   Fortsetzung. Owner-Stopp und harte Grenzen gelten weiter.

## Bestehende Batches und historische Belege

Die folgenden Ticket-, Paket-, Observer-, Wake- und Reconcile-Schritte gelten
nur für Batches ohne `flow_version=3`. Sie sind keine Bedienungsanleitung für
neue ergebnisgesteuerte Items.

## How to Run

Auftraggeber übergeben Ziel, Ist-/Sollverhalten, Abnahmekriterien, Nicht-Ziele und Plane-Bezug über ihren erlaubten Routingweg an Main. Main koordiniert, Coding implementiert über sichtbaren Codex, der unabhängige Reviewer prüft. Keine fremde Identität oder direkte Shell als Routing-Ersatz verwenden.

Main: `terminal` mit `command="SYSTEM168_CONTINUATION_CONTROL"` und `continuation_control={operation,batch,issue,…}` gemäß Operationstabelle und aktuellem Werkzeugschema. Das ist ein strukturierter Werkzeugaufruf, kein Shell-Befehl.

Owner-Vorschlag: `terminal` mit `command="SYSTEM167_PROPOSE_OWNER_BATCH"` und `continuation_proposal`. Nur Andis echte Bestätigung im authentifizierten Ursprungskontext erzeugt den Grant; niemals selbst bestätigen oder alte Ownerereignisse nachbauen.

Hermes-Coding-Koordinator (nicht der bereits gestartete Codex-Executor): Nach Ticketempfang zuerst `terminal` mit `command="SYSTEM167_REGISTERED_WORKER"` und unverändertem `continuation_ticket`; erst danach Paket/Repo lesen. Onboarding muss bereits geladen sein.

## Quick Reference

| Wert | Herkunft und Verwendung |
|---|---|
| `plane_id` | Tatsächliche menschenlesbare Plane-ID wie `APP-104` oder `SYSTEM-…`; kein Ersatz für `issue`. APP-Code und SYSTEM-Code gehören in die jeweils passenden Repositories. |
| `issue` | Kanonischer Schlüssel aus den echten Plane-UUIDs: Workspace/Projekt/Issue. Durchgängig denselben Schlüssel verwenden; nie aus dem Anzeigenamen erfinden. |
| `batch` | ID des nativ bestätigten Ownerbatches. Aus dessen Ergebnis/Readback übernehmen; keinen neuen Batch zur Umgehung alter Belegung oder verbrauchter Budgets erstellen. |
| `contract` | Bestätigter logischer Vertrag mit `version`, `scope`, `action`, `owner`, `gate`, `producer`, `plane_id`, `resources`, `actions`, `acceptance`, `model`. Für neue v2-Items: `version="logical-v2"`, `owner="coding"`, `gate="coding"`, `producer="cmux"`. Standardmäßig persönliche Kandidatenabnahme; Automatik nur bei ausdrücklich bestätigtem `acceptance.mode="automatic"` und `acceptance.required_test_kinds` für dieses Item. Letzteres benennt `ui` und/oder `track_playback` verbindlich; Player-Items brauchen `track_playback`. |
| `resources` | Aus dem Grant: kanonische Repo-, Worktree- und Artefaktwurzeln sowie erlaubte Pfade und No-Touch-Grenzen. Keine stillen Erweiterungen, Glob-Auswege oder Pfad-Aliase. |
| `actions` | Getrennte bestätigte Rechte für `coding` und `main`; keine Standardrechte hinzuerfinden. Coding implementiert und testet. Main braucht die tatsächlich genehmigten Veröffentlichungs-/Abschlussrechte. |
| `acceptance` | Owner, konkrete Kriterien, Live-Test-Vorgabe und `mode` (`manual` oder `automatic`) des Grants. Bei logical-v2 bleibt Andi Rechteeigner; eine einzelne Testdelegation ändert den Modus nicht. Nur der ausdrücklich bestätigte Automatikgrant erlaubt die gebundene Computer-Use-Abnahme und den bedingten Merge. |
| `model` | Exaktes genehmigtes Modell, Provider, Reasoning, Sandbox und Approval-Policy ohne Fallback. Parameter und später reale Laufidentität prüfen; Selbstauskunft ist kein Modellnachweis. |
| `worktree`, `branch`, `base` | Durch Main vorbereitete Werte: neuer Pfad innerhalb der Grant-Wurzel, `hermes/WP-<plane_id>-<slug>`, frisch geprüfter Commit von `origin/main`. Erst die native Reservierung erlaubt die Worktree-Erstellung. |
| `target` | Frisch verifiziertes sichtbares Coding-Ziel aus dem nativen Resolver; nicht aus einem alten Ticket kopieren. Im registrierten `worker_route` heißt das entsprechende Feld `visible_target`. |
| Paket und Route | Paket enthält bestätigte `logical_resources`, `role_actions`, `acceptance`, Modellbindung in `coding_route`, Lane und tatsächlich verlangte Prüfungen. `worker_route` bindet die konkreten Paket-/Receipt-/Worktree-/Branch-/Zielpfade und die exakte Codex-`argv`. |
| `task`, `generation`, Ticket | Ergebnisse der nativen Registrierung; gehören zu genau diesem Versuch. Kein frei erfundener Ersatzname, kein umgeschriebenes Ticket und keine andere Generation. |
| `candidate_id` | Vom nativen Kandidatenaufbau erzeugte Identität des geprüften Quellbaums und Builds. Für Testresultat und Abnahme exakt übernehmen; Änderungen brauchen einen neuen Kandidaten. |
| Gehashte Evidence | `path` und `sha256` der tatsächlichen unveränderten Datei im erlaubten Artefaktbereich. Der Inhalt muss das richtige Item, den richtigen Kandidaten bzw. Lauf und echte Ergebnisse belegen. Hashes oder Ergebnisse niemals erfinden. |

## Procedure

### 1. Auftrag und vorhandene Belegung

Main liest das vollständige Plane-Item, prüft Repo, Kriterien, Abhängigkeiten/Holds und bindet Lane plus Pflichtprüfungen. Vor neuem Start den bestehenden Item-/Versuchsstatus lesen. Unbekannte Wirkung oder ungeklärte Belegung sperren; stille Terminals, fehlende PIDs und abgelaufene Tickets beweisen keinen Nichtstart.

Altversuche nur über den passenden nativen Reconciliation-/Owner-Archivpfad klären. Vorschau und Quittung müssen genau diesen Versuch betreffen. Keine manuellen Resets von Ledger, Grant, Budget oder Worktree.

Für einen neuen `logical-v2`-Nachfolger kann Main im Owner-Vorschlag `replaces={batch,task,generation,core_sha256,accept_unknown:true}` verwenden. Die Werte stammen aus genau dem blockierten Vorgänger und `operator_archive_core`; dessen tatsächlicher Exit muss nativ belegbar sein. Die Vorschau benennt unbekannte historische Wirkung und möglichen Doppelaufwand. Erst Andis direkte Bestätigung archiviert diesen Vorgänger und reserviert atomar den Nachfolger; keine zusätzliche Archivrunde. Alte Belege/Worktree bleiben erhalten. Ein Nachfolge-Confirm darf nur einen exakt gebundenen blockierten oder bereits abgeschlossenen Altversuch betreffen; der native Pfad muss dessen tatsächliches Ende belegen. Einen tatsächlich laufenden Worker niemals automatisch stoppen oder archivieren. „Abgeschlossen“ erweitert den vorhandenen nativen Archivpfad nicht: bereits abgeschlossene/freigegebene Versuche brauchen keine erneute Archivierung; beim aktuellen APP-104 bleibt `replaces=null`. Bereits archivierte Vorgänger brauchen diese gekoppelte Disposition nicht erneut; bestehende Grants werden nicht nachträglich umgeschrieben.

### 2. Autorisieren, reservieren und vorbereiten

Nach Ownergrant und Bereitschaftsprüfung reserviert Main mit `item_prepare` **vor** Git-I/O. Paket, Receipt und Ergebnis brauchen getrennte frische Pfade. Bei Unterbrechung `item_prepare_readback`, danach gegebenenfalls `item_prepare_resume` für dieselben Ressourcen; keinen Ersatzworktree erzeugen.

Coding erstellt den begrenzten Implementierungsprompt nach Vorbereitung und vor Registrierung, einschließlich des vollständigen unveränderten Onboarding-Inhalts aus `skill_view` (`content`). Diesen Text unter einer normalen Überschrift im letzten Prompt-Argument der Codex-`argv` einbetten (nicht mit `---` beginnen); fehlender Inhalt sperrt neue v2-Worker. Nach Registrierung keine gebundene `argv` umschreiben. Kanonischen Launcher mit `hermes_cli.diggr_continuation.logical_launcher_command(contract, route)` formatieren.

Der Vorbereitungsauftrag an Hermes-Coding und der letzte Prompt-Parameter für Codex sind unterschiedliche Texte. Nur der Vorbereitungsauftrag sagt „noch nicht starten“. Der Codex-Prompt richtet sich an den **bereits nativ gestarteten Executor**: jetzt im gebundenen Worktree implementieren und testen; weder auf ein Startticket warten noch `SYSTEM167_REGISTERED_WORKER` erneut aufrufen. Main prüft diese Rollenübergabe vor Registrierung/Recovery. Ein konkreter Rechte- oder Scope-Widerspruch bleibt ein echter Blocker; diese Rollenklärung erweitert keine Rechte. Alte registrierte Prompts bleiben unverändert; korrigierte Texte gehören ausschließlich in eine frisch geprüfte Recovery-Route.

### 3. Einmal zustellen, echten Start erkennen

Main registriert über `terminal.continuation` mit `background=true`, nativem Ownerbezug und identischem `command`/`launcher_command`. Vollständige Schemafelder verwenden; Reservierung, Route, Lane und Ressourcen müssen übereinstimmen.

Registrierten Handoff einmal sichtbar zustellen; Coding ruft zuerst den registrierten Worker auf. Send-Erfolg/Launcher-Exit sind keine Übernahme: `item_status.execution.claimed`, Generation und Worker-/Prozessidentität prüfen. Bei verlorener Nachricht denselben Claim zurücklesen, niemals blind erneut senden.

### 4. Implementieren, prüfen und Kandidat bauen

Coding beaufsichtigt den sichtbaren Codex, wahrt Scope/No-Touch und prüft Diff plus Tests. Rohresultat unverändert erhalten. Main bewertet separat; fehlt Codings Bewertung, eine Main-Ersatzprüfung ausdrücklich benennen. Exit 0 allein ist kein Erfolg.

Ist das native Worker-Ergebnis unbrauchbar, bevor ein Kandidat existiert, gibt Main es mit `item_return` und einem separaten gehashten Fehlerbericht zurück. Dieser enthält `task`, `worker_generation` aus `worker_result.generation`, dessen `action_id`, `effect_id`, `result_sha256`, `result="failed"` und nichtleere konkrete `findings`. Das gilt nur für den beendeten, unveränderten Worker am Gate `main_validation`; auch eine inzwischen blockierte Main-Bewertung kann so fortgesetzt werden. Der native Pfad prüft den tatsächlichen Exit, erhält das Rohresultat und plant genau einen Reconcile-Wake. Danach bestehendes `recover(kind="review")` mit frischen Exit-/Effektbelegen und verbleibendem Korrekturbudget nutzen. Kein neuer Batch, kein fingierter Kandidat, kein direkter Ersatzworker.

Main prüft alle Quelländerungen, staged ausschließlich beabsichtigte Quellen und erzeugt mit `item_candidate` Build plus Testkarte. Unstaged/untracked Eingaben und Build-Herkunft klären. Bei Unterbrechung `item_build_readback`; solange der Build aktiv ist, weder neu bauen noch parallel korrigieren.

### 5. Funktionstest und persönliche Abnahme vor PR

Testkarte: Item, exakter Kandidat/Build, Handlungen und Erwartungen. Im Automatikmodus kennzeichnet Main jeden Schritt mit `kind="ui"` oder `kind="track_playback"`; jeder Ergebnis-Check muss denselben Typ tragen. Die Karte muss alle im Grant bestätigten Testarten enthalten. Für `track_playback` sind mindestens zwei eigene Wiedergabeschritte mit getrennten Computer-Use-Aktionen nötig. Andi testet standardmäßig selbst. Nur ausdrückliche kandidatspezifische Delegation erlaubt Main den Computer-Use-Test; als delegierten Test über `item_test_result` belegen.

Andis authentifiziertes `/continuation accept CANDIDATE` akzeptiert im Standardmodus genau diesen Kandidaten und erlaubt dessen PR innerhalb der nativen Rechte. Eine einzelne Testdelegation ersetzt diese Abnahme nicht. Nur der ausdrücklich bestätigte Automatikmodus erlaubt Main, den bestandenen Computer-Use-Test über `item_test_result` als Abnahme zu binden. Alle vereinbarten Fälle ausführen; bei Track-Wiedergabe verschiedene relevante Spalten wählen und jeden Track höchstens zehn Sekunden anspielen.

### 6. PR, unabhängiger Review und Merge

Main führt `item_publish_check` aus, erstellt den signierten Commit des abgenommenen Baums und vergleicht beide Bäume. Danach autorisierter Push/PR und GitHub-Readback über `item_pr`. Nach verlorener Antwort erst Remote-Zustand klären.

Reviewer: frischer getrennter sichtbarer Read-only-Kontext, vollständiges Onboarding, exakter PR-Head/Basis, Ziel/Kriterien und echte Nachweise. Keine Implementierung oder Git-Mutation. Vorhandenen Reviewer-Wrapper mit vollständigem Review-Schema verwenden; Main reicht unveränderten Bericht, Skill-Receipt und echte Laufmetadaten über `item_review` ein. CI oder veralteter Review genügen nicht.

Findings seriell im selben Item korrigieren; Codeänderungen erfordern betroffene Tests, neue Kandidatenabnahme und unabhängigen Review. Vor autorisiertem Merge `item_merge_check` und Branch-Protection prüfen, unveränderten Head binden. Neue manuelle Grants brauchen Andis direkte Freigabe genau dieses geprüften PR-Heads mit `/continuation merge-approve CANDIDATE PR_HEAD`; der ausdrücklich bestätigte Automatikmodus enthält die bedingte Freigabe für dieses eine Item. Bestehende Grants behalten ihre bisherigen Rechte. Queue-Eintrag ist noch kein Merge.
Nach `item_review` prüft Main `item_merge_check`. Nur wenn dessen native Quittung `may_merge=true` für den unveränderten PR-Head liefert und die externen Schutzprüfungen bestehen, folgen headgebundener Merge, Merge-Readback, integrierter Main-Smoke und `item_closeout`. Ein fehlendes manuelles Merge-Go hält an; Main meldet den konkreten Freigabebefehl und wartet auf das authentifizierte Owner-Ereignis. Ein ausdrücklicher aktueller Stopp oder ein fehlgeschlagenes Gate hält ebenso an. Nach einem unerwartet beendeten Turn nur über den bestehenden nativen Fortsetzungspfad und innerhalb des Wake-Budgets weiterarbeiten; keine zweite Reviewrunde, kein neuer Worker und kein Budget-Bypass.

### 7. Integriert prüfen, ablegen und schließen

Tatsächlichen Merge zurücklesen, vereinbarten Smoke auf sauberem integriertem `main` ausführen, Evidence in Nextcloud sichern und Plane mit Pflichtfeldern auf den projektgebundenen abgeschlossenen Status bringen: `Accepted`, oder `Done`, wenn kein abgeschlossenes `Accepted` existiert. Der native Readback muss die Gruppe `completed` belegen; Statusnamen nicht erfinden. Erst erfolgreicher `item_closeout` gibt die Lane frei.

Bei integriertem Smoke-Fehler bleibt dasselbe Item belegt: `item_followup_prepare` für neuen Korrekturbranch und Folge-PR; gemergten PR unverändert lassen. Fehlt nur Verwaltung, ausschließlich den fehlenden Schritt anhand echter Belege nachholen.

### Native Itemoperationen und zusätzliche Felder

Alle Operationen benötigen `operation`, `batch`, `issue` und passende native Main-Rechte. Werte aus den oben genannten Herkunftsquellen übernehmen.

| `operation` | Zusätzliche Payload-Felder / Bedeutung |
|---|---|
| `item_status` | Keine. Liefert Phase, Item und getrennte Ausführungsbelege; ein erfolgreicher Readback ist kein Arbeitsabschluss. |
| `item_prepare` | `worktree`, `branch`, `base`, `target`; reserviert vor Git-I/O. |
| `item_prepare_readback`, `item_prepare_resume` | Keine. Prüfen bzw. setzen die bereits reservierten Ressourcen fort; kein neuer Versuch. |
| `item_migration_preview` | Keine. Prüft ein gestopptes bestehendes Item und liefert den gebundenen nativen Owner-Vorschaubefehl; keine Umschaltung. |
| `item_migration_rollback_preview` | Keine. Nur vor Zustellung der neuen Zuweisung; liefert den gebundenen Owner-Rollback-Vorschaubefehl. |
| `item_candidate` | `assessment={path,sha256}`, `build_argv`, `build_path`, `steps=[{action,expected},…]`; im Automatikmodus hat jeder Schritt zusätzlich `kind="ui"` oder `kind="track_playback"`. Bindet echte Workerbewertung, Quellbaum und Build. |
| `item_build_readback` | Keine. Prüft unterbrochenen Build; behauptet keinen Build-Erfolg. |
| `item_test_result` | `candidate_id`, `result`, `evidence={path,sha256}`. Erfordert passende Testdelegation oder den bestätigten Automatikmodus; Evidence enthält `candidate_id`, `tree`, `method="computer_use"`, `result` und bestandene beobachtete Checks für alle Kriterien und Testkartenschritte. Im Automatikmodus trägt jeder Check den `kind` seines Testkartenschritts. Bei `track_playback`: Track, Spalte und Dauer je Fall, höchstens zehn Sekunden und verschiedene Spalten. |
| `item_publish_check` | Keine. Prüft Kandidatenabnahme und Veröffentlichungsgrenzen. |
| `item_pr` | `pr_number`; liest echten GitHub-Zustand zurück. |
| `item_review` | `evidence={path,sha256}`, `review_run={path,sha256}`; vollständiges Review und tatsächliche unabhängige Laufidentität. |
| `item_merge_check` | Keine. Prüft aktuellen Head und dessen Review vor dem genehmigten Merge. |
| `item_return` | `evidence={path,sha256}`; Fehlerbericht bindet aktuellen `candidate_id`, `result="failed"` und nichtleere `findings`; vor Kandidatenbau stattdessen den tatsächlich beendeten Worker mit `task`, `worker_generation`, `action_id`, `effect_id`, `result_sha256` (siehe Schritt 4). |
| `item_followup_prepare` | `worktree`, `branch`, `base`, `target`, `evidence={path,sha256}`; tatsächlicher Post-Merge-Smoke-Fehler desselben Items. |
| `item_closeout` | `main_worktree`, `smoke={path,sha256}`, `archive={path,sha256}`, `nextcloud_path`; verifiziert Merge, Smoke, identische Ablage und Plane-Abschluss. |

## Pitfalls

- Ein neuer Name, Batch, Pfad oder Phasenwechsel setzt kein kumulatives Arbeits-/Korrekturbudget zurück. Echte Erschöpfung mit dem konkreten Zusatzbedarf melden. Die einzige Ausnahme ist eine direkte, authentifizierte Ablehnung eines noch nicht abgenommenen Kandidaten: `item_return` darf dann genau eine gleich begrenzte Korrektur aus diesem Owner-Feedback ableiten. Arbeitszeit und Dateiscope werden dabei nicht erweitert.
- Bei unbekanntem Start oder unbekannter Wirkung denselben Versuch nativ abgleichen. Keine blinde Wiederholung, kein unregistrierter Ersatzworker und keine parallelen Writer.
- Nach Review-/Testfehler `item_return` und den passenden nativen Reconcile-Wake abwarten. Recovery benötigt die echten Exit-/Effektbelege und restlichen Rechte desselben Items; sie ist keine pauschale Startfreigabe.
- Stop/Widerruf beachten. Eine Statusfrage ist kein Stop, aber auch keine zusätzliche Genehmigung. Ownerentscheidungen niemals stellvertretend erzeugen.
- Technischer Fehler, fehlendes Recht und fehlender Nachweis sind verschiedene Ergebnisse. Konkrete Phase, fehlenden Beleg und zuständige nächste Aktion melden statt „fertig“ oder einer generischen Neustartempfehlung.

## Verification

Vor jedem Übergang: richtige Rolle/Item, erlaubte Aktion, echte Werteherkunft, passender Kandidat/Head/Versuch und vollständige Nachweise. Bei Abweichung stoppen und konkreten fehlenden Beleg samt nächster zuständiger Aktion nennen.

Betrieblich vollständig nachgewiesen ist der Loop erst nach realem Durchlauf bis bestätigtem `Accepted`, Ablage und Lane-Freigabe. Installation, einzelne Tests und ein gedanklicher Durchlauf beweisen das nicht.

Vollständige Payload-Definitionen: aktuelles `terminal`-Werkzeugschema und `/Users/diggr/Projects/diggr_claw/docs/runbooks/optimized_coding_loop.md`. Bei Widerspruch anhalten und melden; keine Felder oder Rechte erraten.
