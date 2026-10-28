# Mock dell'admin portal per il workflow FortiGate (p2p)

Un finto admin portal dentro il cluster, contro cui far girare **lo stesso WorkflowTemplate**
del workflow FortiGate (`config/samples/p2p-workflow`): dodici chiamate API, dalla scelta della
subnet iTop alla delivery e all'attesa del job. Il mock non crea niente di reale: risponde con
valori fittizi ma coerenti, tiene in memoria segmenti, subnet e job, simula il job di delivery
con le sue 14 fasi, e si può **rompere a comando** in punti precisi.

Stessa logica del [mock OpenStack](MOCK-OPENSTACK.md): stesso binario, stesso motore di stato,
stesse regole di guasto, stessa interfaccia di controllo `/__mock/`, stesso trattamento delle
chiamate non previste. Cambiano solo le API simulate.

## Indice

1. [Obiettivi e limiti](#1-obiettivi-e-limiti)
2. [Architettura e parti in comune](#2-architettura-e-parti-in-comune)
3. [Collegamento con latch](#3-collegamento-con-latch)
4. [Dati iniziali](#4-dati-iniziali)
5. [Chiamate da simulare](#5-chiamate-da-simulare)
6. [Comportamenti realistici](#6-comportamenti-realistici)
7. [Interfaccia di controllo e guasti](#7-interfaccia-di-controllo-e-guasti)
8. [Scenari di test](#8-scenari-di-test)
9. [Piano di implementazione](#9-piano-di-implementazione)

---

## 1. Obiettivi e limiti

**Obiettivi**

- Eseguire il workflow FortiGate con gli **stessi step** (input, output, `select`, `expect`,
  `retry`, `poll`, `recover`) cambiando solo l'indirizzo del portale e la chiave API.
- Risposte con la stessa forma di quelle del portale, per quello che gli step leggono, ed errori
  nella stessa forma (`{"detail": {"code", "message", "detail"}}` e la lista di validazione).
- Stato in memoria: una subnet usata non è più libera, un segmento creato esiste, un template in
  uso da una delivery è bloccato, un job avanza di fase in fase.
- Il job di delivery come quello vero: fasi, stati di attesa di una persona, fallimenti, ripresa
  con `retry` dalla fase indicata e con le stesse regole di validazione.
- Guasti iniettabili via API, come nel mock OpenStack, più quelli propri del job (fallire alla
  fase N, restare in attesa, revisione manuale richiesta).

**Limiti, dichiarati**

- Implementa le chiamate del workflow p2p, più quelle che servono ai suoi scenari (ripresa,
  decisioni sul job, pulizia del segmento). Le altre del portale rispondono `501` e vengono
  registrate (vedi 5.4).
- Le regole del portale sono quelle documentate in `fortigate-api-examples/crea-fortigate.py`;
  dove il comportamento reale non è noto, il mock sceglie quello più restrittivo e lo dichiara.
- Lo stato vive nel processo: un riavvio del pod del mock cancella segmenti e job.

## 2. Architettura e parti in comune

Lo stesso binario e la stessa Service di `cmd/mock-api` (namespace `apicall-debug`), sotto il
prefisso `/portal/api/v1/` che ricalca quello del portale vero, **in HTTPS sulla porta 8443**.
L'operatore non manda mai credenziali presi da un Secret (qui la chiave API) su HTTP in chiaro,
e non c'è motivo di indebolire questa regola per un test: nello stack Tilt il mock serve HTTPS
sulla 8443, accanto all'HTTP della 8080, con un certificato di test creato da
`hack/mock-api-tls.sh` (Secret `mock-api-tls`), e l'operatore si fida della sua CA
(`--apicall-ca-file`, ConfigMap `apicall-debug-ca`) senza disattivare nessuna verifica.

```
ApiCall (operatore latch)
   │  endpoint = https://mock-api.apicall-debug.svc.cluster.local:8443/portal/api/v1/...
   ▼
mock-api :8080
   ├── /portal/api/v1/...       admin portal: iTop, segmenti, ambienti, template, FlexVM, delivery, job
   ├── /identity, /compute, /network, /image   mock OpenStack (MOCK-OPENSTACK.md)
   ├── /__mock/...              controllo comune: inventario, reset, guasti, richieste
   └── (percorsi esistenti)     /echo, /vdcs, /networks, /oauth/token, /__debug/requests
```

**Motore comune**, scritto una volta e usato da entrambi i mock (`cmd/mock-api/engine*.go`):

| Parte | Cosa fa |
|---|---|
| Store | Mappe per tipo di risorsa con mutex, ID generati, eventi di creazione e modifica |
| Orologio | Gli stati asincroni (VM in `BUILD`, fasi del job) avanzano col tempo reale, con durate configurabili |
| Regole di guasto | Stessa sintassi e stessi effetti (`status`, `delay`, `reset`, `hang`, `body`), con `after`, `times`, `probability`, `expiresInSeconds`; `match.service` vale anche `portal` |
| Registro richieste | `/__mock/requests` con esito e regola applicata |
| Chiamate non previste | `501` con messaggio chiaro, registrate in `/__mock/unhandled` |
| Reset | `POST /__mock/reset` (tutti) o `POST /__mock/reset?service=portal` |

## 3. Collegamento con latch

Nel workflow API l'indirizzo del portale sta negli ApiCallTemplate, non in un Secret: per il mock
servono delle copie con un altro indirizzo. Le copie sono **generate** dai manifest veri, così
non possono divergere.

| Oggetto | Nome | Differenza rispetto al workflow reale |
|---|---|---|
| ApiCallTemplate | `p2p-*-mock` (una per ogni `p2p-*`) | Base dell'endpoint `https://mock-api.apicall-debug.svc.cluster.local:8443/portal/api/v1`, Secret `portal-api-key-mock`, niente `debug`; tutto il resto identico |
| Secret | `portal-api-key-mock` | La chiave che il mock accetta (`mock-portal-key`) |
| WorkflowTemplate | `p2p-free-subnet-mock` | Copia di quello vero che usa i template `p2p-*-mock`; `debug` tolto da tutti gli step |
| Workflow | `p2p-free-subnet-mock` | Copia del Workflow d'esempio, stessi parametri |

- **Generazione:** `go run ./hack/gen-mock-samples` (logica in `internal/mocksamples`) produce
  `config/samples/p2p-workflow/mock/` dai manifest veri: cambia base dell'endpoint, nomi, Secret
  e toglie `debug`. Un test fallisce se le copie non corrispondono più agli originali.
- **Origine ammessa:** l'operatore dello stack Tilt ammette
  `https://mock-api.apicall-debug.svc.cluster.local:8443` (`config/tilt`).
- Il workflow API non ha state esterno: ogni Workflow è un'esecuzione, e lo stato che conta è
  quello del mock (segmenti, subnet usate, job).

## 4. Dati iniziali

Caricati all'avvio e da `POST /__mock/reset?service=portal`. Contengono i valori del Workflow
d'esempio (`config/samples/p2p-workflow/workflow.yaml`), così gli step trovano quello che cercano.

| Tipo | Valori | Note |
|---|---|---|
| Blocco iTop | `BLK-AUTOMAZIONE-P2P` (+ un altro blocco) | `/blocks[] {id, name}` |
| Subnet /31 del blocco | 4 subnet, le prime 2 libere | `{ip, mask, available, block_id, nsx_ip, tenant_ip, id}`; il numero di quelle libere è configurabile |
| Ambiente | `vcloud-r1.dinova.cloud` (`env_type: vcloud`, `vcenter_env_name`), più un ambiente `vcenter` | `/envs[]` |
| Template FortiGate | 3 per il vCenter dell'ambiente, il primo libero | `locked_by_job_id`, `locked_job_vm_name`, `locked_job_status` |
| Organizzazione vCloud | `testav17` (+ un'altra) | |
| Programma FlexVM | serial `ELAVMR0000000657` | |
| Config FlexVM | `Istanza FortiGate SMALL - Funzionalità UTP Bundle` (id numerico, come `3619`) e altre | |
| Storage profile | `Cloud_Gold`, `DATA4_ENT` | |
| Chiave API | configurabile | header `X-API-Key` |

## 5. Chiamate da simulare

Percorsi relativi a `/portal/api/v1`. "Legge" indica i campi che gli step usano.

### 5.1 Letture (step 1–9, con `retry` sulle GET)

| Step | Chiamata | Risposta | Legge |
|---|---|---|---|
| `list-blocks` | `GET /p2p/itop/blocks` | `{blocks: [{id, name}]}` | `id` del blocco con `name` = `blockName` |
| `free-subnet` | `GET /p2p/itop/block-subnets?mask=31&block_id=…` | `{subnets: [{ip, mask, available, block_id, nsx_ip, tenant_ip}]}` | la prima con `available: true` |
| `check-name` | `GET /p2p/segments/name-check?client_name=…&env_name=…` | `{ok: bool}` (falso se il segmento esiste già) | `expect /ok == true` |
| `get-env` | `GET /fortigate-vm/envs` | `[{id, env_type, vcloud_env_name, vcenter_env_name}]` | quello `vcloud` con il nome dato |
| `get-templates` | `GET /fortigate-vm/templates?vcenter_env_name=…` | `[{id, label, locked_by_job_id, …}]` | `/0/id` |
| `get-orgs` | `GET /fortigate-vm/vcloud/{env_id}/orgs` | `[{id, name}]` (`404` se l'ambiente non esiste) | `id` dell'org |
| `get-flexvm-programs` | `GET /fortigate-vm/flexvm/programs` | `[{id, serial}]` | |
| `get-flexvm-configs` | `GET /fortigate-vm/flexvm/configs?program_id=…` | `[{id, name}]` | |
| `get-storage-profiles` | `GET /fortigate-vm/vcloud/{env_id}/provider-storage-profiles` | `[{name}]` | |

### 5.2 Scritture

| Step | Chiamata | Risposta |
|---|---|---|
| `create-segment` | `POST /p2p/segments/create` `{env_name, client_name, subnet_ip, subnet_mask, itop_block_id, …}` | `201` con il segmento; la subnet diventa `available: false`; `409` se il nome esiste, `422` se la subnet non è libera |
| `deliver` | `POST /fortigate-vm/vcloud/{env_id}/deliver` (corpo come in `fortigate-api-examples/03-vcloud-flexvm.json`) | `202 {job_id, status: "queued"}`; il template passa a bloccato; `409 TEMPLATE_LOCKED` se lo era già, **lasciando comunque un job `failed`** come il portale vero; `422` con la lista dei campi se il corpo non è valido |

### 5.3 Job

| Uso | Chiamata | Risposta |
|---|---|---|
| `wait-deliver` (poll) | `GET /fortigate-vm/jobs/{job_id}` | `{id, status, vm_name, last_completed_step, total_steps: 14, license_serial, licensing_mode, manual_review_required, rollback_attempted, assigned_ip, log_output}`; `404` se il job non esiste |
| ripresa (`recover`) | `POST /fortigate-vm/jobs/{job_id}/retry` `{resume_from_step, forticloud_license_account_password?}` | `200 {status: "queued"}`; `409` se il job non è `failed`; `422` se `resume_from_step` > `last_completed_step + 1` o < 1 |
| elenco | `GET /fortigate-vm/jobs` | i job senza `log_output` |
| decisione licenza | `GET` / `POST /fortigate-vm/jobs/{id}/license-decision` `{terminate_existing}` | il job riparte da `waiting_license_decision` |
| IP sorgente | `GET /fortigate-vm/jobs/{id}/src-ips`, `POST …/src-ip` `{src_secondary_ip}` | il job riparte da `waiting_src_ip` |
| pulizia segmento (`deleteSteps`, se attivati) | `DELETE /fortigate-vm/envs/{env_id}/nsx-segment?name=…` | `200`, `200 {result: "not_found"}`, `409` con porte attaccate, `422` nome fuori convenzione; la subnet torna libera |

### 5.4 Chiamate non previste

Come nel mock OpenStack: `501`, registrate in `/__mock/unhandled`, mai una risposta inventata.

## 6. Comportamenti realistici

| Comportamento | Default | Configurabile |
|---|---|---|
| Durata di una fase del job | 2 s (14 fasi ≈ 30 s) | `phaseSeconds` |
| Stato iniziale del job | `queued` per 1 fase, poi `running` | |
| Fine del job | `success`, `last_completed_step: 14`, `license_serial` valorizzato per `flexvm` | |
| Ripresa da `resume_from_step: k` | riparte da `k`, `last_completed_step = k-1`; con `1` su vCloud la "VM" del tentativo prima viene cancellata | |
| Template bloccato | dalla delivery fino alla fine del job (`success` o `failed` senza ripresa entro `lockSeconds`) | `lockSeconds` |
| `log_output` | cresce di una riga per fase; può essere gonfiato a N KiB per provare il limite di 64 KiB e `keepJSONPointers` | `logKiB` |
| Chiave API mancante o sbagliata | `401 {"detail": {"code": "UNAUTHORIZED", …}}` | |
| Errori | `{"detail": {"code", "message", "detail"}}`; validazione `{"detail": [{"loc", "msg"}]}` | |

## 7. Interfaccia di controllo e guasti

### 7.1 Comune ai due mock

Inventario (`GET /__mock/state`), richieste (`/__mock/requests`), chiamate non previste
(`/__mock/unhandled`), reset e configurazione, e le **regole di guasto** con la stessa sintassi
di [MOCK-OPENSTACK.md §7.2](MOCK-OPENSTACK.md#72-guasti), con `match.service: "portal"`. Esempi:

```json
{"name": "envs-503", "match": {"service": "portal", "method": "GET", "path": "/fortigate-vm/envs$"},
 "action": {"type": "status", "status": 503}, "times": 2}

{"name": "poll-drop", "match": {"service": "portal", "method": "GET", "path": "/jobs/[0-9]+$"},
 "action": {"type": "hang", "seconds": 60}, "after": 3, "times": 1}

{"name": "deliver-reset-after-create", "match": {"service": "portal", "method": "POST", "path": "/deliver$"},
 "action": {"type": "reset", "afterHandler": true}, "times": 1}
```

`afterHandler: true` (solo per `reset` e `hang`) esegue prima la richiesta e poi chiude la
connessione: il job esiste ma latch non lo sa. Vale per entrambi i mock (per OpenStack: VM
creata, risposta persa).

### 7.2 Propri del portale

| Chiamata | Effetto |
|---|---|
| `POST /__mock/portal/next-job` `{failAt: [3, 6, 6], waitAt: {5: "waiting_license_decision"}, manualReviewAt: 9, neverEnds: false}` | Copione del **prossimo** job: fallisce alla fase 3, poi alla 6 due volte, chiede una decisione alla fase 5, chiede la revisione manuale alla 9 |
| `POST /__mock/portal/jobs/{id}/fail` `{atStep?, manualReview?}` | Fa fallire un job in corso |
| `POST /__mock/portal/jobs/{id}/wait` `{status}` | Mette un job in attesa (`waiting_src_ip`, `waiting_license_decision`) |
| `POST /__mock/portal/jobs/{id}/stuck` | Il job resta `running` senza avanzare |
| `POST /__mock/portal/templates/{id}/lock` / `unlock` | Template in uso da un'altra delivery |
| `POST /__mock/portal/subnets/exhaust` | Nessuna subnet libera nel blocco |
| `POST /__mock/portal/segments` `{name}` | Segmento già esistente (per `name-check`) |

## 8. Scenari di test

| # | Scenario | Come si rompe il mock | Cosa verificare |
|---|---|---|---|
| P1 | Percorso normale | nessun guasto | 12 step `Succeeded`, output (`job_id`, `license_serial`, `last_completed_step: 14`), segmento e subnet usata nell'inventario |
| P2 | GET con errori transitori | `503` su `/envs` con `times: 2` | `retry` dell'ApiCall (3 tentativi) e successo; `status.attempts` |
| P3 | GET con errore definitivo | `500` (non tra gli `statusCodes`) | il Workflow fallisce subito, nessuna scrittura |
| P4 | Rete giù sulle letture | `reset` con `times: 3` | `RetriesExhausted` dopo 3 tentativi |
| P5 | `select` senza risultato | blocco inesistente, `subnets/exhaust`, org assente | `OutputMissing` chiaro, nessuna scrittura |
| P6 | Nome segmento occupato | `segments {name}` | `expect` fallisce con "The segment name is not available" |
| P7 | Creazione segmento rifiutata | `409` / `422` su `/segments/create` | il Workflow fallisce; nessuna delivery |
| P8 | Template bloccato | `templates/{id}/lock` | `409 TEMPLATE_LOCKED` sulla delivery, un job `failed` lasciato nel portale (visibile nell'inventario) |
| P9 | Delivery non valida | `422` con la lista dei campi | messaggio con il campo colpevole |
| P10 | Risposta della delivery persa | `reset` con `afterHandler: true` | il job esiste ma il Workflow fallisce; un Workflow nuovo trova il template bloccato (P8): la `POST` non è ripetibile |
| P11 | Job riuscito | nessun guasto | `poll` dentro un solo ApiCall (`status.attempts`), output dalla risposta finale |
| P12 | Fallimenti a catena con ripresa | `next-job {failAt: [3, 6, 6]}` | `recover` con `resume_from_step` 3, 6, 6; `status.recoveries`; successo (come l'envtest) |
| P13 | Nessun progresso | `next-job {failAt: [4, 4, 4, 4]}` | `RecoveryExhausted` dopo 3 riprese senza progresso |
| P14 | Troppe riprese | 11 fallimenti in fasi crescenti | `RecoveryExhausted` per `maxRecoveries` |
| P15 | Revisione manuale | `next-job {failAt: [7], manualReviewAt: 7}` | `RecoveryBlocked` con il messaggio della regola `unless` |
| P16 | Job in attesa di una persona | `next-job {waitAt: {5: "waiting_license_decision"}}` | `PollFailed` subito, nessuna ripresa |
| P17 | Ripresa rifiutata | `422` su `/retry` | `RecoveryFailed` |
| P18 | Job che non finisce | `next-job {neverEnds: true}` con `maxAttempts` basso in una copia del template | `PollTimeout` |
| P19 | Poll con errori transitori | `503` / `reset` su `/jobs/{id}` con `times: 2` | tollerati (`retryOnStatusCodes`, `retryOnTransportErrors`), il poll continua |
| P20 | Poll con errore definitivo | `404` su `/jobs/{id}` | fallimento immediato |
| P21 | Risposta del job grande | `logKiB: 200` | `keepJSONPointers` tiene solo i campi letti; nessun `ResponseTooLarge` |
| P22 | Risposta malformata | `body` con JSON troncato sul poll | errore chiaro |
| P23 | Operatore riavviato durante il poll | riavvio del pod dell'operatore | il poll riprende, nessuna delivery ripetuta |
| P24 | Due delivery in parallelo | due Workflow con lo stesso template | la seconda trova il template bloccato |
| P25 | Pulizia del segmento | `deleteSteps` attivati, Workflow cancellato | `DELETE …/nsx-segment`, subnet di nuovo libera; con `409` il finalizer resta |
| P26 | Rete intermittente | `reset` con `probability: 0.5` su tutto | quali step reggono (GET con `retry`) e quali no (`POST`) |

## Uso

```bash
kubectl apply -k config/samples/p2p-workflow/mock/             # template, WorkflowTemplate, chiave
M=localhost:8088                                                # port-forward di Tilt (HTTP; HTTPS su 8443)
curl -s -X POST $M/__mock/portal/next-job -d '{"failAt":[3,6,6]}'   # copione del prossimo job
kubectl create -f config/samples/p2p-workflow/mock/workflow.yaml
curl -s $M/__mock/state | jq .portal
curl -s -X POST "$M/__mock/reset?service=portal"
```

Ogni Workflow del mock crea un segmento: un secondo run con lo stesso `clientName` trova il nome
occupato (`check-name`, come il portale vero). Si cambia `clientName` o si fa il reset.

## 9. Piano di implementazione

Dopo il motore comune (passi 1 e 5 di [MOCK-OPENSTACK.md](MOCK-OPENSTACK.md#9-piano-di-implementazione)):

- [x] **1. Dati iniziali e letture** (4, 5.1), con errori nella forma del portale e controllo
      della chiave API.
- [x] **2. Segmenti** (`name-check`, `create`, `DELETE nsx-segment`) con subnet che si occupano e
      si liberano.
- [x] **3. Delivery e job**: macchina a stati delle 14 fasi, template bloccati, copione del
      prossimo job, ripresa con le regole di validazione, attese di una persona.
- [x] **4. Controlli propri** (7.2) e `afterHandler` nelle regole di guasto.
- [x] **5. Test Go** del mock: ogni chiamata, la macchina a stati del job, le regole di ripresa.
- [x] **6. Manifest generati** (`go run ./hack/gen-mock-samples`, `config/samples/p2p-workflow/mock/`)
      e test che le copie corrispondano agli originali.
- [x] **7. Validazione**: P1 completo nel cluster in 37 s senza chiamate non previste; anche P12
      (fallimenti alle fasi 3, 6, 6 e ripresa automatica fino alla fase 14).
- [x] **8. Scenari** P2–P26: vedi [Esiti](#esiti).

## Esiti

Eseguiti il 27/10/2026 nel cluster `kind-er-operator` contro il mock in HTTPS, con una copia del
WorkflowTemplate che fa il poll ogni 5 s invece di 30 (solo il ritmo cambia) e il blocco del
template dopo un job fallito ridotto a 1 s; portale finto azzerato prima di ogni scenario.

| # | Esito | Note |
|---|---|---|
| P1 | ✅ | 12 step in 37 s, nessuna chiamata non prevista |
| P2 | ✅ | Due 503 su `/envs` assorbiti dal `retry` (3 tentativi) |
| P3 | ✅ 🔧 | 500 su `/envs`: fallimento immediato. Il messaggio non diceva lo stato HTTP: **corretto** (vedi P9) |
| P4 | ✅ | Tre connessioni chiuse: `RetriesExhausted` |
| P5 | ✅ | Nessuna subnet libera: `OutputMissing` su `free-subnet`, nessuna scrittura |
| P6 | ✅ | Nome occupato: `expect` fallisce con "The segment name is not available" |
| P7 | ✅ | 409 sulla creazione del segmento |
| P8 | ✅ | Template bloccato: 409 sulla delivery e un job `failed` lasciato nel portale |
| P9 | ✅ 🔧 | 422 sulla delivery. **Due difetti corretti**: il messaggio del Workflow ora riporta il motivo (`Remote API returned HTTP 422`, anche per `RecoveryFailed`), e la risposta di errore non viene più ridotta a `null` da `keepJSONPointers` (si riducono solo le risposte 2xx) |
| P10 | ✅ | Risposta della delivery persa: il job gira nel portale senza che nessuno lo segua; un nuovo Workflow trova il template bloccato |
| P11 | ✅ | Coperto da P1 |
| P12 | ✅ | Fallimenti alle fasi 3, 6, 6: tre riprese (3, 6, 6) e job concluso alla fase 14 |
| P13 | ✅ | Nessun progresso: `RecoveryExhausted` alla quarta volta alla fase 3 |
| P14 | ✅ | 10 riprese con progresso, poi `RecoveryExhausted` per `maxRecoveries` |
| P15 | ✅ | Revisione manuale: `RecoveryBlocked` con il messaggio della regola `unless` |
| P16 | ✅ | `waiting_license_decision`: `PollFailed` immediato |
| P17 | ✅ | `/retry` rifiutato: `RecoveryFailed` con lo stato HTTP |
| P18 | ✅ | Job che non finisce: `PollTimeout` |
| P19 | ✅ | 503 sul poll tollerati, poll continua |
| P20 | ✅ | 404 sul poll: fallimento immediato con lo stato HTTP |
| P21 | ✅ | Risposta del job oltre 200 KiB: ridotta agli 8 campi letti, nessun troncamento |
| P22 | ✅ nota | Una sola risposta malformata chiude l'attesa (`ConditionInvalid`) senza riprovare |
| P23 | ✅ | Operatore riavviato durante il poll: un solo job, nessuna delivery ripetuta |
| P24 | ⚠️ | Due Workflow in parallelo scelgono **la stessa subnet libera**: il secondo fallisce su `create-segment` (422). Manca una prenotazione tra lettura e scrittura |
| P25 | ✅ | `deleteSteps`: segmento non in uso cancellato e subnet liberata; segmento in uso → 409, il finalizer trattiene il Workflow |
| P26 | ✅ nota | Rete al 50 %: le GET reggono con il `retry`, la prima `POST` fallisce (non si ripete, giustamente) |

### Difetti e osservazioni

- [x] **Messaggio di `ChildCallFailed` senza motivo** (P3, P7, P9): corretto in
      `internal/controller/workflow/reconcile.go`.
- [x] **Risposte di errore perse con `keepJSONPointers`** (P9): corretto in
      `internal/controller/apicall/api_executor.go`, con test.
- [ ] **Subnet contesa tra Workflow paralleli** (P24): due delivery per clienti diversi lanciate
      insieme si contendono la prima subnet libera. Da risolvere nel portale (prenotazione) o
      serializzando i Workflow p2p.
- [ ] **Risposta del poll malformata** (P22): oggi chiude l'attesa; si potrebbe tollerarla come
      un errore transitorio.
- [ ] **Delivery con risposta persa** (P10): il job resta orfano nel portale. Serve una procedura
      (o un controllo per `vm_name` prima della delivery) per ritrovarlo.
