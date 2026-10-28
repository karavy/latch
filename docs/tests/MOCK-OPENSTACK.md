# Mock OpenStack per gli step Terraform di Virtuozzo

Un finto Virtuozzo/OpenStack dentro il cluster, contro cui far girare **lo stesso codice
Terraform** del workflow `virtuozzo-vm`. Il mock non crea niente di reale: risponde alle
chiamate del provider `terraform-provider-openstack` con valori fittizi ma coerenti, tiene in
memoria le "risorse" create, e si può **rompere a comando** in punti precisi per vedere come si
comportano latch e Terraform.

Serve a ripetere in modo deterministico, veloce e senza toccare Virtuozzo i test di
[TEST-PLAN.md](TEST-PLAN.md), soprattutto quelli che sul sistema reale dipendono dal caso (la
finestra di un'interruzione, la rete che va e viene, il pool di IP pieno) o che costano risorse
vere (duplicati, orfani).

## Indice

1. [Obiettivi e limiti](#1-obiettivi-e-limiti)
2. [Architettura](#2-architettura)
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

- Eseguire `network` e `vm` (plan, apply, destroy, refresh) con il **codice Terraform reale**
  (stessi TerraformTemplate e ConfigMap), cambiando solo le credenziali.
- Risposte con la stessa forma di quelle di OpenStack, per quello che il provider legge.
- Stato in memoria: ciò che Terraform crea esiste finché non lo cancella, quindi i run successivi
  vedono "No changes", il drift, le risorse sparite.
- Guasti iniettabili via API: stato HTTP, ritardo, connessione chiusa, richiesta appesa,
  risorsa che resta in `BUILD` o va in `ERROR`, cancellazione che non finisce, pool di IP pieno.
- Inventario consultabile: quante VM con lo stesso nome, quali risorse non sono in nessuno state.

**Limiti, dichiarati**

- Non è OpenStack: implementa solo le chiamate che il provider usa per queste risorse, con la
  semantica minima. Un'altra versione del provider può fare chiamate diverse: il mock le
  registra come "non implementate" (vedi 5.5) invece di inventare una risposta.
- Lo stato vive nel processo: un riavvio del pod del mock cancella tutte le risorse finte (è
  anche uno scenario utile: "infrastruttura sparita, state presente").
- Niente TLS né Keystone reale: il token è una stringa qualunque, l'autenticazione verifica solo
  utente e password configurati.

## 2. Architettura

Lo stesso binario e la stessa Service del mock esistente (`cmd/mock-api`, namespace
`apicall-debug`, porta 8080, HTTP), con nuovi percorsi. Resta solo libreria standard Go, ma il
codice si divide in più file del package `main` (`openstack_*.go`, `faults.go`): il
`Dockerfile.mock-api` va aggiornato per copiare tutti i `.go`.

```
Job Terraform (namespace default)
   │  TF_VAR_os_auth_url = http://mock-api.apicall-debug.svc.cluster.local:8080/identity/v3
   ▼
mock-api :8080
   ├── /identity/v3/...          Keystone: token e catalogo
   ├── /compute/v2.1/<project>/... Nova: server, flavor, azioni
   ├── /network/v2.0/...         Neutron: reti, subnet, router, porte, floating IP
   ├── /image/v2/...             Glance: immagini
   ├── /__mock/...               controllo: inventario, reset, guasti, drift
   └── (percorsi esistenti)      /echo, /vdcs, /networks, /oauth/token, /__debug/requests
```

- **Catalogo.** Il token restituisce un catalogo con `compute`, `network`, `image`, `identity`,
  tutti verso il mock stesso, nella regione `RegionOne`.
- **Stato.** Un unico store protetto da mutex: mappe per tipo di risorsa, ID UUID generati,
  contatori per il pool di IP. Ogni modifica registra un evento (tipo, ID, operazione, ora) per
  l'inventario.
- **Guasti.** Un middleware valuta le regole di guasto prima di ogni handler OpenStack (vedi 7).
- **Motore comune.** Store, orologio degli stati asincroni, regole di guasto, registro delle
  richieste, chiamate non previste e reset sono condivisi con il
  [mock dell'admin portal](MOCK-PORTAL.md): stessa logica, scritta una volta.
- **Log.** Ogni richiesta OpenStack viene registrata (metodo, percorso, esito, regola di guasto
  applicata) e consultabile in `/__mock/requests`, oltre a finire nel log del pod.

## 3. Collegamento con latch

Il codice Terraform resta quello vero; cambiano solo credenziali e state.

| Oggetto | Nome | Differenza rispetto al workflow reale |
|---|---|---|
| Secret | `virtuozzo-credentials-mock` | `TF_VAR_os_auth_url` verso il mock, utente e password del mock; gli altri `TF_VAR_` uguali (nomi di rete, immagine, flavor…) |
| WorkflowTemplate | `virtuozzo-vm-mock` | Copia di `virtuozzo-vm` che usa il Secret del mock e gli state `virtuozzo-vm-mock/{{parameters.clientName}}/…` |
| TerraformTemplate | `virtuozzo-network`, `virtuozzo-vm` | **Gli stessi**: il mock prova il codice che va in produzione |
| Workflow | `virtuozzo-vm-example-mock` | Copia del Workflow d'esempio |
| WorkflowInstance | `acme-vms-mock` | Copia dell'istanza d'esempio |

Gli state del mock stanno sotto `default/virtuozzo-vm-mock/…`: non si mescolano mai con quelli
veri. I manifest stanno in `config/samples/terraform-workflow/mock/` e sono **generati** dai
manifest veri con `go run ./hack/gen-mock-samples` (logica in `internal/mocksamples`): un test
fallisce se una copia non corrisponde più all'originale, quindi dopo aver cambiato un sample
vero si rilancia il generatore.

Il Keystone del mock si chiama in HTTP (`:8080`): il provider Terraform parla direttamente con
il mock, senza passare dall'operatore.

## 4. Dati iniziali

Caricati all'avvio e da `POST /__mock/reset`. Corrispondono ai valori del Secret
`virtuozzo-credentials`, così il codice Terraform non cambia.

| Tipo | Nome | Note |
|---|---|---|
| Progetto | `admin` (ID fisso) | dominio `virtuozzo` / `Default` |
| Utente | `admin` / password configurabile | |
| Rete esterna | `public` | `router:external: true`, subnet con pool di floating IP di **N indirizzi** (configurabile, default 6) |
| Rete interna | `private` | con la subnet **`a6c3e3aa-f3b3-44da-98ab-1a20411c7234`** che `network.tf` usa in modo fisso |
| Immagine | `ubuntu-2604-kube-v1.36.1` | `status: active` |
| Flavor | `medium` (e `small`, `large`) | |
| Keypair | `capi-key` | |
| Security group | `default` | |
| Tipo di volume | `default` | |

## 5. Chiamate da simulare

Elenco **confermato** il 27/10/2026 facendo girare `network` e `vm` (creazione, refresh,
drift, destroy) contro il mock: sono le chiamate registrate in `/__mock/requests`, senza nessuna
chiamata non prevista. Le colonne "Risposta" descrivono solo i campi che il provider legge.

Chiamate di un run completo (`network` + `vm`, due VM):

```
12 GET  /network/v2.0/networks?name=…        6 POST /identity/v3/auth/tokens
10 GET  …/servers/{id}                         4 GET  /network/v2.0/floatingips/{id}
 2 POST /network/v2.0/floatingips              2 POST …/servers/{id}/action   (addFloatingIp)
 2 POST …/servers                              2 GET  /network/v2.0/routers/{id}
 2 GET  /network/v2.0/ports/{id}               2 GET  /network/v2.0/floatingips?…
 2 GET  /image/v2/images?name=…                2 GET  …/servers/{id}/tags
 2 GET  …/flavors/detail                       2 GET  …/flavors/{id}
 1 PUT  /network/v2.0/routers/{id}/add_router_interface
 1 POST /network/v2.0/routers
```

### 5.1 Autenticazione (tutti gli step)

| Operazione | Chiamata | Risposta |
|---|---|---|
| Token | `POST /identity/v3/auth/tokens` (password, scope progetto) | `201`, header `X-Subject-Token`, corpo con `token.catalog`, `project`, `user`, `expires_at` |
| Verifica token (eventuale) | `GET /identity/v3/auth/tokens` | `200` |

### 5.2 Step `network`

| Risorsa | Operazione | Chiamata |
|---|---|---|
| `data.openstack_networking_network_v2.public` | lettura | `GET /network/v2.0/networks?name=public` |
| `openstack_networking_router_v2.r` | create | `POST /network/v2.0/routers` (con `external_gateway_info.network_id`) → `201`, poi attesa `status: ACTIVE` con `GET …/routers/{id}` |
| | read / refresh | `GET /network/v2.0/routers/{id}` (`404` se sparito) |
| | delete | `DELETE /network/v2.0/routers/{id}`, poi `GET` fino a `404` |
| `openstack_networking_router_interface_v2.ri` | create | `PUT /network/v2.0/routers/{id}/add_router_interface` (`subnet_id`) → `port_id`; attesa porta `ACTIVE` con `GET …/ports/{port_id}` |
| | read | `GET /network/v2.0/ports/{port_id}` |
| | delete | `PUT …/routers/{id}/remove_router_interface`, poi `GET` porta fino a `404` |

### 5.3 Step `vm`

| Risorsa | Operazione | Chiamata |
|---|---|---|
| `data.openstack_images_image_v2.img` | lettura | `GET /image/v2/images?name=…` (con ordinamento per `most_recent`) |
| `data.openstack_networking_network_v2.net` | lettura | `GET /network/v2.0/networks?name=private` |
| `openstack_compute_instance_v2.<vm>` | flavor per nome | `GET /compute/v2.1/{p}/flavors/detail` |
| | create | `POST /compute/v2.1/{p}/servers` (`block_device_mapping_v2`, `networks`, `security_groups`, `key_name`, `metadata`) → `202`; attesa `ACTIVE` con `GET …/servers/{id}` (`BUILD` → `ACTIVE`) |
| | read | `GET …/servers/{id}` (indirizzi, `accessIPv4`, stato, `OS-EXT-STS:power_state`, metadata), `GET …/servers/{id}/os-security-groups`, eventuali `os-interface` / `os-volume_attachments` |
| | update | `PUT …/servers/{id}` (nome), `POST|PUT …/servers/{id}/metadata`, `POST …/servers/{id}/action` (`addSecurityGroup`, `removeSecurityGroup`, `os-start`, `os-stop`) |
| | delete | `DELETE …/servers/{id}`, poi `GET` fino a `404` |
| `openstack_networking_floatingip_v2.<vm>` | create | risoluzione del pool `GET /network/v2.0/networks?name=public`, `POST /network/v2.0/floatingips` → `201` (o `409 IpAddressGenerationFailure` se il pool è pieno) |
| | read | `GET /network/v2.0/floatingips/{id}` |
| | delete | `DELETE /network/v2.0/floatingips/{id}`, poi `GET` fino a `404` |
| `openstack_compute_floatingip_associate_v2.<vm>` | create | azione Nova `POST …/servers/{id}/action` `{"addFloatingIp": {"address"}}` (**confermato**) |
| | read | `GET …/floatingips?floating_ip_address=…` e `GET …/servers/{id}` |
| | delete | azione Nova `removeFloatingIp` |
| | (anche) | `GET …/servers/{id}/tags` alla lettura di ogni VM |

### 5.4 Versioni

Il mock risponde anche alle richieste di *version discovery* (`GET /identity/v3`,
`GET /compute/`, `GET /network/`, `GET /image/`) con un documento di versioni minimo, nel caso il
provider le faccia.

### 5.5 Chiamate non previste

Ogni chiamata a un percorso OpenStack non implementato risponde `501` con un messaggio chiaro,
viene registrata in `/__mock/unhandled` e scritta nel log. Così, se il provider usa una chiamata
che non abbiamo previsto, il Job fallisce in modo riconoscibile e sappiamo esattamente cosa
aggiungere, invece di ricevere una risposta inventata.

## 6. Comportamenti realistici

Quanto basta perché i test dicano qualcosa di vero sul sistema reale.

| Comportamento | Default | Configurabile |
|---|---|---|
| Durata di `BUILD` di una VM | 3 s | `buildSeconds` |
| Durata della cancellazione di una VM (resta `ACTIVE`/`DELETING`, poi `404`) | 2 s | `deleteSeconds` |
| Router e porte | `ACTIVE` subito | `routerSeconds` |
| Floating IP | dal pool della subnet di `public`, `DOWN` finché non è associato, `ACTIVE` dopo | dimensione del pool |
| Gateway di un router | consuma un IP del pool (come su Virtuozzo) | |
| Pool pieno | `409 IpAddressGenerationFailure` su floating IP e gateway | |
| Subnet già collegata a un router | `409` sul secondo `add_router_interface` (il rischio R1) | |
| Router con floating IP che ne dipendono | `409` su `remove_router_interface` (come nel test 9.3) | |
| Nomi duplicati | ammessi, come in OpenStack (il rischio R5) | |
| Volume di avvio | creato con la VM, cancellato con lei (`delete_on_termination`) | |
| Indirizzi | IP privati dalla subnet di `private`, `accessIPv4` valorizzato | |
| `power_state` | `1` (running) o `4` (shutdown) con `os-stop` / `os-start` | |

## 7. Interfaccia di controllo e guasti

Tutto sotto `/__mock/`, in JSON. Si usa con `kubectl exec` o con un port-forward.

### 7.1 Inventario e stato

| Chiamata | Effetto |
|---|---|
| `GET /__mock/state` | Tutte le risorse finte, per tipo, con ID, nome, stato, IP |
| `GET /__mock/duplicates` | Nomi usati da più risorse dello stesso tipo (per scoprire i duplicati, R5) |
| `GET /__mock/requests?since=…` | Ultime richieste OpenStack con esito e guasto applicato |
| `GET /__mock/unhandled` | Chiamate non implementate ricevute (vedi 5.5) |
| `POST /__mock/reset` | Svuota tutto e ricarica i dati iniziali (simula "infrastruttura sparita") |
| `PUT /__mock/config` | Durate, dimensione del pool, password (vedi 6) |

### 7.2 Guasti

Una regola dice **quali richieste** colpire e **cosa fare**. Le regole sono valutate in ordine;
la prima che corrisponde si applica.

```json
POST /__mock/faults
{
  "name": "keystone-down",
  "match": {
    "service": "identity|compute|network|image",
    "method": "POST",
    "path": "regexp sul percorso, es. /servers$",
    "body": "regexp opzionale sul corpo, es. lt-vm-2"
  },
  "action": {
    "type": "status | delay | reset | hang | body",
    "status": 503,
    "body": "{\"NeutronError\": {...}}",
    "seconds": 30
  },
  "after": 0,
  "times": 3,
  "probability": 1.0,
  "expiresInSeconds": 120
}
```

| `action.type` | Effetto | Corrisponde a |
|---|---|---|
| `status` | Risponde con lo stato e il corpo indicati (401, 403, 404, 409, 500, 503…) | errori API |
| `delay` | Aspetta `seconds`, poi risponde normalmente | rete lenta |
| `reset` | Chiude la connessione senza rispondere | `REJECT`, `connection reset` |
| `hang` | Tiene la connessione aperta per `seconds` (o finché il client rinuncia) | `DROP` |
| `body` | Risponde `200` con un corpo malformato o incompleto | API con risposte rotte |

- `after`: lascia passare le prime N richieste che corrispondono (es. rompe il **terzo** `GET`
  della VM, cioè durante l'attesa di `ACTIVE`).
- `times`: quante volte applicarla prima che si disattivi (assente = sempre).
- `probability`: per guasti intermittenti (es. `0.5` per la rete che va e viene).
- `expiresInSeconds`: la regola sparisce da sola (es. interruzione di 20 s).

`GET /__mock/faults` le elenca con il numero di applicazioni; `DELETE /__mock/faults` le toglie
tutte, `DELETE /__mock/faults/{name}` una sola.

### 7.3 Guasti sulle risorse e drift

Modificano lo stato come farebbe Virtuozzo o una persona, senza passare da Terraform.

| Chiamata | Effetto |
|---|---|
| `POST /__mock/servers/{id|name}/error` | La VM va in `ERROR` (alla creazione o dopo) |
| `POST /__mock/servers/{id|name}/stuck` | La VM resta in `BUILD` (o in cancellazione) finché non viene sbloccata |
| `POST /__mock/servers/{id|name}/delete` | Cancellata "a mano" |
| `POST /__mock/servers/{id|name}/stop` | Spenta a mano |
| `POST /__mock/servers/{id|name}/remove-security-group` | Security group tolto a mano |
| `POST /__mock/routers/{id|name}/delete` | Router cancellato a mano |
| `POST /__mock/floatingips/{ip}/release` | Floating IP rilasciato a mano |
| `POST /__mock/next/servers/error` | La **prossima** VM creata andrà in `ERROR` |

## 8. Scenari di test

Ripetono quelli di [TEST-PLAN.md](TEST-PLAN.md) in modo deterministico, più quelli che sul reale
non si possono fare. Ogni scenario: regola o comando del mock, run, verifica su latch e su
`/__mock/state`.

| # | Scenario | Come si rompe il mock | Cosa verificare | Test reale |
|---|---|---|---|---|
| M1 | Percorso normale, idempotenza, destroy | nessun guasto | creazione, `No changes`, destroy completo, inventario vuoto | 1.1, 1.2, 13.1 |
| M2 | Keystone irraggiungibile | `reset` su `identity` | fallimento in `plan`, retry dell'istanza | 4.1 |
| M3 | Keystone appeso | `hang` su `identity` | tempo prima del fallimento, nessun lock | 4.2 |
| M4 | Credenziali sbagliate | `status 401` su `/auth/tokens` | `Authentication failed` | 4.3 |
| M5 | 5xx transitori | `status 503`, `times: 2` | il provider ritenta e riesce | — |
| M6 | Pool di IP pieno | pool da 2 indirizzi | `409` su floating IP; apply parziale salvato | 2.1 |
| M7 | Interruzione durante la creazione | `reset` sui `GET …/servers/{id}` con `after: 1` | VM nello state come `tainted`, retry senza duplicati | 5.1 |
| M8 | Interruzione silenziosa durante la creazione | `hang` sugli stessi `GET` | durata, timeout del Job, lock | 5.2, 12 |
| M9 | Interruzione breve | `reset` con `expiresInSeconds: 20` | il provider supera l'interruzione | 5.3 |
| M10 | Interruzione durante la cancellazione | `reset` sui `GET` dopo `DELETE` | state con risorse sparite (N2), retry | 5.4 |
| M11 | VM in `ERROR` | `next/servers/error` | errore chiaro, VM `tainted`, retry la sostituisce | — |
| M12 | VM bloccata in `BUILD` | `servers/{id}/stuck` | timeout del provider / del Job | 12 |
| M13 | Cancellazione che non finisce | `stuck` in cancellazione | destroy che fallisce, nessun lock | — |
| M14 | Due clienti sulla stessa subnet | due istanze | `409` sul secondo router (R1) | 5.5, 10.3 |
| M15 | Crash del Job a metà | SIGKILL a `tofu` durante `BUILD` (lungo, `buildSeconds: 30`) | lock orfano, duplicati visibili in `/__mock/duplicates` | 6.2 |
| M16 | State perso | cancellazione dello state su MinIO | duplicati in `/__mock/duplicates`; recupero per import | 8.1, 8.2 |
| M17 | Infrastruttura persa | `POST /__mock/reset` | il run ricrea tutto (state con risorse "deleted") | 8.5 al contrario |
| M18 | Drift | `delete`, `stop`, `remove-security-group` | correzione al run successivo; con `Manual`, plan pendente | 9.x, 11 |
| M19 | Risposte malformate | `body` con JSON troncato | errore del provider, nessuna modifica allo state | — |
| M20 | Rete intermittente | `reset` con `probability: 0.5` | come si comporta con la rete di oggi | incidente del 26/10 |
| M21 | `TemplateChanged` con Job attivo | `buildSeconds: 60`, modifica del WorkflowTemplate durante la creazione | il Workflow aspetta il Job (correzione D1) | 3.5c |

## Uso

```bash
kubectl apply -k config/samples/terraform-workflow/mock/       # Secret e WorkflowTemplate
kubectl create -f config/samples/terraform-workflow/mock/workflow.yaml
M=localhost:8088                                                # port-forward di Tilt verso il mock
curl -s $M/__mock/state | jq .openstack                         # risorse finte
curl -s $M/__mock/unhandled                                     # chiamate non previste
curl -s -X POST $M/__mock/faults -d '{"name":"wait-reset","match":{"service":"openstack","method":"GET","path":"/servers/[0-9a-f-]+$"},"action":{"type":"reset"},"times":20}'
curl -s -X PUT "$M/__mock/config?service=openstack" -d '{"floatingIPPool":2,"buildSeconds":30}'
curl -s -X POST $M/__mock/reset                                 # tutto da capo
```

Il mock si può anche avviare in locale (`go run ./cmd/mock-api`, porta `PORT`) e usare con
`tofu` sui file Terraform renderizzati: è il modo più rapido per provare una modifica del mock.

## 9. Piano di implementazione

- [x] **1. Cattura delle chiamate.** Catch-all OpenStack che registra tutto (5.5) e
      autenticazione minima; un run di `network` e `vm` con `TF_LOG=DEBUG` sul Job. Si ottiene
      l'elenco esatto delle chiamate del provider e si corregge la sezione 5.
- [x] **2. Keystone e catalogo** (5.1, 5.4).
- [x] **3. Neutron**: reti, subnet, router, interfacce, porte, floating IP, pool (5.2, 5.3, 6).
- [x] **4. Glance e Nova**: immagini, flavor, server con stati asincroni, azioni, associazione
      dei floating IP (5.3, 6).
- [x] **5. Controllo e guasti** (7): inventario, reset, configurazione, regole, drift.
- [x] **6. Test Go** del mock (`cmd/mock-api/*_test.go`): ogni handler, le regole di guasto, gli
      stati asincroni; nessuna dipendenza esterna.
- [x] **7. Build e deploy**: `Dockerfile.mock-api` con tutti i file; Tilt lo ricostruisce da solo.
- [x] **8. Manifest** in `config/samples/terraform-workflow/mock/`, generati (vedi 3).
- [x] **9. Validazione**: M1 completo nel cluster (latch, MinIO, Job) e in locale (creazione,
      `No changes`, drift, destroy) senza chiamate non previste. Provati in locale anche M6, M7, M11.
- [x] **10. Scenari** M2–M21: vedi [Esiti](#esiti).

## Esiti

Eseguiti il 27/10/2026 nel cluster `kind-er-operator`, con latch, MinIO e i Job veri, contro il
mock, cliente `ACME` (Workflow `mk-*`, poi rimossi).

| # | Esito | Note |
|---|---|---|
| M1 | ✅ | Creazione, `No changes`, destroy (`vm` poi `network`) e secondo destroy tutto `Skipped`; nessuna chiamata non prevista |
| M2 | ✅ nota | Keystone con connessione chiusa: fallimento in 11 s, ma **una sola** richiesta: il provider non ritenta le connessioni chiuse |
| M3 | ✅ nota | Keystone appeso 120 s: il provider aspetta tutto (nessun timeout per richiesta); un'API bloccata terrebbe il Job fino al suo timeout |
| M4 | ✅ | `Authentication failed` |
| M5 | ❌ provider | **Un solo 503** su `GET /networks` fa fallire lo step: il provider non ritenta i 503 |
| M6 | ✅ | Pool pieno: `IpAddressGenerationFailure`, VM salvata nello state senza floating IP (come il test reale 2.1). Corretto un difetto del mock (pool ristretto sotto gli indirizzi in uso) |
| M7 | ✅ | Connessioni chiuse durante l'attesa di `ACTIVE`: VM `tainted`, il retry la sostituisce, nessun duplicato |
| M8 | ✅ R3 | Richiesta appesa e timeout del Job: VM `tainted`, ma **il lock resta** (OpenTofu non chiude entro la grazia con una richiesta appesa) |
| M9 | ⚠️ | 20 s di connessioni chiuse: lo step fallisce; sul reale 20 s di `REJECT` erano stati superati (5.3). Il provider ritenta i "connection refused", non i reset: il `reset` del mock è più severo |
| M10 | ✅ | Colpito di passaggio in M7 e M9 (attesa della cancellazione): il run successivo chiude |
| M11 | ✅ | VM in `ERROR`: errore chiaro, `tainted`, sostituita al retry |
| M12 | ✅ nota | VM bloccata in `BUILD` e timeout: `tainted`, lock rilasciato. **Finché la VM resta in `BUILD`, ogni run fallisce già nel refresh** (`Invalid power_state … BUILD`) |
| M13 | ✅ | Cancellazione bloccata e timeout: lock rilasciato, la VM resta nello state; sbloccata, il run converge |
| M14 | ✅ R1 | Secondo cliente sulla subnet fissa: `409 IpAddressAlreadyAllocated`, router duplicato visibile in `/__mock/duplicates` |
| M15 | ✅ R3 R5 | SIGKILL durante `BUILD`: lock orfano; dopo lo sblocco VM duplicata, run `Succeeded`, duplicato visibile in `/__mock/duplicates` |
| M16 | ✅ R5 | State di `vm` perso: tutte le VM duplicate (12 risorse create), run `Succeeded`; ripristino dalla copia e pulizia |
| M17 | ✅ | Infrastruttura persa (pod del mock ricreato): tutto "has been deleted" e ricreato |
| M18 | ✅ | VM cancellata, VM spenta, security group tolto, router cancellato: un run corregge tutto |
| M19 | ✅ | Risposta malformata durante l'attesa: `unexpected EOF`, VM `tainted` |
| M20 | ❌ provider | 50 % di connessioni chiuse: 3 run su 3 falliscono già in `network` (come la rete reale del 26/10) |
| M21 | ✅ D1 | `TemplateChanged` con Job attivo: il Workflow resta `Running` finché il Job finisce, poi fallisce; nessun Job in più (correzione D1 verificata nel cluster) |

### Osservazioni

- **Il provider OpenStack non ritenta né i 503 né le connessioni chiuse** (M2, M5, M9, M20), e
  non ha un timeout per richiesta (M3). Un Virtuozzo lento o una rete instabile fanno fallire il
  run, che latch riprova solo con il `retryInterval` dell'istanza.
- **Un timeout del Job con una richiesta appesa lascia il lock** (M8): oltre al crash (M15), anche
  il timeout poteva bloccare tutti i run successivi. **Corretto il 27/10/2026**: l'operatore
  rimuove il lock di un runner che non è più attivo e lo registra nello status dello step
  (`releasedStateLock`); verificato nel cluster con un lock orfano (rimosso, run riuscito) e con
  il lock di un'altra persona (lasciato, run fallito sul lock).
- **Rimozione del lock resa prudente, e sospensione dopo la rimozione** (27/10/2026). Un lock
  orfano rimosso lascia nel cloud ciò che il runner aveva creato senza salvarlo (R5). Ora
  l'operatore rimuove un lock `runner@<pod>` solo se il Job di quel pod esiste, è di un Workflow
  ed è finito, e nessun pod con quel nome è vivo; se il nome è tagliato a 63 caratteri o il Job
  non c'è più (TTL di 24 h), il lock resta. Alla rimozione scrive accanto allo state
  `<state>.released-lock`, emette l'Event `StateLockReleased` sul Workflow, e da quel momento
  gli step Apply e ApplyPlan su quello state falliscono con reason `StateLockReleased` finché
  l'ID del lock non è in `spec.acceptStateHolds` (del Workflow o dell'istanza; il campo si
  chiamava `acceptReleasedStateLocks` nella prima versione); Plan e Destroy girano. L'istanza
  mostra `status.stateHold` e la condizione `StateHold=True`, e non riprova finché il lock non è
  accettato; accettato, parte
  subito. Verificato nel cluster con il mock (client `LOCKTEST2`):
  1. `hang` con `afterHandler` sulla `POST …/routers` (router creato, risposta persa), pod del
     runner forzato: Workflow `lt-lock-2` fallito con "the state lock it left was removed",
     state salvato vuoto (152 B) col router già nel mock, marker scritto, Event emesso;
  2. `lt-lock-3` senza accettazione: `Failed`/`StateLockReleased`, nessun Job creato;
  3. `lt-lock-4` con l'ID accettato: Event `StateLockAccepted`, marker cancellato, apply
     eseguito; il mock ha un `router-test` in più (il router orfano non è stato tolto perché il
     mock non implementa `DELETE …/routers/{id}`). L'apply è poi fallito su
     `IpAddressAlreadyAllocated` della subnet fissa condivisa fra i client (R1).
  Rischio residuo: `kubectl delete pod --force` toglie il pod dall'API mentre il processo può
  ancora girare sul nodo per qualche secondo; l'operatore lo considera finito.
- **Una VM rimasta in `BUILD` dopo un timeout blocca lo step** (M12) finché OpenStack non la porta
  in un altro stato.
- **R1, R3, R5 riprodotti in modo deterministico**; con `/__mock/duplicates` i duplicati sono
  visibili subito, mentre latch e Terraform li considerano un successo.
- **Creazioni non salvate e stop prima della scadenza** (27/10/2026, P1/P2 di TEST-PLAN.md).
  Verificato col mock:
  1. `reset` sulle `GET …/routers/{id}` dopo una `POST` riuscita (`p1-mock-1`): router creato,
     attesa fallita, fuori dallo state; il runner registra `network/terraform.tfstate.unsaved-resources`,
     Workflow `Failed`/`UnsavedResources`; `p1-mock-2` fermo senza Job; router cancellato e
     `acceptStateHolds` (`p1-mock-3`): Event `StateHoldAccepted`, record cancellato, run
     riuscito, nessun duplicato;
  2. `409` sulla `POST …/routers` (`p1-mock-409`): "The API refused to create …: nothing was
     created", `TerraformJobFailed`, nessuna sospensione;
  3. `delay` di 90 s sulla `POST …/servers` con `vm` a 120 s (`p2-mock-2`): a 90 s il runner
     interrompe OpenTofu, che salva la VM `tainted` e toglie il lock; "reached its time limit,
     before its timeout (120s)", nessun record (non manca nulla).
