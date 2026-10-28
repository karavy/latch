# Piano di test: step Terraform del workflow Virtuozzo

Test completo degli step `network` e `vm` del WorkflowTemplate `virtuozzo-vm`, eseguiti da latch
su Kind (`kind-er-operator`) contro Virtuozzo (OpenStack su `192.168.20.176`). Copre il percorso
normale, le modifiche a input e codice, gli errori di comunicazione, l'interruzione durante il
deploy, i retry, la perdita e la corruzione dello state, il lock, il drift, la concorrenza e il
destroy.

Ogni test ha l'obiettivo, i passi, il risultato atteso e le verifiche da spuntare. Dove il
comportamento non è ancora stato osservato è scritto **da osservare**: annotare cosa succede,
perché è proprio lì che il test serve. Le anomalie vanno nella sezione [Esiti](#esiti) in fondo.

## Indice

0. [Preparazione](#0-preparazione)
1. [Percorso normale](#1-percorso-normale)
2. [Modifiche agli input](#2-modifiche-agli-input)
3. [Modifiche al codice Terraform](#3-modifiche-al-codice-terraform)
4. [Errori di comunicazione prima del deploy](#4-errori-di-comunicazione-prima-del-deploy)
5. [Interruzione durante il deploy e retry](#5-interruzione-durante-il-deploy-e-retry)
6. [Interruzioni del cluster: Job, operatore, storage](#6-interruzioni-del-cluster-job-operatore-storage)
7. [Lock dello state](#7-lock-dello-state)
8. [Perdita e corruzione dello state](#8-perdita-e-corruzione-dello-state)
9. [Drift: modifiche fatte a mano su Virtuozzo](#9-drift-modifiche-fatte-a-mano-su-virtuozzo)
10. [Concorrenza](#10-concorrenza)
11. [Approvazione manuale dei plan](#11-approvazione-manuale-dei-plan)
12. [Timeout](#12-timeout)
13. [Destroy](#13-destroy)
14. [Pulizia finale](#14-pulizia-finale)

## Rischi noti, da confermare con i test

- **R1. Nomi fissi in `network.tf`.** Il router si chiama sempre `router-test` e l'interfaccia
  usa una subnet fissa (`a6c3e3aa-…`): due clienti (o `acme-vms` e l'istanza di test) si
  pestano i piedi. Per questo i test girano con `acme-vms` sospesa e le sue risorse cancellate.
- **R2. Nessun retry dentro un run.** Il Job ha `backoffLimit: 0`: un errore fa fallire il
  Workflow, e si riprova solo con il run successivo dell'istanza (`retryInterval`).
- **R3. Lock orfano.** Un Job ucciso senza che OpenTofu possa chiudere (SIGKILL, pod forzato,
  timeout) può lasciare il file `.tflock`: i run dopo falliscono finché non lo si toglie a mano.
- **R4. State non salvato.** Se lo storage S3 non è raggiungibile alla fine di un apply,
  OpenTofu scrive `errored.tfstate` nella cartella dello step, sul PVC del Workflow. Il PVC è
  del Workflow: sparisce quando il run esce dallo storico dell'istanza.
- **R5. State perso = risorse duplicate.** OpenStack accetta nomi duplicati: senza state un run
  ricrea router e VM accanto a quelli esistenti, senza errori.
  *Mitigato il 27/10/2026 per i lock rimossi dall'operatore*: dopo la rimozione di un lock
  orfano nessun apply tocca quello state finché il lock non è accettato
  (`acceptStateHolds`), così le risorse non salvate si controllano prima (vedi
  MOCK-OPENSTACK.md, Osservazioni). Uno state perso o cancellato resta non coperto.
- **R6. Nessuna versione dello state.** Il bucket MinIO non ha il versioning: uno state
  sovrascritto o cancellato non si recupera.
- **R7. I Secret non fanno partire run.** Cambiare credenziali o variabili nel Secret
  `virtuozzo-credentials` non cambia la revisione dell'istanza: l'effetto si vede solo al run
  successivo.

---

## 0. Preparazione

### 0.1 Ambiente

- [ ] Tilt avviato, tutte le risorse `ok`: `tilt get uiresources`
- [ ] Operatore senza errori: `kubectl -n latch-system logs deploy/latch-controller-manager -c manager --tail=100 | grep ERROR`
- [ ] MinIO su PVC (`minio-data`, `Bound`): `kubectl -n tfstate-storage get pvc`
- [ ] Virtuozzo raggiungibile dal nodo Kind:
      `docker exec er-operator-control-plane sh -c 'nc -zvw3 192.168.20.176 5000'`
- [ ] `acme-vms` sospesa e le sue risorse su Virtuozzo cancellate a mano (R1):
      `kubectl patch workflowinstance acme-vms --type merge -p '{"spec":{"suspend":true}}'`
- [ ] Accesso a Virtuozzo per le verifiche: CLI `openstack` con le credenziali di
      `virtuozzo-credentials`, oppure la console web

### 0.2 Istanza di test

Un'istanza dedicata, con un cliente e nomi di VM che non si confondono con il resto, e
intervalli brevi per non aspettare ore. **Approval `Auto`** salvo dove indicato.

```yaml
# Salvare in un file locale (non nel repository) e applicare con kubectl apply -f
apiVersion: dinova.one/v1alpha1
kind: WorkflowInstance
metadata:
  name: latch-test
  labels: {dinova.one/client: LATCHTEST}
spec:
  templateRef: {name: virtuozzo-vm}
  parameters:
    - {name: clientName, value: LATCHTEST}
    - {name: vms, value: '["lt-vm-1","lt-vm-2"]'}
  interval: 15m
  retryInterval: 3m
  approval: Auto
  successfulRunsHistoryLimit: 10
  failedRunsHistoryLimit: 10
```

Lo storico alto (10 + 10) tiene i Workflow, e quindi i loro PVC e i log dei Job, per tutta
la durata dei test.

### 0.3 Comandi di supporto

```bash
INST=latch-test
STATE=default/virtuozzo-vm/LATCHTEST          # prefisso degli state degli step
# Ultimo run dell'istanza
lastrun() { kubectl get workflow -l dinova.one/workflow-instance=$INST --sort-by=.metadata.creationTimestamp -o name | tail -1; }
# Stato del run: fase, condizione, step
runstatus() { kubectl get ${1:-$(lastrun)} -o jsonpath='{.metadata.name} {.status.phase}{"\n"}{range .status.conditions[*]}  {.type}={.status} {.reason}: {.message}{"\n"}{end}{range .status.templates[0].steps[*]}  {.name}: {.phase} | {.message}{"\n"}{end}'; }
# Log del Job di uno step dell'ultimo run (network o vm)
joblog() { kubectl logs job/$(kubectl get $(lastrun) -o jsonpath="{.status.templates[0].steps[?(@.name=='$1')].jobName}") ${2:-}; }
# Istanza: Ready, ultimo run, prossimo run
inst() { kubectl get workflowinstance $INST -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}: {.message}{"\n"}{end}lastRun={.status.lastRun} {.status.lastRunPhase} next={.status.nextRunTime}{"\n"}'; }
# mc dentro il pod MinIO (alias "local" da creare una volta)
mcx() { kubectl -n tfstate-storage exec deploy/minio -- mc "$@"; }
mcx alias set local http://localhost:9000 tfstate-dev tfstate-dev-secret
# State, lock, plan e file eseguiti del cliente di test
mcx ls -r local/tfstate/$STATE/
mcx cat local/tfstate/$STATE/vm/terraform.tfstate | jq '.serial, [.resources[] | .type + "." + .name]'
# Connettività verso Virtuozzo: blocco con risposta immediata (REJECT) o silenzioso (DROP)
vz_reject() { docker exec er-operator-control-plane iptables -I FORWARD 1 -d 192.168.20.176 -j REJECT; }
vz_drop()   { docker exec er-operator-control-plane iptables -I FORWARD 1 -d 192.168.20.176 -j DROP; }
vz_open()   { docker exec er-operator-control-plane sh -c 'while iptables -D FORWARD -d 192.168.20.176 -j REJECT 2>/dev/null || iptables -D FORWARD -d 192.168.20.176 -j DROP 2>/dev/null; do :; done'; }
# Risorse su Virtuozzo
vz_list() { openstack router list -f value -c Name; openstack server list -f value -c Name -c Status; }
```

> `vz_open` va sempre eseguito alla fine di ogni test di connettività, anche se il test va male.

### 0.4 Stato di partenza

- [ ] Nessuna risorsa `router-test`, `lt-vm-*` su Virtuozzo: `vz_list`
- [ ] Nessuno state per `LATCHTEST`: `mcx ls -r local/tfstate/$STATE/` vuoto
- [ ] Nessuna regola iptables residua: `docker exec er-operator-control-plane iptables -L FORWARD -n | grep 192.168.20.176` vuoto

---

## 1. Percorso normale

### 1.1 Prima creazione

**Obiettivo:** l'istanza crea rete e VM al primo run, nell'ordine `network` → `vm`.

1. `kubectl apply -f latch-test.yaml`
2. Seguire: `watch -n5 'runstatus; inst'`

**Atteso:** parte `latch-test-1`; `network` e poi `vm` arrivano a `Succeeded`; il run è
`Succeeded`; l'istanza `Ready=True Applied`.

- [ ] `vm` parte solo dopo che `network` è `Succeeded`
- [ ] Output `routerId` di `network` e `vmIps` di `vm` presenti nello status del run
- [ ] Su Virtuozzo: 1 router `router-test`, VM `lt-vm-1` e `lt-vm-2` `ACTIVE`
- [ ] Due state: `$STATE/network/terraform.tfstate` e `$STATE/vm/terraform.tfstate`
- [ ] Nessun `.tflock` rimasto
- [ ] File eseguiti su S3 per ogni step (`status…steps[].renderedFiles`), con `manifest.json`
      che riporta workflow, step, revisione del TerraformTemplate e chiave dello state
- [ ] I `.tf` renderizzati di `vm` contengono una risorsa per ogni VM della lista

### 1.2 Run senza modifiche (idempotenza)

**Obiettivo:** un run successivo senza modifiche non cambia niente.

1. Aspettare l'`interval` (15 min) oppure, con l'istanza sospesa, creare a mano un Workflow con
   gli stessi parametri.

- [ ] Il run è `Succeeded`
- [ ] Nei log: `No changes` per entrambi gli step (`joblog network`, `joblog vm`)
- [ ] Il `serial` degli state non cambia, oppure cambia senza risorse nuove
- [ ] Nessuna risorsa nuova su Virtuozzo

---

## 2. Modifiche agli input

### 2.1 Aggiungere una VM

`kubectl patch workflowinstance $INST --type merge -p '{"spec":{"parameters":[{"name":"clientName","value":"LATCHTEST"},{"name":"vms","value":"[\"lt-vm-1\",\"lt-vm-2\",\"lt-vm-3\"]"}]}}'`

- [ ] Parte subito un run (cambia la revisione), senza aspettare l'intervallo
- [ ] `network`: nessuna modifica
- [ ] `vm`: plan `1 to add`; `lt-vm-3` creata; `lt-vm-1` e `lt-vm-2` intatte
- [ ] `vmIps` contiene tre VM

### 2.2 Togliere una VM

Lista `["lt-vm-2","lt-vm-3"]`.

- [ ] `vm`: `lt-vm-1` distrutta (con il suo volume: `delete_on_termination`), le altre intatte
- [ ] Nessun volume orfano su Virtuozzo: `openstack volume list`

### 2.3 Rinominare una VM

Lista `["lt-vm-2","lt-vm-4"]` (`lt-vm-3` → `lt-vm-4`).

- [ ] Il plan distrugge `lt-vm-3` e crea `lt-vm-4` (il nome è anche l'indirizzo della risorsa)

### 2.4 Input non validi

- [ ] `vms` non JSON (`'[lt-vm-1'`): il run fallisce **prima** di creare il Job, con motivo
      `VariablesMappingFailed`; nessuna modifica su Virtuozzo
- [ ] Nome VM con caratteri non ammessi (`"lt vm"`): **da osservare** dove fallisce (render del
      template o plan) e con quale messaggio
- [ ] Parametro mancante (togliere `vms`): il run parte e fallisce subito con `InvalidTemplate`
      ("parameter … has no default"), senza Job; l'istanza è `Ready=False RunFailed`
- [ ] Ripristinare la lista valida e verificare che l'istanza torni `Applied`

---

## 3. Modifiche al codice Terraform

Le ConfigMap con il codice sono immutabili: si cambia creando una ConfigMap nuova e puntandoci
il TerraformTemplate.

### 3.1 Nuova revisione applicata da sola

1. Copiare `step-virtuozzo-vm-configmap.yaml` come `step-virtuozzo-vm-v2` e cambiare qualcosa di
   innocuo in `vm` (es. un `metadata` sulle VM: `metadata = { owner = "latch-test" }`).
2. Applicarla e far puntare il TerraformTemplate `virtuozzo-vm` alle chiavi della nuova
   ConfigMap.

- [ ] `kubectl get terraformtemplate virtuozzo-vm` passa alla revisione successiva
- [ ] L'istanza parte subito (cambia `status.terraformRevisions`)
- [ ] `vm`: `update in-place`, nessuna VM ricreata
- [ ] Lo step riporta la nuova `terraformRevision` e il nuovo `terraformDigest`
- [ ] Il `manifest.json` dei file eseguiti riporta la nuova revisione

### 3.2 Modifica che forza la ricreazione

Nella v3 cambiare `volume_size` del `block_device` (es. `var.volume_size_gb + 5`).

- [ ] Il plan mostra `must be replaced`: le VM vengono distrutte e ricreate
- [ ] **Da osservare:** durata, ordine destroy/create, IP cambiati in `vmIps`

### 3.3 Ritorno ai file di una revisione precedente

Far puntare di nuovo il TerraformTemplate alla ConfigMap della v1.

- [ ] La revisione torna quella della v1 (rollback), non una nuova
- [ ] Il run riporta le VM al codice della v1

### 3.4 Codice con errori

- [ ] Errore di sintassi in `network.tf`: il Job fallisce in `init`/`plan`, nessuna modifica su
      Virtuozzo, state invariato
- [ ] Riferimento a una variabile inesistente: stesso comportamento
- [ ] Placeholder di loop malformato in `compute.tf.tmpl` (`<FGT_FOR_STR>` senza fine): **da
      osservare**: errore nel render, prima di Terraform
- [ ] Correggere il codice: il run successivo converge

### 3.5 Regole su ConfigMap e template

- [ ] ConfigMap **non** immutabile puntata dal TerraformTemplate: rifiutata, motivo nello status
      del TerraformTemplate; l'istanza non parte
- [ ] ConfigMap cancellata mentre il TerraformTemplate la usa: **da osservare** cosa dice lo
      status, e che un run già partito usi la sua copia
- [ ] Modifica del WorkflowTemplate `virtuozzo-vm` **mentre un run è in corso** (es. cambiare
      `timeoutSeconds`): il run fallisce con `TemplateChanged`, il successivo usa la versione
      nuova

---

## 4. Errori di comunicazione prima del deploy

### 4.1 Virtuozzo irraggiungibile, risposta immediata

1. `vz_reject`
2. Far partire un run (modificare un parametro, o aspettare l'intervallo)

- [ ] `network` fallisce in `plan` con un errore di connessione; il run è `Failed`
      (`TerraformJobFailed`); `vm` resta `Pending`
- [ ] Nessuna modifica allo state; nessun `.tflock` rimasto
- [ ] L'istanza è `Ready=False RunFailed` e il prossimo run è a `lastRunTime + retryInterval` (3 min)
- [ ] `vz_open`: il run successivo (retry) converge

### 4.2 Virtuozzo irraggiungibile, pacchetti persi

Come 4.1 con `vz_drop`.

- [ ] **Da osservare:** quanto tempo passa prima che il provider rinunci (retry interni di
      gophercloud) e se si arriva al timeout del Job
- [ ] Il Job finisce comunque, con un errore e senza lock rimasto
- [ ] `vz_open` e verifica del retry

### 4.3 Credenziali sbagliate

1. Cambiare `TF_VAR_os_password` nel Secret `virtuozzo-credentials`.

- [ ] **Nessun run parte da solo** (R7)
- [ ] Al run successivo: errore di autenticazione (401) in `plan`, nessuna modifica
- [ ] Ripristinare la password: il run successivo converge

### 4.4 Endpoint sbagliato

`TF_VAR_os_auth_url` con porta o host errati.

- [ ] Errore di connessione chiaro nei log, run `Failed`

### 4.5 Registry dei provider irraggiungibile

Ogni run ha un PVC nuovo, quindi scarica di nuovo i provider.

1. Bloccare l'uscita verso il registry dal nodo Kind (`registry.opentofu.org` e il dominio da cui
   scarica i pacchetti, da ricavare dai log di `init`).

- [ ] Il Job fallisce in `init`, prima di toccare Virtuozzo e lo state
- [ ] Sbloccare: il retry converge

### 4.6 Storage dello state irraggiungibile prima del run

`kubectl -n tfstate-storage scale deploy/minio --replicas=0`, poi far partire un run.

- [ ] Il Job fallisce subito (salvataggio dei file eseguiti o `init`), senza toccare Virtuozzo
- [ ] `scale --replicas=1`: il retry converge

---

## 5. Interruzione durante il deploy e retry

Il caso più delicato: Virtuozzo sta creando risorse e la connessione cade. Serve una creazione
lunga, per avere il tempo di intervenire: le VM (avvio da volume).

Preparazione comune: lista con una VM nuova (es. aggiungere `lt-vm-5`), poi seguire i log:

```bash
joblog vm -f | grep -m1 'openstack_compute_instance_v2.*Creating' && vz_reject   # (o vz_drop)
```

### 5.1 Interruzione con risposta immediata (REJECT) durante la creazione di una VM

- [ ] Il Job fallisce; il run è `Failed`
- [ ] **Da osservare e annotare:**
  - [ ] la VM esiste su Virtuozzo? in che stato (`BUILD`, `ACTIVE`, `ERROR`)?
  - [ ] lo state di `vm` la contiene? (`mcx cat … | jq`) marcata `tainted`?
  - [ ] il volume di avvio esiste?
- [ ] Nessun `.tflock` rimasto (OpenTofu è uscito da solo)
- [ ] `vz_open`, attendere il retry:
  - [ ] se la VM era nello state: il retry la completa o la ricrea (`tainted`), senza duplicati
  - [ ] se **non** era nello state: il retry ne crea una seconda con lo stesso nome (R5) →
        segnarlo come difetto e pulire a mano

### 5.2 Interruzione silenziosa (DROP) durante la creazione

- [ ] **Da osservare:** OpenTofu resta in attesa; si arriva al timeout del provider o al
      `timeoutSeconds` del Job (1800 s)? (Per accorciare: vedi [12](#12-timeout).)
- [ ] Se interviene il timeout del Job: lock rimasto (R3)? state salvato?
- [ ] Recupero: `vz_open`, eventuale rimozione del lock ([7.2](#72-lock-orfano-e-sblocco)), retry

### 5.3 Interruzione breve (connessione che torna da sola)

`vz_reject; sleep 20; vz_open` durante la creazione.

- [ ] **Da osservare:** il provider riprova da solo e lo step riesce, oppure fallisce e
      converge al retry

### 5.4 Interruzione durante il destroy di una VM

Togliere una VM dalla lista e bloccare la connessione durante `Destroying...`.

- [ ] Run `Failed`; la VM resta nello state
- [ ] `vz_open`: il retry completa la distruzione

### 5.5 Interruzione nello step network

Con lo state vuoto (cliente nuovo, es. `LATCHTEST2`), bloccare dopo `openstack_networking_router_v2.r: Creating...`.

- [ ] **Da osservare:** router creato senza interfaccia; il retry aggiunge solo l'interfaccia
- [ ] `vm` non parte finché `network` non riesce

---

## 6. Interruzioni del cluster: Job, operatore, storage

### 6.1 Pod del Job cancellato normalmente durante l'apply

`kubectl delete pod <pod del Job vm>` durante la creazione di una VM.

- [ ] Il runner riceve SIGTERM: **da osservare** se OpenTofu fa in tempo a salvare lo state e
      togliere il lock nei 30 s di grazia
- [ ] Il Job fallisce (`backoffLimit: 0`), il run è `Failed`
- [ ] Retry: converge? duplicati?

### 6.2 Pod del Job cancellato forzatamente

`kubectl delete pod <pod> --grace-period=0 --force`

- [ ] Lock rimasto (R3): il retry fallisce con `Error acquiring the state lock`
- [ ] Sblocco ([7.2](#72-lock-orfano-e-sblocco)) e retry
- [ ] Risorse create dopo l'ultimo salvataggio dello state → orfane? (R5)

### 6.3 Operatore riavviato durante un Job

`kubectl -n latch-system delete pod -l control-plane=controller-manager` mentre `vm` è in corso.

- [ ] Il Job continua; il nuovo operatore riprende a seguirlo
- [ ] Il run finisce `Succeeded` con gli output corretti, senza un secondo Job

### 6.4 Operatore fermo per tutta la durata di un Job

`kubectl -n latch-system scale deploy/latch-controller-manager --replicas=0`, attendere la fine del
Job, poi `--replicas=1`.

- [ ] L'operatore legge il Job concluso e completa lo step
- [ ] Nessun run nuovo parte mentre l'operatore era fermo; alla ripartenza l'istanza ricalcola
      il prossimo run

### 6.5 Storage dello state irraggiungibile alla fine dell'apply (R4)

Durante la creazione di una VM: `kubectl -n tfstate-storage scale deploy/minio --replicas=0`.
Lasciare finire la creazione.

- [ ] Il Job fallisce nel salvataggio dello state
- [ ] **Da verificare:** `errored.tfstate` nella cartella dello step sul PVC del run
      (`status.templates[0].workspacePVCName`, cartella `vm/`): montare il PVC in un pod di
      debug e copiarlo
- [ ] `scale --replicas=1`; recupero dello state con `tofu state push errored.tfstate` (da un pod
      con il backend configurato), **prima** del retry
- [ ] Retry: nessun duplicato
- [ ] Ripetere **senza** il recupero: il retry crea duplicati (R5) → conferma che il recupero
      è necessario e va documentato

### 6.6 PVC del run non disponibile

Riempire la quota (`workspaceStorage` molto piccolo, es. `1Mi`, su un Workflow manuale).

- [ ] **Da osservare:** il Job fallisce in `init` (provider non scaricabili) o resta `Pending`;
      il messaggio nel run è chiaro?

---

## 7. Lock dello state

### 7.1 Lock rispettato

Durante un run lungo, lanciare a mano un Workflow con gli stessi parametri ([10.1](#101-workflow-manuale-sullo-stesso-state)).

- [ ] Il secondo fallisce con `Error acquiring the state lock`; il primo non ne risente

### 7.2 Lock orfano e sblocco

Creare un lock finto: `echo '{}' | kubectl -n tfstate-storage exec -i deploy/minio -- mc pipe local/tfstate/$STATE/vm/terraform.tfstate.tflock`

- [ ] Il run successivo fallisce con `Error acquiring the state lock`, con l'ID del lock nei log
- [ ] Sblocco: `mcx rm local/tfstate/$STATE/vm/terraform.tfstate.tflock`
- [ ] Il retry converge
- [ ] Annotare il nome esatto del file di lock creato da OpenTofu in 6.2, e confrontarlo con questo

---

## 8. Perdita e corruzione dello state

### 8.1 State di `vm` cancellato (R5)

`mcx rm local/tfstate/$STATE/vm/terraform.tfstate`, poi far partire un run.

- [ ] **Atteso, ed è un difetto:** il plan crea di nuovo tutte le VM; su Virtuozzo ci sono VM
      duplicate con lo stesso nome
- [ ] Recupero A, import: per ogni VM, `tofu import 'openstack_compute_instance_v2.lt-vm-1' <id>`
      da un pod con il codice renderizzato (prenderlo dai file eseguiti su S3) e il backend
- [ ] Recupero B, pulizia: cancellare a mano le VM originali (o le duplicate) e lasciare
      convergere
- [ ] Annotare quale recupero è praticabile e quanto costa: serve una procedura scritta

### 8.2 State di `network` cancellato

- [ ] Router duplicato (`router-test` due volte); **da osservare** se l'interfaccia sulla subnet
      fissa fallisce perché la subnet è già collegata (R1)
- [ ] `vm` legge `routerId` dal nuovo router: coerenza da verificare

### 8.3 State corrotto

`echo 'not json' | … mc pipe local/tfstate/$STATE/vm/terraform.tfstate`

- [ ] Il Job fallisce in `init`/`plan` con un errore di lettura dello state, nessuna modifica
- [ ] Un Workflow `Destroy` fallisce con `StateInvalid` senza lanciare Job
- [ ] Ripristino da una copia salvata prima (`mcx cp` in un altro percorso prima del test)

### 8.4 State vecchio rimesso al posto di quello nuovo

Salvare lo state dopo 2.1 (tre VM), proseguire fino a 2.2, poi rimettere quello vecchio.

- [ ] **Da osservare:** OpenTofu rifiuta per `serial`/`lineage`, oppure pianifica di ricreare
      `lt-vm-1` e di aggiornare le altre
- [ ] Conclusione su R6: serve il versioning del bucket?

### 8.5 Bucket intero perso

Simula la perdita del PVC di MinIO (vedi incidente del 26/10): svuotare il prefisso del cliente
di test.

- [ ] Stesso comportamento di 8.1 e 8.2 insieme; procedura di recupero completa annotata

---

## 9. Drift: modifiche fatte a mano su Virtuozzo

### 9.1 VM cancellata a mano

`openstack server delete lt-vm-2`, poi attendere l'intervallo.

- [ ] Il run successivo ricrea `lt-vm-2`; `vmIps` aggiornato

### 9.2 VM spenta a mano

`openstack server stop lt-vm-2`

- [ ] **Da osservare:** il provider non gestisce lo stato di accensione, quindi nessuna
      modifica attesa; annotare

### 9.3 Interfaccia del router rimossa a mano

- [ ] Il run successivo la ricrea in `network`

### 9.4 Risorsa modificata a mano (es. security group della VM)

- [ ] Il plan riporta la VM al codice

### 9.5 Drift con approvazione manuale

Come 9.1 con `approval: Manual` ([11](#11-approvazione-manuale-dei-plan)).

- [ ] Nessuna correzione automatica: l'istanza passa a `Drifted=True` con il plan da approvare

---

## 10. Concorrenza

### 10.1 Workflow manuale sullo stesso state

Con l'istanza in esecuzione, creare un Workflow `virtuozzo-vm` con `clientName: LATCHTEST`.

- [ ] Uno dei due fallisce sul lock (7.1), l'altro conclude; nessuna risorsa doppia

### 10.2 Due istanze con lo stesso cliente

- [ ] Stesso risultato del 10.1, a ogni intervallo: segnalarlo come configurazione da vietare

### 10.3 Due clienti diversi in parallelo (R1)

Istanza `latch-test-2` con `clientName: LATCHTEST2`.

- [ ] **Atteso, ed è un difetto:** due `router-test`; l'interfaccia sulla subnet fissa fallisce
      per il secondo cliente → `network.tf` va parametrizzato prima dell'uso reale

---

## 11. Approvazione manuale dei plan

Passare l'istanza ad `approval: Manual`.

- [ ] Nessuna modifica: run `Plan`, istanza `Ready=True`, `Drifted=False`
- [ ] Con una modifica (es. aggiungere una VM): `Drifted=True`, `status.pendingPlan` con lo step
      e i conteggi; plan leggibile nel Secret indicato
- [ ] Approvazione (`spec.approvePlan`): parte un `ApplyPlan` che applica **esattamente** quel
      plan, poi un nuovo `Plan` per lo step successivo
- [ ] Plan "stale": dopo il plan e prima dell'approvazione, cambiare lo state (es. cancellare una
      VM a mano e far girare un `Apply` manuale) → l'`ApplyPlan` fallisce senza toccare niente
- [ ] Approvazione di un ID sbagliato: ignorata, il messaggio di `Drifted` lo dice
- [ ] I file del plan su S3 (`default/plans/…`) spariscono dopo l'apply e quando il run `Plan`
      viene tolto dallo storico
- [ ] Interruzione di connettività durante l'`ApplyPlan` (come 5.1): **da osservare** se il
      plan approvato viene riproposto o se serve un nuovo plan

---

## 12. Timeout

Su una copia del WorkflowTemplate (`virtuozzo-vm-timeout`) con `timeoutSeconds: 120` sullo step
`vm`, lanciata a mano con un cliente dedicato.

- [ ] Una creazione più lunga di 120 s viene interrotta (`activeDeadlineSeconds`), il run è
      `Failed`
- [ ] **Da osservare:** lock rimasto? state salvato? VM orfana?
- [ ] Combinato con `vz_drop` (5.2): il timeout chiude un Job bloccato

---

## 13. Destroy

### 13.1 Workflow Destroy completo

Workflow con `terraformMode: Destroy` e i parametri del cliente di test.

- [ ] Prima passata: `network` e `vm` leggono gli output (`Pending`, nessun Job)
- [ ] Seconda passata: Job `destroy` su `vm`, poi su `network`
- [ ] Su Virtuozzo non resta niente di `LATCHTEST`; gli state restano, senza risorse
- [ ] Un secondo Workflow Destroy: entrambi gli step `Skipped` ("state has no resources"), nessun Job

### 13.2 Destroy con connettività interrotta

`vz_reject` durante il destroy di `vm`.

- [ ] Il Destroy fallisce; `network` non viene toccato
- [ ] `vz_open`; un nuovo Workflow Destroy completa il lavoro

### 13.3 Destroy con state parzialmente perso

Cancellare lo state di `network`, poi Destroy.

- [ ] `network` `Skipped` ("no state"); `vm` legge `net_id`? Se le sue variabili dipendono da
      `network`, il destroy di `vm` fallisce invece di indovinare (comportamento voluto)
- [ ] Il router resta orfano su Virtuozzo: annotarlo

### 13.4 Istanza con `deletionPolicy: Destroy`

1. `kubectl patch workflowinstance $INST --type merge -p '{"spec":{"deletionPolicy":"Destroy"}}'`
2. `kubectl delete workflowinstance $INST` (con un run in corso, per il primo controllo)

- [ ] `Ready=False WaitingToDestroy` finché il run in corso non finisce
- [ ] Parte `latch-test-destroy-<uid>`, `status.destroyRun` lo riporta, `Ready` è `Destroying`
- [ ] Con `vz_reject`: `DestroyFailed`, l'istanza resta; `vz_open`, cancellare il Workflow di
      destroy → ne riparte uno, che riesce → l'istanza sparisce
- [ ] Il Workflow di destroy resta dopo la cancellazione dell'istanza
- [ ] Variante: dopo `DestroyFailed`, `deletionPolicy: Retain` → l'istanza sparisce, le risorse restano

---

## 14. Pulizia finale

- [ ] `vz_open` e nessuna regola iptables residua
- [ ] MinIO e operatore a 1 replica
- [ ] Istanze di test cancellate; Workflow di test cancellati
- [ ] Nessuna risorsa `router-test` / `lt-vm-*` su Virtuozzo, nessun volume orfano
- [ ] State, lock e plan dei clienti di test rimossi: `mcx rm -r --force local/tfstate/default/virtuozzo-vm/LATCHTEST/` (e `LATCHTEST2`)
- [ ] Copie di ConfigMap/TerraformTemplate di test rimosse; `virtuozzo-vm` riportato alla
      revisione di partenza
- [ ] `acme-vms` riattivata solo dopo aver deciso cosa fare dei rischi R1 e R5

---

## Esiti

Campagna del 26/10/2026 (19:00–20:50 UTC), istanza `latch-test`, cliente `LATCHTEST`, run
`latch-test-1`…`latch-test-69` più Workflow manuali `lt-*`. Sospesa alla sezione 13 per un
problema della rete locale (vedi in fondo). Ripresa il 27/10/2026 (16:55–17:25 UTC) con la rete
stabile: run `latch-test-70`…`latch-test-79`, istanza `latch-test-2` (cliente `LATCHTEST2`) e
Workflow `lt-*`/`lt2-*`; rifatti anche i test toccati dalle correzioni (righe "bis"). Le caselle
dei singoli test non sono spuntate: fa fede questa tabella.

| Test | Data | Esito | Note / difetto trovato |
|---|---|---|---|
| 1.1 | 26/10 | ✅ | `network` 26 s poi `vm` 20 s; state, output, file eseguiti e manifest corretti. VM create in ~11 s |
| 1.2 | 26/10 | ✅ | `No changes`, serial invariato |
| 2.1 | 26/10 | ❌ infra | Pool dei floating IP della rete `public` esaurito (`IpAddressGenerationFailure`). Apply parziale corretto: VM salvata nello state, nessun lock |
| 2.2 | 26/10 | ✅ | VM e volume distrutti, nessun orfano |
| 2.3 | 27/10 | ✅ | Rilasciati i FIP orfani `.224`/`.230`. `lt-vm-3` → `lt-vm-4`: plan 3 add / 3 destroy (istanza, FIP, associazione), `lt-vm-3` e il suo volume cancellati, nessun orfano |
| 2.4 | 26/10 | ✅ note | `vms` non JSON: `VariablesMappingFailed` senza Job ma messaggio generico; nome con spazio: errore solo nel Job (`Invalid resource name`); parametro mancante: `InvalidTemplate` chiaro |
| 3.1 | 26/10 | ✅ | Revisione 2, run automatico, update in-place; revisione/digest in status e manifest |
| 3.2 | 26/10 | ✅ | Ricreazione in 42 s (destroy poi create), FIP conservati, volumi vecchi cancellati |
| 3.3 | 26/10 | ✅ | Ritorno ai file v1 = revisione 1 (nessuna revisione nuova) |
| 3.4 | 26/10 | ✅ note | Sintassi: `init` fallisce; variabile inesistente: `plan` fallisce; loop malformato: errore di render chiaro. Nota: `renderedFiles` punta a un prefisso vuoto se il render fallisce; due modifiche ravvicinate a due TerraformTemplate producono un run in più |
| 3.5c bis | 27/10 | ✅ D1 | WorkflowTemplate modificato durante il Job `vm` di `latch-test-72`: il run resta `Running` con `TemplateChanged` ("fails once Terraform Job … has finished"), fallisce alla fine del Job; `latch-test-73` nasce nello stesso secondo, nessun Job in parallelo, `No changes`. Nota N3 |
| 3.5 | 26/10 | ✅ + D1 🔧 | ConfigMap mutabile e cancellata: bloccate con motivo chiaro. ConfigMap cancellata a run partito: lo step successivo fallisce (la copia si fa all'avvio dello step). `TemplateChanged` ok, ma vedi **D1** |
| 4.1 | 26/10 | ✅ | `REJECT`: fallisce in 11 s; retry esattamente a +3 min, converge |
| 4.2 | 26/10 | ✅ | `DROP`: fallisce in ~2 min 20 s (timeout TCP), prima del timeout del Job |
| 4.3 | 26/10 | ✅ | Il cambio del Secret non avvia run (R7); `Authentication failed` al run dopo |
| 4.4 | 26/10 | ✅ | Endpoint sbagliato: errore chiaro in 11 s |
| 4.5 | 26/10 | ✅ | Registry irraggiungibile: `init` fallisce, nessuna modifica |
| 4.6 | 26/10 | ✅ | MinIO giù: fallisce nel salvataggio dei file eseguiti, prima di `init` |
| 5.1 | 26/10 | ✅ | Interruzione in creazione: VM create salvate `tainted` con ID, nessun lock; il retry le sostituisce, nessun duplicato |
| 5.2 | 26/10 | ✅ | Come 5.1 con `DROP`: fallisce dopo ~3 min 20 s |
| 5.3 | 26/10 | ✅ | Interruzione di 20 s superata dai retry del provider |
| 5.4 | 26/10 | ✅ note | Interruzione in destroy: VM cancellata ma rimasta nello state; il retry dice `No changes` e **non aggiorna lo state** (N2) |
| 5.5 | 27/10 | ⚠️ R5 | Diverso dal previsto: `REJECT` 2 s dopo `router_v2.r: Creating...`: router creato su Virtuozzo ma **non nello state** (nemmeno `tainted`): il provider salva l'ID del router solo dopo l'attesa di `ACTIVE`. Il retry voleva crearne un secondo (`2 to add`), fermato solo dal pool esaurito. Vedi **P1** |
| 6.1 | 26/10 | ✅ | Pod cancellato: SIGTERM, state salvato `tainted`, lock rilasciato |
| 6.2 | 26/10 | ✅ R3 R5 | `--force` non basta (il kubelet manda SIGTERM). Con SIGKILL a `tofu`: **lock orfano** e **state non aggiornato**; dopo lo sblocco il retry crea **VM duplicate** senza alcun errore |
| 6.2 bis | 27/10 | ✅ R3 R5 | Col nuovo comportamento: SIGKILL a `tofu` durante `lt-vm-6: Creating...` (`latch-test-75`): VM su Virtuozzo ma non nello state; lock rimosso dall'operatore, marker `vm/terraform.tfstate.released-lock`, Event `StateLockReleased`. Il retry `latch-test-76` si ferma con `StateLockReleased` senza Job; l'istanza mostra `stateLockHold` e non riprova. Cancellata a mano la VM orfana, `acceptReleasedStateLocks` sull'istanza → run subito, `lt-vm-6` creata **una volta** (poi pool FIP esaurito, infrastruttura) |
| 6.3 | 26/10 | ✅ | Operatore riavviato durante il Job: run completato, un solo Job |
| 6.4 | 26/10 | ✅ | Operatore fermo per tutto il Job: completato alla ripartenza |
| 6.5 | 26/10 | ✅ R4 | MinIO giù a fine apply: `errored.tfstate` sul PVC del run, lock orfano; recupero riuscito (sblocco + `tofu state push`) |
| 6.6 | — | ➖ | Non verificabile su Kind: `local-path` ignora la dimensione del PVC |
| 7.1 | 26/10 | ✅ | Due run sovrapposti: il secondo fallisce sul lock |
| 7.2 | 26/10 | ✅ note | Lock orfano reale: messaggio solo `412 PreconditionFailed` (chi/quando sono nel file `.tflock`); senza intervento l'istanza fallisce ogni 3 min all'infinito |
| 8.1 | 26/10 | ✅ note | Recupero per import riuscito, ma l'import non legge `block_device.volume_type`: senza correggere lo state le VM verrebbero ricreate |
| 8.2 | 26/10 | ✅ note | Senza state di `network` il plan crea un secondo router (bloccato solo dal pool esaurito); ripristino da copia ok |
| 8.3 | 26/10 | ✅ | State corrotto: `init` fallisce; Destroy si ferma con `StateInvalid` senza Job |
| 8.4 | 26/10 | ✅ R6 | State di un altro lineage accettato senza avvisi |
| 8.5 | 26/10 | ➖ | Coperto da 8.1 + 8.2 (e dall'incidente reale del 26/10) |
| 9.1 | 26/10 | ✅ | VM cancellata a mano ricreata dal run d'intervallo |
| 9.2 | 26/10 | ⚠️ | Diverso dal previsto: il provider gestisce `power_state` e **riaccende** una VM spenta a mano |
| 9.3 | — | ➖ | Non eseguibile: Neutron rifiuta di togliere l'interfaccia (i FIP ne dipendono) |
| 9.4 | 26/10 | ✅ | Security group tolto a mano rimesso in-place |
| 9.5 | 26/10 | ✅ | Con `Manual` il drift resta un plan da approvare |
| 10.1 | 26/10 | ✅ | = 7.1 |
| 10.2 | — | ➖ | Coperto da 10.1 |
| 10.3 | 27/10 | ✅ R1 | Difetto confermato: secondo `router-test` creato per `LATCHTEST2`, interfaccia rifiutata `IpAddressAlreadyAllocated 192.168.128.254` (subnet fissa) |
| 11 | 26/10 | ✅ | Plan senza modifiche, plan pendente, ID sbagliato ignorato, plan stale rifiutato, apply esatto + nuovo plan, file del plan cancellati; interruzione durante `ApplyPlan`: serve una nuova approvazione |
| 12 | 26/10 | ✅ note | Timeout del Job (`DeadlineExceeded`): state salvato `tainted`, lock rilasciato; il Workflow non dice che è un timeout |
| 12 bis | 27/10 | ✅ nota | `DROP` durante la creazione con timeout 60 s: "Terraform Job exceeded its timeout (60s)". Al fallimento il pod terminava ancora: lock lasciato (giusto); riaperta la rete nella grazia, OpenTofu ha salvato la VM e tolto il lock da solo. **Ma** un FIP creato in quella chiusura non è finito nello state (orfano, nessuna sospensione): vedi **P2** |
| 13.2 | 26/10 | ✅ | Destroy interrotto: `network` non toccato |
| 13.1 | 26/10 | 🔧 D2 | Il Destroy dopo un destroy interrotto falliva con `OutputMissing`: **corretto**, `vm` distrutto. `network` non distrutto per la rete reale |
| 5.5 ter | 27/10 | ✅ P1 | Con la correzione: router `d35a8e51` creato e fuori dallo state, registrato; `Failed`/`UnsavedResources` con indirizzo e ID; il run dopo si ferma senza Job; router cancellato a mano, `acceptStateHolds` → run riuscito con un solo router |
| 12 ter | 27/10 | ✅ P2 | Con la correzione (`vm` a 120 s, `DROP` durante la creazione): a 90 s il runner ferma OpenTofu, lock tolto, VM salvata `tainted`; FIP `.225` creato e fuori dallo state, registrato; "reached its time limit, before its timeout (120s)"; FIP cancellato a mano, `acceptStateHolds` → plan 3 add / 1 destroy, nessun orfano |
| 13.1 | 27/10 | ✅ | Completato: `lt-destroy-7` distrugge `network` (`vm` Skipped, state vuoto); `lt-destroy-8` tutto `Skipped`, nessun Job |
| 13.3 | 27/10 | ✅ | State di `network` cancellato: `network` Skipped ("no state"), `vm` distrutto lo stesso (le sue variabili di destroy non dipendono da `network`), router orfano su Virtuozzo come previsto |
| 13.4 | 27/10 | ✅ | `WaitingToDestroy` durante `latch-test-79`; `latch-test-destroy-88db1222` in `status.destroyRun`, `Destroying`; con `REJECT` `DestroyFailed` e l'istanza resta; cancellato il Workflow di destroy, ne riparte uno che riesce, l'istanza sparisce, il Workflow di destroy resta. Variante su `latch-test-2`: dopo `DestroyFailed`, `Retain` → istanza sparita, router e state restano |
| 14 | 27/10 | ✅ | Nessuna regola iptables; MinIO e operatore a 1 replica; istanze e Workflow di test cancellati; nessuna risorsa `router-test`/`lt-vm-*`/volume di test su Virtuozzo; state di `LATCHTEST`/`LATCHTEST2` rimossi; ConfigMap di test e `virtuozzo-vm-timeout` cancellati, `virtuozzo-vm` alla revisione 1. `acme-vms` resta sospesa (R1, R5) |

### Difetti e azioni

- [x] **D1. Job orfano dopo un Workflow fallito.** Con `TemplateChanged` il Workflow falliva
      mentre il suo Job Terraform continuava; l'istanza partiva subito con un run nuovo, e i due Job
      giravano in parallelo sullo stesso state (osservato: run 18 e 19). Corretto in
      `internal/controller/workflow/orchestrate.go`: un Workflow che non può proseguire (template
      modificato, cancellato o non valido) aspetta la fine del Job in corso, senza fermarlo, e
      intanto resta `Running` con il motivo nel `Ready`; poi fallisce. Envtest aggiunto. Da
      ripetere sul cluster il test 3.5c.
- [x] **D2. Destroy bloccato da output mancanti.** Corretto in
      `internal/controller/workflow/destroy.go` (si pubblicano solo gli output presenti), con
      envtest.
- [x] **R3. Lock orfano dopo un crash o un timeout** (SIGKILL, OOM, nodo perso, timeout del Job
      con una richiesta appesa). Corretto il 27/10/2026: l'operatore rimuove il lock scritto dal
      runner di latch (`runner@<pod>`) quando quel pod non è più attivo, prima di avviare uno step
      e quando un Job fallisce, e lo registra in `status…steps[].releasedStateLock`. I lock di
      chiunque altro restano. Il messaggio di un Job fermato dal timeout ora lo dice.
- [ ] **R5. Duplicati dopo un crash.** Togliere il lock non ripara lo state: le risorse create
      dopo l'ultimo salvataggio non ci sono, e il run successivo può crearle di nuovo. *Mitigato il
      27/10 per i lock rimossi dall'operatore (6.2 bis) e per le creazioni non salvate (P1, P2):
      sospensione fino ad `acceptStateHolds`*. Resta scoperto lo state perso o cancellato (8.x).
- [x] **P1. Router fuori dallo state se l'attesa fallisce** (5.5). *Corretto il 27/10*: il
      runner registra le creazioni iniziate e non salvate (`<state>.unsaved-resources`), il
      Workflow fallisce con `UnsavedResources` e lo state resta sospeso fino ad
      `acceptStateHolds`; verificato in 5.5 ter. Il provider OpenStack salva
      l'ID di `openstack_networking_router_v2` solo dopo che il router è `ACTIVE`: un errore di
      rete durante l'attesa lascia un router orfano, e il retry ne crea un altro. Azione: lookup
      del router per nome (data source) prima di crearlo, o import nel runner.
- [x] **P2. Risorse fuori dallo state dopo un timeout** (12 bis). *Corretto il 27/10*: il runner
      ferma OpenTofu prima della scadenza del Job (grazia del pod 120 s) e registra ciò che non ha
      salvato, come per P1; verificato in 12 ter. Un Job fermato dal timeout può
      creare una risorsa durante la chiusura senza salvarla, togliendo comunque il lock: nessuna
      sospensione. Azione: dopo un `DeadlineExceeded` (o un Job fallito) segnalare lo state come da
      verificare anche senza lock rimasto.
- [x] **N3. Step `Running` in un Workflow fallito.** *Corretto il 27/10* (envtest): lo step prende
      l'esito del Job. Con `TemplateChanged` (3.5c bis) il Job
      finito con successo resta "Running | Terraform Job submitted" nello status del run fallito.
      Azione: chiudere lo step con l'esito del Job.
- [ ] **R4. `errored.tfstate` sul PVC del run.** Il recupero funziona ma il PVC sparisce con lo
      storico. Azione: il runner, se trova `errored.tfstate`, lo carica su S3 accanto ai file
      eseguiti; documentare la procedura di `state push`.
- [ ] **R6. Nessuna versione dello state.** Azione: attivare il versioning del bucket
      (`mc version enable local/tfstate`) in sviluppo e in produzione.
- [ ] **R1. Nomi fissi in `network.tf`** (router `router-test`, subnet fissa): due clienti non
      possono convivere. Azione: parametrizzarli.
- [ ] **N2. State non aggiornato dopo un refresh senza modifiche.** Il runner applica solo se il
      plan ha modifiche: lo state resta con risorse sparite. Azione: `apply -refresh-only` (o apply
      sempre) quando il refresh trova differenze.
- [ ] **Import.** `block_device.volume_type` non viene letto: aggiungere `ignore_changes` o
      documentare la correzione dello state.
- [ ] **Messaggi.** `VariablesMappingFailed` senza il parametro colpevole; timeout del Job non
      distinto; `renderedFiles` valorizzato anche se il render fallisce.
- [ ] **N1. Log dell'operatore.** Conflitti di versione ritentati che producono il warning
      "Reconciler returned both a result … and a non-nil error": solo rumore.
- [ ] **Infrastruttura.** Pool dei floating IP di `public` piccolo (`.221–.231`, 11 indirizzi:
      DHCP, gateway agent, 3 gateway di router e i FIP lo riempiono con 5–6 VM). FIP `.224` e
      `.230` orfani di `acme-vms` rilasciati il 27/10. La rete verso `192.168.20.176` alterna ~5 s su e
      ~5 s giù (`No route to host` dall'host e dal nodo Kind, via `192.168.10.1`); con quell'errore
      i retry del provider si esauriscono in pochi secondi.
