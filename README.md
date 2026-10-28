# latch
## Lifecycle Automation with Templates, Calls and Hooks
## Lifecycle Automation of Terraform, Calls and Hooks

## Moduli condivisi

L'operator utilizza due moduli Go locali, tramite le direttive `replace` in
[go.mod](go.mod). I checkout devono essere in directory sorelle del repository:

- `github.com/karavy/terraform-engine`: directory sorella `terraform-engine`,
	usata da `ApplyTerraformRule`.
- `github.com/karavy/api-engine`: directory sorella `api-engine`, package
	`github.com/karavy/api-engine/pkg/apiengine`, usato da `ApplyAPIRequest`.

Il manager inizializza un solo client API in [cmd/main.go](cmd/main.go) e lo
inietta nel controller in
[internal/controller/basecluster_controller.go](internal/controller/basecluster_controller.go).
Il client supporta GET, PUT, POST e DELETE, propaga il contesto della chiamata e
invia il body senza serializzazioni aggiuntive. La configurazione predefinita
prevede un timeout di 30 secondi, risposte limitate a 10 MiB, nessun retry
applicativo e nessun redirect automatico.

`ApplyAPIRequest` restituisce la risposta del modulo senza convertire gli status
HTTP 4xx/5xx in errori Go: il chiamante deve verificare `Response.IsSuccess()` e
gestire gli errori prima di utilizzare eventuali risposte parziali. Endpoint e
destinazioni devono essere autorizzati dall'operator; le credenziali devono
provenire da Secret e non vanno registrate nei log.

La riconciliazione di `BaseCluster` è ancora uno scheletro: questa integrazione
rende disponibile il client, ma non esegue chiamate automaticamente e non
aggiunge campi alla CRD. Il mapping delle operazioni sulle risorse esterne resta
da definire.

## WorkflowTemplate e Workflow

Due CRD, come per ApiCallTemplate e ApiCall:

- [WorkflowTemplate](api/v1alpha1/workflowtemplate_types.go) è la **definizione** riusabile:
  `parameters` (con un eventuale `default`), `steps` e `deleteSteps`. Non ha status né controller
  e non viene mai eseguito da solo.
- [Workflow](api/v1alpha1/workflow_types.go) **lancia** uno o più template, **in sequenza**, e
  dà un valore ai loro parametri. È immutabile: è la registrazione di una esecuzione.

```yaml
kind: Workflow
spec:
  templates:
    - name: free-subnet            # nome di questo uso, unico nel Workflow
      templateRef: {name: p2p-free-subnet}
      parameters:
        - {name: clientName, value: EXMACHINA}
```

Esempi in [config/samples/p2p-workflow/](config/samples/p2p-workflow/) e in
[config/samples/v1alpha1_workflowtemplate.yaml](config/samples/v1alpha1_workflowtemplate.yaml).

**Esecuzione.** Un template parte quando il precedente è finito, e il primo che fallisce ferma
il Workflow (`status.phase: Failed`, con il motivo nella condizione `Ready`). Lo stesso template
si può usare più volte con nomi diversi. Lo stato di ogni template è in `status.templates`.

**Parametri.** Il Workflow dà un valore ai parametri che il template dichiara; uno che il
template non dichiara è rifiutato, e lo è anche uno mancante senza `default` (`InvalidTemplate`),
così un errore di battitura non parte con il valore sbagliato. Gli step usano un parametro con un
input `parameter: <nome>`, con `equalsParameter` in un `select`, o con `{{parameters.<nome>}}` in
un output `template`.

**Template modificato.** Un template resta modificabile, ma un Workflow che lo sta eseguendo
fallisce con `TemplateChanged` se cambia: meglio fermarsi che mescolare due versioni.

**Eliminazione.** I `deleteSteps` dei template girano quando il Workflow viene **eliminato** da
Kubernetes, **dall'ultimo template al primo** e solo per quelli partiti (anche quello fallito,
che potrebbe aver lasciato qualcosa). Sono ammessi solo step `api`. Il Workflow resta fino alla
fine grazie al finalizer `dinova.one/workflow-deletion`, che l'operator aggiunge solo se
un template ha `deleteSteps`. Uno step che ha bisogno dell'output di uno step che non è mai
arrivato in fondo viene **saltato** (`Skipped`): non c'è niente da annullare. Se uno step di
eliminazione **fallisce**, il Workflow non sparisce: resta con `status.deletion.phase: Failed`,
così la pulizia non va persa in silenzio; per rinunciarci si toglie il finalizer a mano.
Anche un template che non esiste più blocca la pulizia con `WorkflowTemplateNotFound`.

**Nomi degli oggetti creati.** Gli ApiCall, i Secret, i Job e i PVC di un template hanno il nome
del suo uso nel nome e le etichette `dinova.one/workflow` e
`dinova.one/workflow-template`, quindi due template con step omonimi non si confondono.
Gli output degli step sono visibili solo dentro il proprio template.

## WorkflowInstance: tenere applicato un template

Un Workflow è una sola esecuzione. Una [WorkflowInstance](api/v1alpha1/workflowinstance_types.go)
è invece lo **stato desiderato** di un cliente o progetto, come la `Kustomization` di Flux o la
CR `Terraform` del Tofu Controller: un WorkflowTemplate con dei parametri fissi, che l'operatore
tiene applicato nel tempo lanciandolo come **nuovo Workflow** (`<istanza>-1`, `<istanza>-2`…):

- **subito**, alla creazione;
- **appena cambia quello che eseguirebbe**: una nuova [revisione](#revisioni) di uno dei suoi
  TerraformTemplate (quindi una ConfigMap ricreata con altri file), una modifica del
  WorkflowTemplate o dei parametri. L'istanza osserva template e TerraformTemplate, quindi non
  aspetta l'intervallo;
- **a ogni `interval`** anche se non cambia niente, così le modifiche fatte a mano fuori da
  Terraform vengono riportate allo stato del codice. Dopo un run fallito aspetta `retryInterval`
  (default: `interval`).

```yaml
kind: WorkflowInstance
metadata:
  name: acme-vms
  labels: {dinova.one/client: ACME}   # copiate sui run
spec:
  templateRef: {name: virtuozzo-vm}
  parameters:
    - {name: clientName, value: ACME}
    - {name: vms, value: '["vm-1","vm-2"]'}
  interval: 6h
  retryInterval: 30m
```

```
$ kubectl get workflowinstance
NAME       TEMPLATE       READY   LAST RUN     NEXT RUN               AGE
acme-vms   virtuozzo-vm   True    acme-vms-2   2026-10-24T14:28:01Z   2d
```

- **Step API solo se ripetibili.** Un'istanza riesegue il template a ogni run: Terraform converge
  sempre allo stesso stato, ma una `POST` ripetuta crea ogni volta una risorsa nuova. Per questo
  uno step `api` è accettato solo se è sicuro da ripetere: una GET, o uno step con
  `idempotent: true` (vedi "Step ripetibili" nella sezione [ApiCall](#apicall)). Un template con step API richiede
  inoltre `approval: Auto` (gli step API non hanno un plan) e `deletionPolicy: Retain` (un
  Workflow `Destroy` esegue solo step Terraform). Anche gli ApiCallTemplate degli step entrano
  nella revisione: modificarne uno fa partire un run. I template con `deleteSteps` sono sempre
  rifiutati, perché l'istanza cancella i suoi run vecchi e li eseguirebbe. Un template rifiutato
  dà `Ready=False` con motivo `UnsupportedTemplate`.
- **Revisione.** `status.revision` è lo SHA-256 di quello che un run eseguirebbe (template,
  parametri, revisione di ogni TerraformTemplate, in `status.terraformRevisions`). Quando è
  diversa da `status.lastAttemptedRevision` parte un run. Cambiare solo `interval`, `suspend` o i
  limiti dello storico non fa partire niente.
- **Un run alla volta.** Finché un run è in corso non ne parte un altro; quando finisce, si
  ricontrolla.
- **`Ready`** è `True` quando l'ultimo run ha applicato la revisione corrente; altrimenti `False`
  con il motivo (`RunInProgress`, `RunFailed`, `Suspended`, `WorkflowTemplateNotFound`, il motivo
  del TerraformTemplate non pronto…).
- **Storico.** Restano gli ultimi `successfulRunsHistoryLimit` run riusciti e
  `failedRunsHistoryLimit` falliti (default 3 e 3).
- **`suspend: true`** non fa partire run nuovi; quello in corso continua.
- **Eliminazione (`deletionPolicy`).** Con `Retain`, il default, eliminare l'istanza elimina i
  suoi run, **non** le risorse: gli state Terraform restano. Con `Destroy` l'istanza le distrugge
  prima di andarsene:
  1. aspetta che il run in corso finisca (`Ready` con motivo `WaitingToDestroy`);
  2. lancia il template come Workflow `Destroy`, chiamato `<istanza>-destroy-<inizio dell'UID>` e
     riportato in `status.destroyRun` (`Ready` con motivo `Destroying`);
  3. quando quel Workflow riesce, toglie il finalizer `dinova.one/instance-destroy` e l'istanza
     sparisce. Il Workflow `Destroy` non appartiene all'istanza e resta, come registrazione della
     distruzione.

  Se il destroy fallisce l'istanza resta, con `Ready` a `DestroyFailed`: si cancella il Workflow
  `Destroy` per riprovare, oppure si imposta `deletionPolicy: Retain` per rinunciare e lasciare
  le risorse.
- **Un'istanza e un Workflow a mano non vanno usati sullo stesso state:** due run in parallelo si
  bloccano sul lock dello state.

### Approvazione manuale dei plan

Con `approval: Manual` l'istanza **non applica niente da sola**: ogni run (cambio di codice o
intervallo) è un Workflow in modalità `Plan`, che fa solo il plan di ogni step e lo salva nello
storage S3 accanto allo state.

1. Se il plan non ha differenze, l'istanza resta `Ready=True` e `Drifted=False`.
2. Se ha differenze, l'istanza passa a `Drifted=True` e `status.pendingPlan` le riassume: l'`id`
   del plan (il nome del run), lo step, quante risorse aggiunge, modifica e distrugge, e il
   Secret con il testo leggibile del plan (è un Secret perché il plan mostra i valori delle
   risorse).
   ```bash
   kubectl get workflowinstance acme-vms -o jsonpath='{.status.pendingPlan}'
   kubectl get secret <pendingPlan.secret> -o jsonpath='{.data.plan\.txt}' | base64 -d
   ```
3. Se ti va bene, lo approvi scrivendo il suo id nell'istanza:
   ```bash
   kubectl patch workflowinstance acme-vms --type merge -p '{"spec":{"approvePlan":"acme-vms-7"}}'
   ```
4. L'operatore lancia un Workflow `ApplyPlan` che applica **esattamente** quel plan salvato,
   senza ricalcolarne uno, poi fa subito un nuovo plan per gli step successivi e per verificare
   il risultato.

- **Uno step alla volta.** Il plan si ferma al primo step con differenze: gli step dopo
  potrebbero dipendere da quello che cambierà, e un loro plan calcolato prima sarebbe sbagliato.
  Gli step senza differenze pubblicano gli output dal loro state, così lo step successivo si
  può pianificare. Per questo la prima creazione conviene farla con `approval: Auto`.
- **L'approvazione vale per quel plan.** Se il codice cambia o arriva un plan più recente,
  l'approvazione di quello vecchio viene ignorata (il messaggio di `Drifted` lo dice). Se lo
  state cambia dopo il plan, Terraform rifiuta il plan come "stale": il run fallisce senza
  toccare niente. Un plan approvato non viene mai applicato due volte.
- **Pulizia dei plan.** I file di un plan (plan, testo e riepilogo, sotto
  `<namespace>/plans/<workflow>-<uid>/` nello storage) vengono cancellati appena il plan è
  applicato, e comunque quando il Workflow `Plan` che li ha salvati viene cancellato (dallo
  storico dell'istanza o a mano): il finalizer `dinova.one/plan-cleanup` lo trattiene finché la
  cancellazione non riesce, perché un plan contiene i valori delle risorse come lo state.
- **Workflow a mano.** Le stesse modalità valgono per un Workflow: `spec.terraformMode: Plan`
  per un controllo, `ApplyPlan` con `spec.applyPlan` (`workflow`, `uid`, `step`) per applicare
  un plan salvato. Sono ammesse solo con un template di soli step Terraform.

### Distruggere le risorse (`terraformMode: Destroy`)

Un Workflow con `spec.terraformMode: Destroy` distrugge quello che gli step del suo template
gestiscono, negli state indicati dai parametri. Si crea come un qualunque Workflow, con gli
stessi parametri del run che ha creato le risorse:

```yaml
kind: Workflow
spec:
  terraformMode: Destroy
  templates:
    - name: zone
      templateRef: {name: google-dns-zone}
      parameters:
        - {name: clientName, value: acme}
        - {name: parentZone, value: k8s-test-dinova-services}
```

1. **Lettura, in ordine.** Ogni step legge i suoi output dal suo state, senza Job: gli step dopo
   possono averne bisogno per le variabili o per la chiave dello state. Uno step senza state, o
   con uno state senza risorse, è `Skipped` ("Nothing to destroy"), e così uno la cui chiave
   dipende da un output di uno step saltato.
2. **Destroy, dall'ultimo step al primo.** Per ogni step non saltato parte un Job con
   `tofu destroy`, con le stesse variabili e lo stesso state di un apply. Si va dall'ultimo
   perché le risorse di uno step possono dipendere da quelle degli step prima.

Lo state resta nello storage, senza risorse: un nuovo Workflow `Apply` ricrea tutto da capo. Come
`Plan` e `ApplyPlan`, accetta un solo template, con soli step Terraform.

Esempio completo in
[config/samples/terraform-workflow/virtuozzo-vm-instance.yaml](config/samples/terraform-workflow/virtuozzo-vm-instance.yaml).

## WorkflowPipeline: un processo in più fasi

Una [WorkflowPipeline](api/v1alpha1/workflowpipeline_types.go) descrive un processo intero, per
esempio l'attivazione di un cliente, fatto di **fasi**. Ogni fase è una WorkflowInstance (risorse
Terraform tenute applicate) o un Workflow (operazioni una tantum, come le chiamate API).
L'operatore crea gli oggetti delle fasi nell'ordine delle dipendenze e passa gli output da una
fase ai parametri della successiva.

```yaml
kind: WorkflowPipeline
metadata: {name: acme-k8s}
spec:
  parameters:
    - {name: clientName, value: acme}     # a ogni fase il cui template lo dichiara
  stages:
    - name: vdc
      instance: {templateRef: {name: newk8s-vdc}, interval: 6h}
    - name: vsphere-network
      dependsOn: [vdc]
      parameters:
        - name: orgName
          valueFrom: {stage: vdc, step: vdc, output: orgName}
      workflow: {templateRef: {name: newk8s-vsphere-network}}
```

Esempio completo, costruito su [FLUSSO.md](config/samples/newk8scluster/FLUSSO.md), in
[config/samples/newk8scluster/](config/samples/newk8scluster/pipeline.yaml).

- **Ordine.** Una fase parte quando tutte le fasi in `dependsOn` sono pronte: un'istanza con
  `Ready=True`, un Workflow `Succeeded`. Le fasi che non dipendono l'una dall'altra girano in
  parallelo. Dipendenze circolari o verso fasi inesistenti sono rifiutate.
- **Nomi.** L'istanza di una fase si chiama `<pipeline>-<fase>`, i run di una fase Workflow
  `<pipeline>-<fase>-<n>`; tutti hanno le etichette della pipeline più
  `dinova.one/workflow-pipeline` e `dinova.one/pipeline-stage`. Nome della pipeline e nome di
  una fase insieme non superano 55 caratteri.
- **Parametri.** I `parameters` della pipeline vanno a ogni fase il cui template li dichiara; i
  parametri della fase prevalgono. `valueFrom` legge un output già pubblicato dal motore del
  Workflow (lo status del run): dal run corrente di una fase Workflow riuscita, o dal run
  riuscito più recente di un'istanza che lo pubblica. Un output stringa passa com'è, gli altri
  come testo JSON (nel template: `asNumber`, `asJSON`). Una fase che legge un output deve
  dipendere, anche indirettamente, dalla fase che lo pubblica. Gli output `sensitive` non si
  passano: finirebbero in chiaro nella spec della fase.
- **Fasi istanza.** La pipeline tiene la spec dell'istanza allineata alla fase (`interval`,
  `approval`, `approvePlan`, `acceptStateHolds`…: si modificano nella pipeline, non
  nell'istanza). Se un output letto cambia, cambia la revisione dell'istanza, che riapplica da
  sola.
- **Fasi Workflow.** Girano una volta. Se un valore che hanno usato cambia, la fase diventa
  `Outdated` e lo status elenca i valori cambiati (`status.stages[].changed`), senza rilanciare
  niente. Per rilanciarla, o per riprovare una fase `Failed`, si scrive il suo Workflow corrente
  (`status.stages[].workflow`) in `spec.rerun`:
  ```bash
  kubectl patch workflowpipeline acme-k8s --type merge -p '{"spec":{"rerun":["acme-k8s-fortigate-1"]}}'
  ```
  La fase crea un nuovo run con i valori attuali; la voce vale una volta sola, perché nomina un
  run che non è più il corrente.
- **`rerunOnChange` e `idempotent`.** Con `rerunOnChange: true` una fase Workflow si rilancia da
  sola quando cambiano i suoi valori. È accettato solo se ogni step API del template è sicuro da
  ripetere: una GET, o uno step con `idempotent: true` (per esempio una PUT che imposta una
  configurazione, o una creazione che prima controlla se l'oggetto esiste). L'operatore non può
  verificarlo: uno step dichiarato idempotente che non lo è può creare duplicati.
- **Storico.** I run delle fasi Workflow restano tutti: cancellarne uno eseguirebbe i suoi
  `deleteSteps`, che potrebbero annullare quello che il run successivo ha configurato. Le
  istanze tengono il loro storico (`successfulRunsHistoryLimit`, `failedRunsHistoryLimit`).
- **Stato.** `Ready` è `True` quando tutte le fasi sono `Ready`; altrimenti indica la prima
  fase `Failed`, poi `Outdated`, poi in corso. Ogni fase ha in `status.stages` fase, oggetto e
  messaggio:
  ```bash
  kubectl get workflowpipeline acme-k8s -o jsonpath='{range .status.stages[*]}{.name}{"\t"}{.phase}{"\t"}{.message}{"\n"}{end}'
  ```
- **Eliminazione.** Cancellare la pipeline cancella gli oggetti delle fasi in ordine inverso:
  una fase va quando nessuna fase che dipende da lei ha ancora oggetti. Un'istanza con
  `deletionPolicy: Destroy` distrugge prima le sue risorse, un Workflow esegue i suoi
  `deleteSteps`: la pipeline (finalizer `dinova.one/pipeline-deletion`) aspetta che ognuno sia
  sparito.
- **Un oggetto, un proprietario.** Non creare a mano istanze o Workflow con i nomi delle fasi, e
  non usare un'istanza o un Workflow a mano sugli state Terraform di una fase.

## ApiCall

I file Kubebuilder [internal/controller/apicall_controller.go](internal/controller/apicall_controller.go)
e [internal/controller/apicall_controller_test.go](internal/controller/apicall_controller_test.go)
restano nella cartella principale dei controller e usano la suite envtest comune.
La logica di supporto, l'executor HTTP, la preparazione delle richieste e i test
unitari sono raccolti nel package `internal/controller/apicall`.

La CRD [ApiCall](api/v1alpha1/apicall_types.go) rappresenta una singola esecuzione
di una chiamata HTTP tramite `api-engine`. Il manifest completo è in
[config/samples/v1alpha1_apicall.yaml](config/samples/v1alpha1_apicall.yaml).

**Un ApiCall creato da solo non viene mai eseguito.** La definizione riusabile di
una chiamata è un [ApiCallTemplate](api/v1alpha1/apicalltemplate_types.go) (metodo,
endpoint, body, header, autenticazione, timeout), che non ha status né controller
e non viene mai eseguito. Un [Workflow](api/v1alpha1/workflow_types.go) lo richiama,
attraverso uno step di un WorkflowTemplate, per nome con `templateRef`: per ogni step crea un ApiCall figlio, di cui è owner, ed
è solo questo che il controller esegue. Lo stesso template può quindi essere riusato
da più workflow e più step. Poll e passaggio dei dati (`inputs`/`outputs`) si
definiscono nello step del WorkflowTemplate; anche il poll richiama un template per nome.
Esempi in
[config/samples/v1alpha1_apicalltemplate.yaml](config/samples/v1alpha1_apicalltemplate.yaml)
e [config/samples/v1alpha1_workflowtemplate.yaml](config/samples/v1alpha1_workflowtemplate.yaml).

**Ripetere una GET dentro lo stesso ApiCall.** `spec.retry` dell'[ApiCall](api/v1alpha1/apicall_types.go)
rimanda la stessa richiesta senza creare altri oggetti: `status.attempts` conta i tentativi,
`status.response` è l'ultima risposta e `status.nextAttemptAt` dice quando parte il prossimo
mentre l'ApiCall resta `Running`. Vale solo per le GET (il CRD rifiuta `retry` con un altro
metodo). Ha `maxAttempts` (tentativi in tutto, il primo compreso, default 3), `intervalSeconds`
(default 5), `onStatusCodes` (gli stati HTTP che meritano un altro tentativo) e
`onTransportErrors` (default true); un altro errore fa fallire subito l'ApiCall. Con `until`
(`jsonPointer` e `equalsJSON`) una risposta di successo conta solo se soddisfa la condizione:
così si aspetta la fine di un job. Se i tentativi finiscono, l'ApiCall fallisce con
`RetriesExhausted`, o con `PollTimeout` se c'è `until`; una condizione che non si può leggere
(risposta non JSON, campo assente) la fa fallire subito con `ConditionInvalid`.

Nello step di un WorkflowTemplate, `retry` usa questo meccanismo, e anche un `poll` **senza**
`templateRef`, che rimanda la richiesta dello step stessa (in quel caso `maxAttempts` conta le
richieste in tutto, e `inputs` e `retry` non si possono aggiungere). Un `poll` **con**
`templateRef` continua invece a creare un ApiCall per ogni richiesta, perché la richiesta di
attesa è un'altra (per esempio una POST che avvia e una GET che controlla).

Per le API asincrone lo step ha un `poll`: dopo la chiamata iniziale ripete a
`intervalSeconds` (default 5) la chiamata del template `poll.templateRef`, fino a
`maxAttempts` (default 20), finché `conditionJSONPointer` nella risposta non vale
`equalsJSON`. Se il poll non è pronto entro `maxAttempts` il workflow fallisce con
`PollTimeout`. Di default qualsiasi poll con HTTP non 2xx, timeout o errore di rete
fa fallire il workflow; per tollerarli si dichiara `retryOnStatusCodes` (per esempio
`[404, 503]`) e/o `retryOnTransportErrors: true`. Un poll tollerato conta come "non
pronto" e consuma un tentativo; richieste non valide, risposte troppo grandi ed
esecuzioni perse non sono mai tollerate, e le opzioni non valgono per la chiamata
iniziale.

**Step ripetibili (`idempotent`).** `idempotent: true` su uno step API dichiara che rimandare le
sue richieste dà lo stesso risultato (una PUT che imposta una configurazione, una creazione che
prima controlla se l'oggetto esiste). In un Workflow non cambia niente; serve a una
[WorkflowInstance](#workflowinstance-tenere-applicato-un-template) per accettare lo step, che
rieseguirà a ogni run, e a una [WorkflowPipeline](#workflowpipeline-un-processo-in-più-fasi)
per accettare `rerunOnChange` su una fase. Le GET sono ripetibili senza dichiararlo.
L'operatore non può verificarlo: uno step dichiarato idempotente che non lo è può creare
duplicati.

**Riprendere un job fallito (`poll.recover`).** Alcune API fanno partire un job a più fasi e
poi permettono di riprenderlo da dove si è fermato. Con `recover`, un valore del poll che è
anche in `recover.whenJSON` (e quindi in `failWhenJSON`) non fa fallire lo step. Lo step manda
invece la richiesta `recover.templateRef` (una sola volta, senza ripeterla), aspetta un
`intervalSeconds` e ricomincia il poll da capo, con tutti i `maxAttempts`. Gli `inputs` della
ripresa leggono la risposta fallita con `fromResponse` (un JSON Pointer), e `add` somma un
numero, per esempio `{fromResponse: /last_completed_step, to: /resume_from_step, add: 1}`.
La ripresa può ripetersi più volte, per esempio: fallisce alla fase 3, riparte dalla 3, fallisce
alla 6, riparte dalla 6, e così via. Si ferma con `RecoveryExhausted` quando il numero
in `progressPointer` non cresce per `maxRetriesWithoutProgress` riprese di fila (default 3),
oppure dopo `maxRecoveries` riprese in tutto (default 10). Si ferma con `RecoveryBlocked` quando
una condizione di `unless` vale (per esempio `/manual_review_required` uguale a `true`). Se
la richiesta di ripresa fallisce, lo step fallisce con `RecoveryFailed`. Durante la ripresa lo
step è `Recovering`, e `status.recoveries` elenca ogni ripresa con il valore che l'ha causata,
il progresso letto e l'ApiCall della richiesta. Esempio nello step `wait-deliver` di
[config/samples/p2p-workflow/workflowtemplate.yaml](config/samples/p2p-workflow/workflowtemplate.yaml).

### Step Terraform

Come per le chiamate API, la definizione è separata dal workflow: un
[TerraformTemplate](api/v1alpha1/terraformtemplate_types.go) contiene l'elenco dei file
(`spec.files`: `name` + `configMapKeyRef`, nome senza directory e con estensione `.tf` o
`.tf.tmpl`) e lo step lo richiama con `terraform.templateRef`. Il codice sta **solo** in
ConfigMap **immutabili** (`immutable: true`): l'operatore rifiuta le altre, così il codice di
una versione non può cambiare.

#### Revisioni

L'operatore fa le versioni da solo. Ogni volta che il contenuto dei file di un TerraformTemplate
cambia (una ConfigMap cancellata e ricreata con altri file, o il template che punta a un'altra
ConfigMap), calcola lo SHA-256 dei file e ne fa uno snapshot in una
[ControllerRevision](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/controller-revision-v1/)
con il **numero di revisione** successivo, lo stesso meccanismo di StatefulSet e DaemonSet:

```
$ kubectl get terraformtemplate
NAME           REVISION   READY   AGE
virtuozzo-vm   3          True    2d
```

- Lo status del template riporta revisione, `digest` e le ConfigMap lette (con il loro UID).
- File identici a quelli di una revisione ancora conservata riportano quella revisione (rollback);
  un numero già dato non viene mai riusato. `spec.revisionHistoryLimit` (default 10) dice quante
  revisioni tenere.
- Se i file non si possono leggere (ConfigMap mancante o non immutabile, chiave mancante) il
  template ha `Ready=False` con il motivo, e gli step che lo usano falliscono con quel motivo
  invece di eseguire la revisione precedente.
- Ogni step registra nello status del Workflow la revisione eseguita (`terraformRevision`,
  `terraformDigest`). Uno step che parte mentre l'operatore non ha ancora fatto la revisione di
  un template appena modificato la aspetta.

All'avvio dello step il Workflow **copia** i file della revisione corrente in una ConfigMap
immutabile di cui è owner (le revisioni successive non cambiano uno step già partito) e crea un Job che monta
la ConfigMap, il PVC del workflow (solo file di lavoro: template renderizzati e provider) e un
Secret con `terraform.auto.tfvars.json`, costruito dagli `inputs` dello step. Render dei
`*.tf.tmpl` (placeholder `<FGT_NOME>` e cicli `<FGT_FOR_STR>`), `init` e `apply` li fa
[terraform-engine](../terraform-engine) dentro il Job ([cmd/terraform-runner](cmd/terraform-runner));
la logica Kubernetes (file, Job, storage dello state) sta in
[internal/controller/terraform](internal/controller/terraform). Esempio in
[config/samples/v1alpha1_terraformtemplate.yaml](config/samples/v1alpha1_terraformtemplate.yaml).
Un workflow può avere più step Terraform: ognuno lavora nella propria cartella
`/workspace/<nome-step>` del PVC (i job girano in sequenza, quindi il volume ReadWriteOnce
condiviso basta).

#### State condiviso tra workflow

Lo state **non** sta nel workflow né nel PVC: è un oggetto in uno storage S3 compatibile
(MinIO, Garage, Ceph RGW...) identificato da `terraform.state.key`. Workflow diversi, anche
a distanza di tempo e con valori modificati, che usano la stessa chiave nello stesso
namespace lavorano **sullo stesso state**: Terraform confronta i nuovi valori con lo stato
esistente e modifica la risorsa invece di ricrearla. Se un `apply` si interrompe, la
risorsa è già in parte nello state e l'esecuzione successiva riparte da lì.

```yaml
- name: network
  terraform:
    templateRef: {name: network}
    state:
      key: vdc/{{steps.create-vdc.outputs.vdcName}}/network   # la risorsa "rete del VDC acme"
    envFromSecrets: [tfstate-credentials]                     # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    inputs:
      - {step: create-vdc, output: vdcId, to: /vdc_id}
    outputs:
      - {name: networkId, from: /network_id}
```

- **Chiave:** segmenti separati da `/` con lettere, cifre, `.`, `_`, `-` (se inizia con `{{` in YAML
  servono le virgolette). Può contenere `{{parameters.<nome>}}`, sostituito con il valore che il
  Workflow dà al parametro (per esempio il cliente: tutti i Workflow dello stesso cliente
  lavorano sullo stesso state), e
  `{{steps.<step>.outputs.<nome>}}`, sostituito con un output di uno step precedente (anche
  Terraform): deve essere stringa, numero o booleano. I valori devono essere fatti degli stessi
  caratteri dei segmenti, così non possono mai aggiungere segmenti né uscire dal percorso. L'oggetto nello storage è
  `<namespace>/<chiave>.tfstate`, quindi due namespace non condividono mai uno state.
- **I file eseguiti restano nello storage.** Il Job salva nel bucket dello state, con le stesse
  credenziali, i file che esegue così come escono dal render: i `.tf` (compreso
  `backend_override.tf`), `terraform.auto.tfvars.json` e un `manifest.json`. Il prefisso è
  `<namespace>/rendered/<workflow>-<uid>/<template>/<step>/` ed è riportato in
  `status.templates[].steps[].renderedFiles`. Il manifest dice a quale esecuzione
  appartengono: Workflow e UID, template, step, TerraformTemplate con revisione e digest, chiave
  dello state, modalità, ora, e per ogni file la dimensione e lo SHA-256. Il salvataggio avviene
  prima di `init`, quindi restano anche i file di un run fallito, e se non riesce il Job fallisce
  prima di toccare l'infrastruttura. L'operatore non li cancella mai, neanche con il Workflow,
  perché servono a ricostruire cosa è stato applicato. Per tenerli solo per un certo tempo si usa
  una regola di lifecycle del bucket con prefisso `<namespace>/rendered/`, una per namespace (i
  filtri di lifecycle non accettano caratteri jolly). Contengono i valori delle variabili, quindi
  vanno protetti come lo state:
  ```bash
  aws s3 ls --recursive s3://tfstate/<namespace>/rendered/<workflow>-<uid>/
  aws s3 cp s3://tfstate/<namespace>/rendered/<workflow>-<uid>/<template>/<step>/manifest.json -
  ```
- **Il backend lo scrive il runner**, in un file `backend_override.tf` che prevale su qualunque
  `backend` scritto nel template: non si può dimenticare né aggirare, e senza configurazione
  completa il Job fallisce invece di usare uno state locale che si perderebbe. Il lock dello
  state è un file nel bucket (`use_lockfile`: richiede OpenTofu ≥ 1.10 e uno storage con
  scritture condizionali): due esecuzioni sulla stessa chiave non si sovrappongono, la seconda
  fallisce con "Error acquiring the state lock".
- **Stop prima della scadenza del Job:** il runner interrompe OpenTofu da solo poco prima di
  `timeoutSeconds` (`TF_STOP_AFTER_SECONDS`: 120 s prima, o un quarto del timeout se è breve),
  e il pod ha 120 s di grazia: OpenTofu finisce le richieste in corso, salva lo state e toglie il
  lock invece di essere ucciso. Il Workflow lo dice ("reached its time limit, before its
  timeout").
- **State sospesi (risorse forse create fuori dallo state).** Due casi fermano gli apply su uno
  state finché qualcuno non controlla:
  - *lock lasciato da un runner morto*: l'operatore rimuove un lock `runner@<pod>` solo quando il
    Job di quel pod è di un Workflow ed è finito e nessun pod con quel nome è vivo (i lock di
    altri, o non riconducibili al Job, restano); lo registra nello step (`releasedStateLock`),
    con un Event, e in `<state>.released-lock`; reason `StateLockReleased`;
  - *creazioni non salvate*: quando un apply fallisce o viene interrotto, il runner confronta le
    risorse che ha iniziato a creare con lo state salvato; quelle che mancano (es. un router la
    cui attesa di `ACTIVE` è fallita: il provider OpenStack ne salva l'ID solo dopo) sono
    registrate in `<state>.unsaved-resources` e nello step (`unsavedResources`); reason
    `UnsavedResources`. Una creazione rifiutata dall'API (risposta 4xx alla richiesta di
    creazione, es. pool esaurito) non conta: non ha creato nulla, e il retry procede come prima.

  Gli step Apply e ApplyPlan su uno state sospeso falliscono, senza Job, finché l'ID non è in
  `spec.acceptStateHolds` (del Workflow, o dell'istanza, che nel frattempo mostra
  `status.stateHold` e la condizione `StateHold` e non riprova; accettato, riparte subito). Plan
  e Destroy girano comunque, ma un Destroy non tocca le risorse che lo state non conosce. Prima di
  accettare, importare o cancellare quelle risorse nel cloud.
- **Configurazione dell'operator:** `--tfstate-s3-endpoint`, `--tfstate-s3-bucket`,
  `--tfstate-s3-region` (default `us-east-1`) e le variabili d'ambiente
  `TFSTATE_S3_ACCESS_KEY_ID` / `TFSTATE_S3_SECRET_ACCESS_KEY`, con cui l'operator **legge** lo
  state per pubblicare gli output. Senza questi parametri gli step Terraform falliscono con
  `StateBackendNotConfigured`; una configurazione parziale impedisce l'avvio.
- **Credenziali del Job:** vengono da un Secret del namespace elencato in `envFromSecrets`, con
  `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` (il Job non ha accesso all'API Kubernetes).
- **Output:** `terraform.outputs` legge i valori dallo state quando il Job riesce, come un
  unico oggetto JSON `{"nome_output": valore}`: `from` è un JSON Pointer (`/network_id`,
  `/ips/0`) e valgono `defaultJSON`/`sensitive` come per gli step API. Un output marcato
  `sensitive = true` in Terraform va **sempre** nel Secret, anche se nello step non lo si
  dichiara. Se non si dichiarano output lo state non viene nemmeno letto.
- **Limiti:** lo state in chiaro contiene anche i valori sensibili, quindi il bucket va
  protetto come un Secret; non c'è ancora un modo per distruggere le risorse (`destroy`) o per
  cancellare uno state.

### Passare dati da uno step all'altro

Uno step pubblica valori **con nome** dalla propria risposta JSON (`outputs`) e
qualsiasi step successivo, non solo il successivo immediato, li inserisce nel body
della propria richiesta (`inputs`):

```yaml
steps:
  - name: create-vdc
    api:
      templateRef: {name: create-vdc}
      outputs:
        - {name: vdcId, from: /id}                          # JSON Pointer nella risposta
        - {name: region, from: /region, defaultJSON: '"eu-1"'}
        - {name: token, from: /token, sensitive: true}
  - name: create-network
    api:
      templateRef: {name: create-network}
      inputs:
        - {step: create-vdc, output: vdcId, to: /vdcId}     # JSON Pointer nel body
```

- Se `from` non esiste nella risposta, il workflow **fallisce** (`OutputMissing`) e
  non invia mai `null` di nascosto. Un valore di ripiego va dichiarato in modo
  esplicito con `defaultJSON` (qualsiasi JSON, anche `null`).
- I riferimenti sono validati **prima di eseguire qualsiasi step**: step sconosciuto,
  step successivo, output non dichiarato o nomi duplicati portano il workflow in
  `Failed` con reason `InvalidReference`, senza inviare richieste.
- I valori non sensibili sono in `status.steps[].outputs` (JSON, al massimo 4 KiB
  l'uno e 32 per step). Gli output `sensitive: true` non compaiono mai nello status:
  stanno nel Secret `status.outputsSecretName` (owner il Workflow, chiave
  `<step>.<output>`), e lo status ne elenca solo i nomi in `sensitiveOutputs`.
- Con un `poll`, gli `outputs` dello step leggono la risposta **iniziale**;
  `poll.outputs` legge la risposta **finale** (quella che soddisfa la condizione), e
  `poll.inputs` può usare gli output dello step e di quelli precedenti.
- Lo step Terraform usa `inputs` allo stesso modo: `to` è il nome della variabile
  (`/cluster_id`) nel `terraform.auto.tfvars.json`. Gli output di Terraform si pubblicano
  con `outputs` (vedi "State condiviso tra workflow").
- I valori si inseriscono solo nel **body JSON** (non in URL, query o header).

**Limite sui dati sensibili:** il Secret protegge lo status del Workflow e permette
un RBAC separato, ma il valore, una volta usato in un `input`, finisce nel body
dell'ApiCall figlio, e la risposta completa resta in `status.response` di ogni
ApiCall figlio (il Workflow richiede `storeResponseBody: true` per poterla leggere).
Chi può leggere gli ApiCall del namespace può quindi vedere quei valori.

### Configurazione

- `spec.method`: GET, PUT, POST o DELETE.
- `spec.endpoint`: URL assoluto HTTP(S), inclusi eventuali parametri di query.
	È l'unico campo della spec modificabile dopo la creazione. Cambiarlo retargetta
	esplicitamente la chiamata; tutti gli altri campi della spec sono immutabili.
- `spec.body`: stringa inviata **senza serializzazione aggiuntiva**, massimo
	64 KiB. Per JSON, usare una stringa o un blocco YAML e specificare
	`Content-Type: application/json` in `spec.headers`.
- `spec.authentication`: opzionale, con `type: Bearer`, `Basic`, `APIKey` o
  `OAuth2ClientCredentials`.
	Bearer e APIKey usano `tokenSecretRef: {name, key}`; Basic usa
	`usernameSecretRef` e `passwordSecretRef`; APIKey richiede anche `headerName`
	(per esempio `X-API-Key`). OAuth2 richiede `tokenURL`, `clientIDSecretRef` e
	`clientSecretSecretRef`; supporta `clientAuthenticationMethod` uguale a
	`ClientSecretBasic` (default) o `ClientSecretPost`, scopes facoltativi e il
	parametro provider-specific `audience`. L'operator richiede un access token
	con grant `client_credentials` prima della richiesta API, usando lo stesso
	timeout complessivo. `ClientSecretBasic` applica la form-encoding OAuth alle
	credenziali prima dell'header Basic; `ClientSecretPost` le invia nel body TLS.
	Il token Bearer viene usato solo per la chiamata API, non è messo in cache o
	scritto nello stato.
	Token endpoint e API devono entrambi avere origini HTTPS autorizzate.
	Tutti i Secret devono essere nella **stessa namespace** della CR. Le
	credenziali e i token sono inviati solo via HTTPS con verifica TLS. I certificati
	remoti devono essere riconosciuti dal trust store del manager.
- `spec.timeoutSeconds`: default 120, intervallo 1–3600. Il timeout parte quando
	un worker avvia la richiesta e copre anche la lettura della risposta; il tempo
	trascorso in coda non è incluso.
- `spec.storeResponseBody`: default true. Impostare false se la risposta è
	sensibile: il body non verrà salvato nello stato.
- `spec.executionID`: stringa opzionale che distingue ApiCall altrimenti identici.
	La spec è immutabile, quindi non serve a rieseguire un ApiCall esistente.

Il manager espone questi flag:

| Flag | Default | Descrizione |
| --- | --- | --- |
| `--apicall-allowed-origins` | vuoto | Origini esatte separate da virgole, ad esempio `https://api.example.com,https://other.example.com:8443`. Vuoto **nega tutte le chiamate**. |
| `--apicall-allowed-origins-configmap` | vuoto | `namespace/nome` di un ConfigMap la cui chiave `origins` elenca altre origini, una per riga o separate da virgole (le righe che iniziano con `#` sono commenti). Si somma a `--apicall-allowed-origins`. Viene letto a ogni richiesta, quindi una modifica vale subito, senza riavviare l'operator. Un ConfigMap assente non aggiunge origini; una voce che non è un'origine esatta, ad esempio con un path, viene ignorata e scritta nel log. |
| `--apicall-workers` | 4 | Massimo numero di richieste HTTP concorrenti, 1–64. |
| `--apicall-queue-size` | 64 | Capacità aggiuntiva del pool, 1–4096. I risultati completati ma non ancora persistiti occupano capacità. |
| `--apicall-max-response-bytes` | 4194304 (4 MiB) | Massimo numero di byte letti da una risposta, almeno 65536. Una risposta più grande fa fallire l'ApiCall con `ResponseTooLarge`. Nello status finiscono comunque al massimo 64 KiB (vedi `spec.keepJSONPointers`). |

Per il manifest OAuth2 di esempio, configurare
`--apicall-allowed-origins=https://api.example.com,https://auth.example.com`
negli argomenti del manager in [config/manager/manager.yaml](config/manager/manager.yaml)
(oppure elencarle in un ConfigMap, vedi sotto)
e creare il Secret `oauth-client-credentials` con le chiavi `client-id` e
`client-secret` nella namespace della CR. Nessun Secret con credenziali reali
viene incluso nei manifest di esempio.

Con più server API conviene tenere le origini in un ConfigMap nella namespace
dell'operator, non in quelle dei tenant: chi può modificarlo decide dove l'operator
può chiamare.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: latch-system
  name: apicall-allowed-origins
data:
  origins: |
    # portale
    https://dev-portal.admin.dinova.cloud:8453
    https://auth.example.com
```

con `--apicall-allowed-origins-configmap=latch-system/apicall-allowed-origins`
negli argomenti del manager.

### Esecuzione e stato

Il [controller](internal/controller/apicall_controller.go) delega la preparazione e
la gestione dello stato ai [helper](internal/controller/apicall/reconcile.go) e
salva il checkpoint `Running` **prima** di inviarla. Il
[pool di worker](internal/controller/apicall/api_executor.go) è un `manager.Runnable`
leader-only: le goroutine partono da `Start`, non da `Reconcile`, e sono cancellate
e attese alla chiusura del manager. Anche richieste sincrone lente non occupano un
worker di riconciliazione. Non viene creato un processo OS separato.
Per più repliche è necessario abilitare la leader election, come già previsto dal
Deployment di esempio; senza leader election più manager possono inviare duplicati.

Un evento di completamento rimette la CR nella coda del controller. Un controllo
ogni 2 secondi funge da fallback. Solo il controller aggiorna lo stato; se il
salvataggio fallisce, il risultato resta in memoria per il tentativo successivo.
Il pool limita richieste, coda e risultati in memoria. Quando è pieno, le altre CR
restano `Pending`. Un Secret assente viene ricontrollato ogni 10 secondi, senza
avviare HTTP e senza inserire valori sensibili nelle condizioni. Ogni lettura di
un Secret è limitata a 5 secondi, o alla deadline più breve del contesto.
Le credenziali sono fotografate al momento della preparazione della richiesta:
una rotazione del Secret non modifica le operazioni già in coda o in esecuzione
e non ripete quelle completate. Per OAuth2, l'access token viene richiesto dal
worker immediatamente prima della chiamata protetta e non viene memorizzato in
cache né nello stato. Un nuovo ApiCall acquisisce i valori correnti dei
Secret. Per ripetere l'operazione, dopo aver verificato l'effetto remoto, creare
un nuovo ApiCall (per esempio lanciando un nuovo Workflow).

| Fase | Significato |
| --- | --- |
| `Pending` | In attesa di credenziali o capacità del pool. |
| `Running` | Checkpoint persistito; richiesta in coda o in esecuzione. |
| `Succeeded` | Risposta HTTP 2xx completa ricevuta. |
| `Failed` | Configurazione non ammessa, risposta non-2xx, timeout, errore di trasporto/lettura o risposta troppo grande. |
| `Unknown` | Stato del worker perso dopo un riavvio/failover; l'effetto remoto non è noto. |

`status` espone `observedGeneration`, `startedAt`, `completedAt`, condizioni
`Ready`/`Progressing` e `response` con `statusCode`, `contentType`, `body` e
`truncated`. `response.body` è **base64** per conservare anche dati binari;
decodificandolo si ottiene il payload originale. Una risposta viene letta fino a
`--apicall-max-response-bytes`; oltre quel limite l'ApiCall fallisce con
`Failed/ResponseTooLarge` e `truncated: true`. Nello status il body occupa al massimo
64 KiB. Con `spec.keepJSONPointers` (una lista di JSON Pointer) una risposta JSON viene
salvata ridotta ai soli valori indicati, ciascuno al suo posto (negli array gli altri
elementi diventano `null`, così gli indici restano validi). Il puntatore di `retry.until`
viene sempre conservato. I Workflow impostano da soli questa lista con i campi letti dallo
step (`outputs`, `expect`, la condizione del poll, `recover`): una risposta grande, per
esempio un job con il suo log, non fa quindi fallire lo step. Se quello che resta supera
comunque 64 KiB, l'esito della chiamata non cambia, ma `truncated: true` indica che il
body salvato è parziale. Lo step fallisce con `ResponseUnavailable` solo se doveva leggerlo. Le risposte HTTP di errore sono conservate, mentre gli altri header
remoti (potenzialmente sensibili, ad esempio `Set-Cookie`) non vengono salvati.

Un `202 Accepted` conclude la **chiamata HTTP**, non necessariamente il lavoro
remoto. Il polling di un job remoto richiede un protocollo specifico e non è
implementato implicitamente.

### Idempotenza, riavvii e sicurezza

- La spec di un ApiCall è **interamente immutabile** (`self == oldSelf`): l'API
	Kubernetes rifiuta qualsiasi modifica dopo la creazione, `spec.endpoint`
	compreso. Un ApiCall è la traccia di una singola esecuzione; per una richiesta
	diversa serve un nuovo ApiCall, normalmente creato da un Workflow a partire da un
	ApiCallTemplate (che invece resta modificabile). Riconciliazioni, aggiornamenti
	dello stato e modifiche dei metadati non ripetono la chiamata.
- Non ci sono retry applicativi automatici né redirect. Il transport Go può
	ritentare autonomamente alcune richieste idempotenti su connessioni riutilizzate.
	Usare un `Idempotency-Key` stabile negli header solo se l'API remota garantisce
	deduplicazione, e scegliere una nuova chiave per un'operazione nuova.
- Un riavvio con una CR `Running` produce `Unknown`, **non un replay**. Anche
	un crash prima dell'invio ma dopo il checkpoint può produrre `Unknown`.
	Senza un protocollo di idempotenza remoto non è possibile garantire
	exactly-once né ricostruire un risultato HTTP perso. Verificare l'effetto
	remoto prima di creare un nuovo ApiCall per riprovare.
- Cancellazione della CR, nuova generazione e arresto del manager annullano
	l'attesa locale. Questo **non annulla** effetti già avvenuti sul server.
	Nessun finalizer è aggiunto: una richiesta HTTP non può garantire un rollback
	remoto. Un nuovo tentativo può sovrapporsi a un'operazione ancora attiva sul server.
- `status` non è un Secret. Il body remoto potrebbe contenere credenziali o dati
	personali: usare `storeResponseBody: false` e restringere il RBAC delle CR.
	Non mettere credenziali in endpoint, body o header pubblici; usare Secret.
- La allowlist autorizza schema, host e porta, ma **non blocca DNS rebinding** o
	cambiamenti degli IP risolti. Consentire soltanto origini fidate e usare una
	NetworkPolicy/egress policy per bloccare metadata cloud e destinazioni vietate.
	Una origine autorizzata autorizza tutti i suoi percorsi.
- L'operator ha RBAC `get` sui Secret. Chi può creare ApiCall può chiedergli di
	utilizzare un Secret della stessa namespace verso una destinazione autorizzata.
	Limitare queste autorizzazioni a utenti e namespace fidati; l'allowlist non
	sostituisce il controllo degli accessi ai Secret.

CRD, RBAC e DeepCopy sono generati con `make manifests generate`. `make test`
verifica schema CRD, esecuzione non bloccante, autenticazione TLS, metodi HTTP,
timeout, cancellazione, limiti della risposta, deduplicazione e recupero `Unknown`.

## Mock API per debug in-cluster

Per verificare cosa invia l'operator, il repository include un server mock
indipendente in [cmd/mock-api/main.go](cmd/mock-api/main.go) e il deployment
opzionale in [config/debug-api](config/debug-api). Il server registra le ultime
100 richieste in memoria e restituisce i dettagli della richiesta. Gli header di
autenticazione, cookie, API key, token e campi sensibili nei body JSON/form sono
oscurati. Le richieste non persistono tra riavvii; `DELETE /__debug/requests`
cancella lo storico.

### Avvio locale

Usare `make mock-api-test` per i test, `make mock-api-run` per ascoltare su
`localhost:8080`, oppure `make mock-api-image` per creare l'immagine
`latch-mock-api:dev`. Endpoint utili:

| Endpoint | Comportamento |
| --- | --- |
| `GET /healthz` | Readiness/liveness. |
| Qualsiasi metodo, per esempio `/echo` | Registra e restituisce metodo, path, query, header e body. |
| `POST /oauth/token` | Simula OAuth Client Credentials e restituisce un token fittizio. Registra la richiesta oscurando client secret e header Basic. |
| Qualsiasi metodo `/status/401` (o altro status 200–599) | Simula una risposta HTTP con quello status e include la richiesta catturata. |
| `GET /__debug/requests?limit=20` | Restituisce le ultime richieste acquisite, fino a 100. |
| `DELETE /__debug/requests` | Svuota lo storico. |

### Avvio nel cluster

1. Costruire e caricare l'immagine nel cluster locale (per esempio Kind):

	 ```sh
	 make mock-api-image
	 kind load docker-image latch-mock-api:dev
	 make debug-api-deploy
	 ```

	 Per un cluster remoto, pubblicare l'immagine in un registry raggiungibile e
	 aggiornare `image` in `config/debug-api/mock-api.yaml`.

2. Per testare solo richieste **senza credenziali**, autorizzare l'origine HTTP
	 sul manager, poi applicare la CR. Il DNS del Service è
	 `mock-api.apicall-debug.svc.cluster.local:8080` e il suo endpoint `/echo` è
	 raggiungibile dal pod operator. Esempio di CR da salvare come file temporaneo:

	 ```yaml
	 apiVersion: dinova.one/v1alpha1
	 kind: ApiCallTemplate
	 metadata:
		 name: mock-echo
	 spec:
		 method: POST
		 endpoint: http://mock-api.apicall-debug.svc.cluster.local:8080/echo
		 headers:
			 Content-Type: application/json
			 X-Debug-Value: hello
		 body: '{"message":"test da ApiCall"}'
	 ---
	 apiVersion: dinova.one/v1alpha1
	 kind: WorkflowTemplate
	 metadata:
		 name: mock-echo-run
	 spec:
		 steps:
			 - name: echo
				 api:
					 templateRef:
						 name: mock-echo
	 ---
	 apiVersion: dinova.one/v1alpha1
	 kind: Workflow
	 metadata:
		 name: mock-echo-run
	 spec:
		 templates:
			 - name: run
				 templateRef:
					 name: mock-echo-run
	 ```

	 Aggiungere `--apicall-allowed-origins=http://mock-api.apicall-debug.svc.cluster.local:8080`
	 agli argomenti del manager (oltre a `--leader-elect` e ai normali argomenti
	 esistenti), quindi applicare le CR. Il template `mock-echo` da solo non fa nulla;
	 la chiamata parte con il Workflow. Leggere risultato e fase della copia figlia
	 con `kubectl get apicall -l dinova.one/workflow=mock-echo-run -o yaml`.

3. **Autenticazione e OAuth richiedono HTTPS anche nei test.** Generare una CA/
	 certificato effimero con SAN del Service; non disattivare la verifica TLS:

	 ```sh
	 openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
		 -keyout /tmp/mock-api.key -out /tmp/mock-api.crt \
		 -subj '/CN=mock-api.apicall-debug.svc.cluster.local' \
		 -addext 'subjectAltName=DNS:mock-api.apicall-debug.svc.cluster.local'
	 kubectl create secret tls mock-api-tls -n apicall-debug \
		 --cert=/tmp/mock-api.crt --key=/tmp/mock-api.key
	 kubectl rollout restart deployment/mock-api -n apicall-debug
	 ```

	 Lo stesso certificato autofirmato può essere usato come CA di test. Creare
	 un ConfigMap nel namespace del manager e applicare la patch pronta all'uso:

	 ```sh
	 kubectl create configmap apicall-debug-ca -n latch-system \
	   --from-file=ca.crt=/tmp/mock-api.crt
	 kubectl patch deployment latch-controller-manager \
	   -n latch-system --type=strategic \
	   --patch-file=config/debug-api/operator-tls-patch.yaml
	 ```

	 La patch monta la CA e configura allowlist e `--apicall-ca-file` senza
	 disabilitare la verifica TLS. Applicare poi il CR OAuth di esempio in
	 [config/debug-api/apicall-oauth-sample.yaml](config/debug-api/apicall-oauth-sample.yaml).
	 Il manifest usa solo credenziali fittizie ed entrambi i passaggi
	 `/oauth/token` e `/echo` sono visibili nello storico.

4. Per ispezionare le richieste dall'ambiente locale, fare port-forward al
	 Service e interrogare `GET /__debug/requests`. Con il certificato di test,
	 `curl` locale deve usare `-k` **solo per il port-forward di debug**; questo non
	 cambia la verifica TLS dell'operator. In alternativa leggere i log con
	 `kubectl logs -n apicall-debug deployment/mock-api`.

Il mock è HTTP senza certificati e passa a HTTPS quando trova il Secret TLS
opzionale. È uno strumento di sviluppo, senza autenticazione sull'interfaccia di
debug e con risposte/catture in memoria: distribuirlo solo in cluster di test,
non esporlo con Ingress/LoadBalancer e non usarlo in produzione. Anche con
redazione, inviare soltanto credenziali fittizie.

## Tilt: stack locale operatore + mock

Il [Tiltfile](Tiltfile) prepara lo stack completo sul cluster Kind locale
`kind-er-operator` (già verificato disponibile): CRD/RBAC/operator, mock API e
un ApiCallTemplate `tilt-mock-echo` e il Workflow `tilt-mock-echo-run` che lo richiama. Tilt costruisce **entrambe le immagini in
locale**; per l'operator il [Dockerfile Tilt](Dockerfile.tilt) usa i checkout
fratelli locali `../api-engine` e `../terraform-engine` tramite BuildKit, senza
registry remoto. Nessun Ingress o LoadBalancer viene creato.

Prerequisiti: Tilt, Docker/BuildKit, `kubectl`, cluster Kind attivo chiamato
`kind-er-operator`, e i due moduli condivisi presenti in `/code/api-engine` e
`/code/terraform-engine`. Dalla root del repository avviare:

```sh
tilt up --context kind-er-operator
```

Tilt mantiene la sessione aperta e mostra build, stato dei pod e log del mock e
dell'operator; l'interfaccia web si apre normalmente su
`http://localhost:10350`. La prima build scarica le immagini base Go e
Distroless; le immagini applicative sono costruite localmente e caricate nel
cluster Kind da Tilt. Gli aggiornamenti al sorgente ricostruiscono l'immagine e
aggiornano il deployment. Tilt crea inoltre un port-forward da
`localhost:8088` al mock per consultare lo storico delle richieste.

Il Workflow `tilt-mock-echo-run` crea un ApiCall dal template `tilt-mock-echo` e
invia una POST al Service interno (il template da solo non viene eseguito). Per controllare risultato e richiesta catturata:

```sh
kubectl get workflow tilt-mock-echo-run -o yaml
kubectl get apicall -l dinova.one/workflow=tilt-mock-echo-run -o yaml
kubectl logs -n apicall-debug deployment/mock-api
```

Lo stack include anche tutto il necessario per gli step Terraform
([config/tilt/tfstate-storage.yaml](config/tilt/tfstate-storage.yaml)): un **MinIO** nel
namespace `tfstate-storage` con il bucket `tfstate` (console su `localhost:9001`, S3 su
`localhost:9000`), i flag `--tfstate-s3-*` e le credenziali di sviluppo dell'operator, il Secret
`tfstate-credentials` per i Job e l'immagine del runner, costruita e caricata nel Kind dal
Tiltfile. Il Kind non riesce a scaricare l'immagine di MinIO (il registry risponde 401) né
`kind load` riesce a caricarla perché è multi-piattaforma, quindi il Tiltfile la importa dal
Docker locale con `docker save | ctr images import`. Il workflow `tilt-api-then-terraform`
([workflow-terraform.yaml](config/tilt/workflow-terraform.yaml)) legge una chiamata API e ne usa il
risultato per avviare Terraform: lanciandone un altro con lo stesso nome del VDC si vede la
risorsa modificata sullo stesso state (`will be updated in-place`):

```sh
kubectl get workflow tilt-api-then-terraform -o yaml
kubectl logs job/<job-del-workflow>
```

Queste credenziali e lo storage (emptyDir) sono **solo per sviluppo**.

Il Workflow `tilt-vdc-network` ([workflow-sequence.yaml](config/tilt/workflow-sequence.yaml))
esegue in sequenza due ApiCallTemplate distinti: `POST /vdcs` crea un VDC e restituisce
l'`id`, poi `POST /networks` riceve quell'`id` come `vdcId` tramite `outputs`/`inputs`. Il mock
risponde 404 se il `vdcId` non esiste, quindi l'ordine è verificato davvero:

```sh
kubectl get workflow tilt-vdc-network
kubectl get apicall -l dinova.one/workflow=tilt-vdc-network
```

Il body inviato e la risposta sono nello status della CR; lo storico redatto è
consultabile in `http://localhost:8088/__debug/requests` oppure dal pod mock.
Per fermare Tilt premere Ctrl+C; questo **lascia le risorse nel cluster** per
poter ispezionare lo stato.
Per eliminarle eseguire:

```sh
tilt down --context kind-er-operator
```

Il target Docker locale si può verificare separatamente con
`make mock-api-image`; Tilt usa invece l'immagine con tag `:tilt` per non
confonderla con il deployment debug manuale.
