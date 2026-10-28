# Catena delle CRD di latch

Come si collegano le risorse di latch, chi le scrive e dove finiscono i dati, per gli step
Terraform e per gli step API. Il dettaglio di ogni campo è nei tipi in
[api/v1alpha1](api/v1alpha1) e nel [README](README.md).

## Le CRD

| CRD | Ruolo | Chi la scrive | Modificabile |
|---|---|---|---|
| [TerraformTemplate](api/v1alpha1/terraformtemplate_types.go) | Elenco dei file Terraform, letti da ConfigMap immutabili | Tu | Sì; l'operatore ne fa revisioni numerate |
| [ApiCallTemplate](api/v1alpha1/apicalltemplate_types.go) | Una richiesta HTTP riusabile (metodo, endpoint, body, header, autenticazione) | Tu | Sì |
| [WorkflowTemplate](api/v1alpha1/workflowtemplate_types.go) | Parametri e sequenza di step `terraform` e `api` (più i `deleteSteps`) | Tu | Sì, ma non mentre un Workflow lo esegue (`TemplateChanged`) |
| [Workflow](api/v1alpha1/workflow_types.go) | **Una** esecuzione di uno o più template | Tu, oppure una WorkflowInstance | No: è la registrazione di un'esecuzione |
| [ApiCall](api/v1alpha1/apicall_types.go) | **Una** richiesta HTTP, figlia di un Workflow | L'operatore | No |
| [WorkflowInstance](api/v1alpha1/workflowinstance_types.go) | Stato desiderato di un cliente o progetto: tiene applicato un template | Tu, oppure una WorkflowPipeline | Sì |
| [WorkflowPipeline](api/v1alpha1/workflowpipeline_types.go) | Un processo in fasi: istanze e Workflow nell'ordine delle dipendenze, con gli output passati tra le fasi | Tu | Sì |

Oggetti Kubernetes standard usati lungo la catena: ConfigMap (codice Terraform e sue copie),
Secret (credenziali, variabili, output sensibili, testo dei plan), ControllerRevision
(revisioni dei TerraformTemplate), PVC e Job (esecuzione di Terraform).

## Catena Terraform

```
ConfigMap  (immutable: true)              file .tf / .tf.tmpl                      ← tu
   │ configMapKeyRef
TerraformTemplate                         elenco dei file                         ← tu
   ├─► ControllerRevision                 snapshot immutabile dei file, numerato  ← operatore
   │ templateRef
WorkflowTemplate                          step terraform: chiave dello state,     ← tu
   │                                      inputs, outputs, envFromSecrets
   │ templateRef + parametri
Workflow                                  un'esecuzione                           ← tu / WorkflowInstance
   │ per ogni step, in ordine:
   ├─► ConfigMap copia dei file           immutabile: la revisione eseguita       ← operatore
   ├─► Secret delle variabili             terraform.auto.tfvars.json dagli inputs ← operatore
   ├─► PVC di lavoro                      file renderizzati e provider            ← operatore
   └─► Job del runner                     terraform-engine: render, init,         ← operatore
          │                               plan / apply
          ├─► file eseguiti su S3         <namespace>/rendered/<workflow>-<uid>/…
          ├─► state su S3                 <namespace>/<chiave>.tfstate
          └─► plan su S3 (modo Plan)      <namespace>/plans/<workflow>-<uid>/…
   output letti dallo state ──► status dello step (Secret se sensibili) ──► step successivi
```

**Codice immutabile e revisioni.** Il codice Terraform sta solo in ConfigMap con
`immutable: true`: l'operatore rifiuta le altre. Ogni volta che il contenuto dei file di un
TerraformTemplate cambia (ConfigMap ricreata, o template che punta a un'altra ConfigMap)
l'operatore ne fa uno snapshot in una ControllerRevision con il numero successivo. File
identici a una revisione ancora conservata riportano quella revisione (rollback). Lo step
esegue la revisione corrente, la copia in una ConfigMap sua e registra nello status del
Workflow numero e digest della revisione eseguita.

**State.** Lo state non appartiene al Workflow: è un oggetto S3 individuato da
`<namespace>/<state.key>.tfstate`. Tutti i Workflow con la stessa chiave lavorano sulle stesse
risorse. La chiave può contenere `{{parameters.<nome>}}` (per esempio il cliente) e
`{{steps.<step>.outputs.<nome>}}`.

**Credenziali.** I Secret elencati in `envFromSecrets` diventano variabili d'ambiente del Job:
quelle dello storage S3 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) e quelle del provider
come `TF_VAR_<nome>`.

### Modalità di un Workflow (`spec.terraformMode`)

| Modalità | Cosa fa |
|---|---|
| `Apply` (default) | Per ogni step fa plan e apply |
| `Plan` | Solo plan, salvato su S3. Uno step senza differenze pubblica gli output dal suo state e si passa al successivo; il primo step con differenze ferma il template, gli step dopo sono `Skipped`. Il testo del plan va in un Secret |
| `ApplyPlan` | Applica **esattamente** il plan salvato indicato in `spec.applyPlan`. Gli step prima leggono solo gli output dallo state, quelli dopo sono `Skipped` |
| `Destroy` | Distrugge quello che gli step gestiscono. Prima legge in ordine gli output di ogni step dal suo state (servono alle variabili e alle chiavi degli step dopo), poi distrugge gli step dall'ultimo al primo. Uno step senza state, o con lo state vuoto, è `Skipped` |

`Plan`, `ApplyPlan` e `Destroy` accettano un solo template, con soli step Terraform.

### WorkflowInstance

```
WorkflowInstance  (templateRef, parametri, interval, approval)
   │ crea un Workflow <istanza>-<n> per ogni run, uno alla volta
   ├─ subito, quando cambia la revisione: ConfigMap, TerraformTemplate,
   │  WorkflowTemplate o parametri (l'istanza li osserva, non aspetta il timer)
   └─ a ogni interval, anche se non cambia niente: corregge le modifiche fatte a mano
```

- **`approval: Auto`:** ogni run è un Workflow `Apply`.
- **`approval: Manual`:** ogni run è un Workflow `Plan`. Se il plan ha differenze l'istanza
  passa a `Drifted=True` e le mostra in `status.pendingPlan`. Per applicarle:
  ```bash
  kubectl get secret <pendingPlan.secret> -o jsonpath='{.data.plan\.txt}' | base64 -d
  kubectl patch workflowinstance <istanza> --type merge -p '{"spec":{"approvePlan":"<pendingPlan.id>"}}'
  ```
  L'operatore lancia un Workflow `ApplyPlan`, poi subito un nuovo `Plan` per gli step
  successivi e per verificare il risultato. Un'approvazione vale solo per quel plan e un plan
  non viene mai applicato due volte. Se lo state è cambiato nel frattempo, Terraform rifiuta il
  plan come "stale".
- **Storico:** restano gli ultimi run riusciti e falliti (default 3 e 3).
- **Pulizia dei plan:** i file di un plan su S3 vengono cancellati appena è applicato, e
  comunque quando il Workflow `Plan` che li ha salvati viene cancellato (finalizer
  `dinova.one/plan-cleanup`).
- **Step API solo se ripetibili:** l'istanza accetta uno step `api` solo se è una GET o è
  marcato `idempotent`, e allora solo con `approval: Auto` e `deletionPolicy: Retain`; rifiuta
  sempre i `deleteSteps`. Gli ApiCallTemplate degli step entrano nella revisione.
- **Cancellazione (`deletionPolicy`):** con `Retain` (default) cancellare l'istanza lascia le
  risorse e gli state. Con `Destroy` l'operatore aspetta il run in corso, poi lancia un Workflow
  `Destroy` (`<istanza>-destroy-<uid>`, in `status.destroyRun`) e tiene l'istanza (finalizer
  `dinova.one/instance-destroy`) finché non riesce. Quel Workflow non appartiene all'istanza e
  resta come registrazione della distruzione.

## Catena API

```
ApiCallTemplate                           metodo, endpoint, body, header,         ← tu
   │                                      autenticazione, timeout
   │ templateRef
WorkflowTemplate                          step api: inputs (body, query, path),   ← tu
   │                                      outputs, expect, retry, poll;
   │                                      deleteSteps
   │ templateRef + parametri
Workflow                                  un'esecuzione                           ← tu
   │ per ogni step, in ordine:
   └─► ApiCall                            la richiesta con gli inputs applicati,  ← operatore
          │                               immutabile, figlia del Workflow
          └─ controller ApiCall           api-engine esegue la chiamata
                                          (origini ammesse, retry)
   output estratti dalla risposta (JSON Pointer, select) ──► status (Secret se sensibili) ──► step successivi
```

- **Inputs:** un input prende il valore da un parametro, da un output di uno step precedente o
  da una chiave di ConfigMap, e lo scrive nel body (`to`), nella query (`toQuery`) o nel path
  (`toPath`). `asString`, `asNumber` e `asJSON` convertono il valore.
- **Ripresa di un job:** un `poll` con `recover` riprende con una terza API il job che è
  fallito, dalla fase dopo l'ultima completata, e poi ricomincia ad aspettare. Lo fa più volte,
  finché il job avanza. Ogni ripresa è un nuovo ApiCall, elencato in `status.recoveries` dello
  step.
- **Ripetizione:** un ApiCall non si riesegue mai. Rilanciare significa creare un nuovo
  Workflow, che crea nuovi ApiCall.
- **Cancellazione:** quando si cancella il Workflow girano i `deleteSteps` (solo api),
  dall'ultimo template al primo. Il finalizer `dinova.one/workflow-deletion` trattiene il
  Workflow finché non riescono.
- **Istanza solo se ripetibili, niente plan:** una `POST` ripetuta crea ogni volta una risorsa
  nuova e non ha un "plan". Gli step API entrano in una WorkflowInstance solo se sono GET o
  `idempotent`, e restano fuori da `Plan`, `ApplyPlan` e `Destroy`.

## Dove si incontrano

Un WorkflowTemplate può alternare step `api` e `terraform`, che si passano dati con gli
output. Esempio in [config/tilt/workflow-terraform.yaml](config/tilt/workflow-terraform.yaml):
una chiamata API crea un VDC, e i suoi output diventano le variabili e la chiave dello state
dello step Terraform. Un template misto si esegue come Workflow `Apply` (a mano o come fase
Workflow di una pipeline), o in una WorkflowInstance se i suoi step API sono ripetibili.

| | Workflow `Apply` | Workflow `Plan` / `ApplyPlan` / `Destroy` | WorkflowInstance |
|---|---|---|---|
| Step `terraform` | Sì | Sì | Sì |
| Step `api` | Sì | No | Solo GET o `idempotent` (con `approval: Auto`, `deletionPolicy: Retain`) |
| `deleteSteps` | Sì | No | No |

### WorkflowPipeline

Per un processo fatto di parti Terraform da tenere applicate e di chiamate API una tantum, una
WorkflowPipeline mette in fila istanze e Workflow:

```
WorkflowPipeline  (parametri, fasi con dependsOn)
   │ crea, quando le fasi da cui dipende sono pronte:
   ├─► WorkflowInstance <pipeline>-<fase>       fase instance: Terraform, tenuta applicata
   │      └─► Workflow <pipeline>-<fase>-<n>    i suoi run
   └─► Workflow <pipeline>-<fase>-<n>           fase workflow: una volta (o in spec.rerun)
   output dallo status dei run (valueFrom) ──► parametri delle fasi successive
```

- Un output cambiato a monte cambia la revisione delle istanze a valle, che riapplicano da sole.
  Una fase Workflow diventa invece `Outdated`, e si rilancia scrivendo il suo run in
  `spec.rerun`, o da sola con `rerunOnChange` se ogni suo step API è una GET o `idempotent`.
- Gli output sensibili non passano tra le fasi.
- Cancellare la pipeline cancella le fasi in ordine inverso alle dipendenze.

Dettagli nel [README](README.md#workflowpipeline-un-processo-in-più-fasi); esempio in
[config/samples/newk8scluster/](config/samples/newk8scluster/pipeline.yaml).

## Dove finiscono i dati

| Dato | Dove |
|---|---|
| Codice Terraform | ConfigMap immutabili; snapshot in ControllerRevision; copia per ogni step |
| Variabili di uno step | Secret `…-tf-input` del Workflow |
| State Terraform | S3: `<namespace>/<chiave>.tfstate` (lo legge anche l'operatore, per gli output) |
| Plan salvati | S3: `<namespace>/plans/<workflow>-<uid>/<template>/<step>.tfplan` (+ `.txt`, `.json`) |
| File eseguiti da uno step (`.tf` renderizzati, variabili, `manifest.json`) | S3: `<namespace>/rendered/<workflow>-<uid>/<template>/<step>/`, indicato in `status…steps[].renderedFiles`; l'operatore non li cancella |
| Testo di un plan | Secret `…-plan` del Workflow, chiave `plan.txt` |
| Output non sensibili | Status del Workflow (`status.templates[].steps[].outputs`) |
| Output sensibili | Secret degli output del Workflow |

## Limiti noti

- **Cancellare non distrugge.** Cancellare un Workflow o una WorkflowInstance non distrugge le
  risorse create da Terraform né gli state. Per Terraform non c'è ancora un destroy alla
  cancellazione.
- **Plan senza differenze.** I loro file restano su S3 finché il run non esce dallo storico
  dell'istanza.
- **Uno state, un solo run.** Un'istanza e un Workflow lanciato a mano sulla stessa chiave
  dello state non devono girare insieme: il secondo fallisce sul lock dello state.
