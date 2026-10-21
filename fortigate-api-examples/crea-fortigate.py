#!/usr/bin/env python3
"""Creazione di un FortiGate VM dal portale — wizard completo da riga di comando.

Gemello di `deploy_vm_linux.py` per l'altro modulo che crea macchine: la' una VM
Linux, qui un FortiGate clonato da un template, licenziato, configurato via
Ansible, depositato in DVLS e censito in iTop. L'API e' la stessa del wizard
dell'interfaccia — nessun endpoint privato, nessuna scorciatoia.

Due modi di usarlo, ed e' deliberato che siano due:

  * **come programma**, che guida passo per passo — ambiente, template, cliente,
    licenza, dimensionamento, rete, destinazione — e alla fine chiede conferma:

        export PORTAL_API_KEY='...'
        python3 crea_fortigate.py --portale https://portal.admin.dinova.cloud

  * **come modulo**, importando `Portale` e `corpo_delivery()`: il wizard serve a
    comporre le scelte una prima volta, poi quelle scelte si salvano
    (`--salva-scelte scelte.json`) e si rieseguono senza nessuno davanti
    (`--da-file scelte.json --conferma`). E' la forma che serve a un CI.

⚠️ **Di default non crea niente.** Senza `--conferma` il wizard arriva in fondo,
stampa il corpo che manderebbe e si ferma: un'anteprima costa zero, un FortiGate
creato per sbaglio costa un entitlement FortiFlex consumato, un IP in iTop, due
connessioni in DVLS e una dismissione.

⚠️ **La chiave sta in `PORTAL_API_KEY`, non qui dentro.** E' una credenziale, e
il gate dei segreti della pipeline riconosce queste chiavi dal loro prefisso:
scriverla in un file lo fa fallire — il che e' il comportamento voluto.

⚠️ **Serve `fortigate_vm: OPERATOR`.** Il wizard si percorre con il solo VIEWER —
tutte le letture lo accettano — e il `POST .../deliver` risponde **403**: cioe'
il rifiuto arriva nel punto peggiore, dopo aver compilato tutto. Per questo la
chiave si prova **prima** di qualunque altra cosa (vedi `main`).

Cosa fa davvero, in ordine, e perche':

   1. `GET /envs`                      l'ambiente di delivery. **Il suo `env_type`
                                       decide tutto il resto**: `vcenter` e
                                       `vcloud` sono due percorsi diversi, con due
                                       endpoint di creazione diversi
   2. `GET /templates`                 il template, censito a mano nei Parametri.
                                       Porta con se' gli IP di provisioning, le
                                       porte e le credenziali native: non si
                                       scansiona una cartella, si sceglie una riga
   3. `GET /flexvm/programs|configs`   la licenza, se FlexVM
   4. `GET /vcenter/{id}/...`          rete e destinazione su vCenter:
                                       `nsx-p2p-segments` + `p2p-segment-ips`
                                       oppure `portgroups` + `itop/subnets` +
                                       `itop/free-ips`, poi `resource-pools` e
                                       `folders`
      `GET /vcloud/{id}/...`           oppure su vCloud: `orgs`, `vdcs`,
                                       `networks`, `network-ips`
   5. `POST /vcenter|vcloud/{id}/deliver`  risponde **202**: e' una promessa, non
                                       un esito
   6. `GET /jobs/{id}`                 si segue finche' non dice `success` o
                                       `failed` — **quattordici** fasi, due
                                       riavvii del FortiGate, decine di minuti
   7. `POST /jobs/{id}/license-decision`   due punti in cui il job **si ferma e
      `POST /jobs/{id}/src-ip`             aspetta una persona**: vedi sotto
   8. `POST /jobs/{id}/retry`          la ripresa, dalla fase scelta
   9. `DELETE /envs/{id}/nsx-segment`  la pulizia di un segmento P2P **creato e
                                       mai usato**: vedi sotto. Qui non serve —
                                       questo esempio i segmenti li sceglie, non
                                       li crea — ma serve a chi li crea

⚠️⚠️ **Un template e' in uso da una delivery alla volta, e il rifiuto non e'
gratuito.** Il portale prende un lock sul template **dopo** aver creato la riga
del job: se quel lock e' gia' di qualcun altro risponde `409 TEMPLATE_LOCKED` e
lascia in cronologia un job **fallito** che nessuno ha chiesto. Per questo qui il
lock si guarda **prima** di mandare la richiesta (`locked_by_job_id` nella voce
del template), e un template occupato non si propone affatto.

⚠️ **Il percorso lo decide l'ambiente, e sbagliarlo non viene rifiutato.**
`POST /vcloud/{id}/deliver` verifica che l'ambiente sia di tipo vCloud; il
gemello `POST /vcenter/{id}/deliver` **non** verifica il contrario — mandandogli
un ambiente vCloud la delivery parte e si ferma su vCenter, senza VDC, senza rete
importata e senza vApp. Qui il percorso si deriva da `env_type`, mai dal
chiamante.

⚠️ **Il job si ferma due volte, e in entrambi i casi aspetta una persona.**
`waiting_license_decision` — un entitlement FlexVM risulta gia' **attivo** e
qualcuno deve dire se terminarlo o lasciarlo e crearne un altro — e
`waiting_src_ip`, dove va scelto l'indirizzo transitorio di provisioning. Un
programma che guardi solo `success`/`failed` resta appeso fino al proprio timeout
e dichiara un fallimento che non c'e'. ⓘ Il secondo oggi non lo scrive nessuna
fase del portale: gli endpoint e il recupero all'avvio esistono, cio' che lo
produceva no. Si gestisce lo stesso — reagire costa cinque righe, ignorare uno
stato vuol dire un job che aspetta per sempre.

⚠️ **La password dell'account FortiCloud non entra nelle scelte salvate.** Serve
alla sola fase 4, il portale la deposita cifrata e la azzera appena la licenza e'
applicata: scriverla in `scelte.json` la lascerebbe in chiaro sul disco di
chiunque rieseguira' quel file. Si chiede ogni volta, e con `--da-file` si
richiede alla ripresa.

⚠️⚠️ **Chi CREA un segmento P2P da programma deve saperlo anche rimuovere.**
Questo esempio non lo fa: sceglie fra quelli gia' censiti, quindi non lascia
niente dietro di se'. Ma il segmento nasce **prima** della delivery — e' un
prerequisito, non un suo effetto — quindi chi lo crea con
`POST /api/v1/p2p/segments` e poi si ferma (un rifiuto piu' avanti, un errore,
una scelta cambiata) ha gia' prodotto tre oggetti su tre sistemi: il segmento su
NSX, il T1 creato con lui, e in iTop la subnet con i suoi indirizzi **allocati**.
Nessun errore lo segnala, e «Interrompi delivery» non li vede: agisce su un job
che non esiste ancora. Successo il 13/10/2026 con `178.239.21.34/31`, ripulito a
mano. Il rimedio e' `DELETE /envs/{env_id}/nsx-segment?name=<display_name>`
(`fortigate_vm: OPERATOR`), che elimina i tre insieme — e rifiuta un segmento che
**non** sia un residuo: nome fuori dalla convenzione dell'automazione (422),
porte attaccate o un secondo segmento sul suo T1 (409). Un segmento che non
esiste piu' risponde 200 `not_found`, perche' e' lo stato di arrivo.

⚠️ E per la stessa ragione **l'elenco dei segmenti puo' contenere dei residui**:
un segmento abbandonato da un wizard interrotto e' vuoto, quindi compare fra
quelli scegliibili e usarlo e' legittimo — ma se si sta cercando «un segmento
libero» vale la pena guardare se qualcuno lo stesse creando per se'.

Cosa questo esempio **non** fa, e va saputo: non crea segmenti P2P
(`POST /api/v1/p2p/segments`, modulo `p2p_segment`) ne' li rimuove
(`DELETE /envs/{id}/nsx-segment`, vedi sopra), non spegne, non riattiva e non
dismette un FortiGate gia' consegnato
(`POST /jobs/{id}/{power-off,reactivate,dismiss}`), non importa un apparato
esistente da DVLS (`GET /dvls-fortigates`, `POST /import`) e non crea ambienti
ne' template (`POST /envs`, `POST /templates`, ADMIN). Sono chiamate singole:
manca il giro del wizard, non la capacita' di farle.

Nessuna dipendenza fuori dalla libreria standard: gira su qualunque Python 3.11
senza un `pip install` da spiegare a chi lo riceve.
"""

from __future__ import annotations

import argparse
import getpass
import ipaddress
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# ─────────────────────────── Il client ──────────────────────────────────────


class ErrorePortale(RuntimeError):
    """Un rifiuto del portale, con dentro cio' che serve per capire il perche'.

    Il portale risponde sempre nella stessa forma (`code`, `message`, `detail`),
    ed e' il `code` che distingue un caso dall'altro: `TEMPLATE_LOCKED` si
    rimedia aspettando, `TEMPLATE_ENV_MISMATCH` scegliendo un altro template,
    `VCENTER_ERROR` guardando l'ambiente. Confrontare il testo del messaggio
    funzionerebbe finche' qualcuno non lo riscrive."""

    def __init__(self, stato: int, codice: str, messaggio: str, dettaglio: Any = None) -> None:
        super().__init__(f"HTTP {stato} [{codice}] {messaggio}")
        self.stato = stato
        self.codice = codice
        self.messaggio = messaggio
        self.dettaglio = dettaglio


class Portale:
    """Client sottile sull'API del portale: nessuna logica, solo le chiamate.

    Sottile di proposito — cio' che decide sta nel portale, e una seconda copia
    delle sue regole qui dentro divergerebbe al primo cambiamento senza che
    nessuno se ne accorga."""

    def __init__(
        self,
        base: str,
        api_key: str,
        *,
        verifica_tls: bool = True,
        timeout: int = 120,
    ) -> None:
        if not api_key:
            raise ValueError(
                "Nessuna API key: si passa nell'ambiente, in PORTAL_API_KEY, "
                "mai come argomento — la riga di comando e' leggibile in `ps`."
            )
        self.base = base.rstrip("/")
        self._api_key = api_key
        # 120 secondi, il doppio del gemello Linux: qui una singola lettura puo'
        # attraversare vCenter **e** NSX-T **e** iTop — `p2p-segment-ips` le
        # interroga entrambe — e su queste installazioni quelle risposte
        # arrivano a decine di secondi. Un timeout piu' stretto fallisce proprio
        # mentre il portale sta lavorando.
        self._timeout = timeout
        self._ctx: ssl.SSLContext | None = None
        if not verifica_tls:
            # Solo per gli ambienti con certificato autofirmato (dev, staging).
            # In produzione non si passa di qui: un certificato non verificato
            # rende la chiave intercettabile da chi sta in mezzo.
            self._ctx = ssl.create_default_context()
            self._ctx.check_hostname = False
            self._ctx.verify_mode = ssl.CERT_NONE

    # ── il trasporto ─────────────────────────────────────────────────────────

    def _chiama(
        self,
        metodo: str,
        percorso: str,
        *,
        params: dict | None = None,
        corpo: dict | None = None,
    ) -> Any:
        url = f"{self.base}/api/v1/fortigate-vm{percorso}"
        if params:
            # I valori vuoti si tolgono: un `q=` vuoto e un `q` assente non
            # significano la stessa cosa per tutti gli endpoint.
            puliti = {k: v for k, v in params.items() if v not in ("", None)}
            if puliti:
                url += "?" + urllib.parse.urlencode(puliti)
        dati = json.dumps(corpo).encode() if corpo is not None else None
        req = urllib.request.Request(url, data=dati, method=metodo)
        req.add_header("X-API-Key", self._api_key)
        req.add_header("Accept", "application/json")
        if dati is not None:
            req.add_header("Content-Type", "application/json")
        try:
            # L'URL e' quello del portale, scelto da chi esegue questo esempio sulla
            # propria postazione: non e' un input che arriva da fuori. E questo file
            # e' documentazione — non entra nell'immagine ne' viene importato da nulla.
            # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
            with urllib.request.urlopen(req, timeout=self._timeout, context=self._ctx) as r:
                testo = r.read().decode()
                return json.loads(testo) if testo else None
        except urllib.error.HTTPError as exc:
            raise self._errore(exc) from None
        except urllib.error.URLError as exc:
            raise ErrorePortale(0, "PORTALE_IRRAGGIUNGIBILE", str(exc.reason)) from None

    @staticmethod
    def _errore(exc: urllib.error.HTTPError) -> ErrorePortale:
        """Il rifiuto letto nella forma in cui il portale lo scrive.

        Due forme, e vanno distinte: `api_error()` produce
        `{"detail": {"code", "message", "detail"}}`, mentre una validazione
        Pydantic produce `{"detail": [ ... ]}` — la seconda nomina **quale**
        campo non va, ed e' l'informazione che serve davvero."""
        try:
            corpo = json.loads(exc.read().decode())
        except Exception:
            return ErrorePortale(exc.code, "HTTP", exc.reason or "errore")
        dettaglio = corpo.get("detail") if isinstance(corpo, dict) else corpo
        if isinstance(dettaglio, dict):
            return ErrorePortale(
                exc.code,
                dettaglio.get("code", "HTTP"),
                dettaglio.get("message", ""),
                dettaglio.get("detail"),
            )
        if isinstance(dettaglio, list):
            campi = "; ".join(
                f"{'.'.join(str(p) for p in v.get('loc', [])[1:])}: {v.get('msg', '')}"
                for v in dettaglio
                if isinstance(v, dict)
            )
            return ErrorePortale(exc.code, "VALIDAZIONE", campi or "richiesta non valida")
        return ErrorePortale(exc.code, "HTTP", str(dettaglio))

    # ── il wizard, un endpoint per passo ─────────────────────────────────────

    def ambienti(self) -> list[dict]:
        return self._chiama("GET", "/envs")

    def parametri(self) -> dict:
        """I parametri del modulo, senza i segreti (`has_<campo>_set` al loro posto).

        Qui servono a una cosa sola: sapere se FlexVM e' acceso prima di
        proporlo — `flexvm_enabled` spento vuol dire che `GET /flexvm/programs`
        rispondera' 503, e offrire una scelta che non puo' riuscire e' peggio
        che non offrirla."""
        return self._chiama("GET", "/settings")

    def template(self, vcenter_env_name: str = "") -> list[dict]:
        return self._chiama("GET", "/templates", params={"vcenter_env_name": vcenter_env_name})

    # licenza
    def programmi_flexvm(self) -> list[dict]:
        return self._chiama("GET", "/flexvm/programs")

    def configurazioni_flexvm(self, program_id: str) -> list[dict]:
        return self._chiama("GET", "/flexvm/configs", params={"program_id": program_id})

    # rete e destinazione su vCenter
    def segmenti_p2p(self, env_id: int) -> list[dict]:
        return self._chiama("GET", f"/vcenter/{env_id}/nsx-p2p-segments")

    def indirizzi_p2p(self, env_id: int, network: str, gateway: str) -> list[dict]:
        return self._chiama(
            "GET",
            f"/vcenter/{env_id}/p2p-segment-ips",
            params={"network": network, "gateway": gateway},
        )

    def portgroup(self, env_id: int) -> list[dict]:
        return self._chiama("GET", f"/vcenter/{env_id}/portgroups")

    def resource_pool(self, env_id: int) -> list[dict]:
        return self._chiama("GET", f"/vcenter/{env_id}/resource-pools")

    def cartelle(self, env_id: int) -> list[dict]:
        return self._chiama("GET", f"/vcenter/{env_id}/folders")

    def subnet_itop(self) -> list[dict]:
        return self._chiama("GET", "/itop/subnets")

    def indirizzi_liberi_itop(self, subnet_id: str) -> list[str]:
        return self._chiama("GET", "/itop/free-ips", params={"subnet_id": subnet_id})

    # rete su vCloud
    def organizzazioni(self, env_id: int) -> list[dict]:
        return self._chiama("GET", f"/vcloud/{env_id}/orgs")

    def vdcs(self, env_id: int, org_id: str) -> list[dict]:
        return self._chiama("GET", f"/vcloud/{env_id}/vdcs", params={"org_id": org_id})

    def reti_vcloud(self, env_id: int, vdc_id: str) -> list[dict]:
        return self._chiama("GET", f"/vcloud/{env_id}/networks", params={"vdc_id": vdc_id})

    def indirizzi_vcloud(self, env_id: int, network_id: str) -> dict:
        return self._chiama(
            "GET", f"/vcloud/{env_id}/network-ips", params={"network_id": network_id}
        )

    # ── la delivery e il suo esito ───────────────────────────────────────────

    def delivery(self, percorso: str, env_id: int, corpo: dict) -> dict:
        """Accoda la delivery. Risponde 202: `{job_id, status}`.

        `percorso` e' `vcenter` o `vcloud`, e lo decide `env_type`
        dell'ambiente — mai il chiamante."""
        return self._chiama("POST", f"/{percorso}/{env_id}/deliver", corpo=corpo)

    def job(self, job_id: int) -> dict:
        """Il job con il suo log: `GET /jobs/{id}` lo include, `GET /jobs` no."""
        return self._chiama("GET", f"/jobs/{job_id}")

    def decisione_licenza(self, job_id: int) -> dict:
        return self._chiama("GET", f"/jobs/{job_id}/license-decision")

    def decidi_licenza(self, job_id: int, termina_esistente: bool) -> dict:
        return self._chiama(
            "POST",
            f"/jobs/{job_id}/license-decision",
            corpo={"terminate_existing": termina_esistente},
        )

    def indirizzi_sorgente(self, job_id: int) -> dict:
        return self._chiama("GET", f"/jobs/{job_id}/src-ips")

    def scegli_indirizzo_sorgente(self, job_id: int, ip: str) -> dict:
        return self._chiama("POST", f"/jobs/{job_id}/src-ip", corpo={"src_secondary_ip": ip})

    def riprendi(self, job_id: int, da_fase: int, password_forticloud: str = "") -> dict:
        """Riprende una delivery **fallita** dalla fase scelta.

        ⚠️ `resume_from_step` e' la **prima fase da eseguire**, e il portale
        accetta al massimo `last_completed_step + 1`: non si salta avanti. E con
        `1` su vCloud la VM del tentativo precedente viene eliminata — e' un
        riavvio da capo, non una ripresa."""
        corpo: dict = {"resume_from_step": da_fase}
        if password_forticloud:
            corpo["forticloud_license_account_password"] = password_forticloud
        return self._chiama("POST", f"/jobs/{job_id}/retry", corpo=corpo)


# ─────────────────────────── I menu ─────────────────────────────────────────


def _titolo(testo: str) -> None:
    print(f"\n\033[1m{testo}\033[0m")


def _avviso(testo: str) -> None:
    print(f"  \033[33m⚠ {testo}\033[0m")


def _interattivo() -> bool:
    """C'e' qualcuno davanti?

    ⚠️ Un prompt senza nessuno che risponda e' un processo appeso per sempre —
    e con `--da-file --conferma` questo esempio e' fatto apposta per girare in un
    CI. Dove non c'e' una console si dichiara cosa andrebbe fatto e si esce,
    invece di aspettare una risposta che non arrivera'."""
    return sys.stdin.isatty()


def scegli(voci: list[dict], etichetta, *, titolo: str, predefinito: int = 0, nota=None) -> dict:
    """Un menu numerato. Restituisce la voce scelta, non il suo indice.

    Un elenco vuoto non si presenta come menu: fermarsi dicendo **cosa** manca
    costa una riga e risparmia un errore piu' avanti, dove la causa non si
    riconosce piu'."""
    if not voci:
        raise SystemExit(f"Nessuna voce disponibile per: {titolo}")
    _titolo(titolo)
    for i, voce in enumerate(voci, 1):
        riga = f"  {i:2d}. {etichetta(voce)}"
        if nota:
            extra = nota(voce)
            if extra:
                riga += f"\n      {extra}"
        print(riga)
    if len(voci) == 1:
        print(f"  → unica voce disponibile: {etichetta(voci[0])}")
        return voci[0]
    while True:
        risposta = input(f"  Scelta [1-{len(voci)}, invio = {predefinito + 1}]: ").strip()
        if not risposta:
            return voci[predefinito]
        if risposta.isdigit() and 1 <= int(risposta) <= len(voci):
            return voci[int(risposta) - 1]
        print("  Valore non valido.")


def chiedi(testo: str, predefinito: str = "") -> str:
    suffisso = f" [{predefinito}]" if predefinito else ""
    risposta = input(f"  {testo}{suffisso}: ").strip()
    return risposta or predefinito


def chiedi_obbligatorio(testo: str) -> str:
    while True:
        risposta = input(f"  {testo}: ").strip()
        if risposta:
            return risposta
        print("  Campo obbligatorio.")


def chiedi_intero(testo: str, predefinito: int | None) -> int | None:
    etichetta = str(predefinito) if predefinito is not None else "quello del template"
    while True:
        risposta = input(f"  {testo} [{etichetta}]: ").strip()
        if not risposta:
            return predefinito
        if risposta.isdigit() and int(risposta) > 0:
            return int(risposta)
        print("  Valore non valido.")


def chiedi_si_no(testo: str, predefinito: bool = False) -> bool:
    suggerito = "S/n" if predefinito else "s/N"
    while True:
        risposta = input(f"  {testo} [{suggerito}]: ").strip().lower()
        if not risposta:
            return predefinito
        if risposta in ("s", "si", "y", "yes"):
            return True
        if risposta in ("n", "no"):
            return False
        print("  Rispondere s o n.")


def chiedi_segreto(testo: str) -> str:
    """Una password si chiede senza eco, e non finisce nelle scelte salvate."""
    return getpass.getpass(f"  {testo}: ").strip()


def _netmask(prefisso: int) -> str:
    return str(ipaddress.IPv4Network(f"0.0.0.0/{prefisso}").netmask)


# ─────────────────────────── I passi del wizard ─────────────────────────────


def passo_ambiente(p: Portale, scelte: dict) -> None:
    ambienti = p.ambienti()
    env = scegli(
        ambienti,
        lambda e: f"{e['label']}  ({e['env_type']})",
        titolo="1 · Ambiente di delivery",
        nota=lambda e: (
            f"vCenter: {e['vcenter_env_name']}"
            + (f" · vCloud: {e['vcloud_env_name']}" if e.get("vcloud_env_name") else "")
        ),
    )
    scelte["env_id"] = env["id"]
    scelte["env_label"] = env["label"]
    # ⚠️ Il tipo dell'ambiente non e' un'etichetta: e' quale dei due endpoint di
    # creazione si chiamera'. Si porta nelle scelte perche' `corpo_delivery()`
    # resti una funzione pura, senza dover richiedere gli ambienti per saperlo.
    scelte["env_type"] = env["env_type"]
    scelte["vcenter_env_name"] = env["vcenter_env_name"]


def passo_template(p: Portale, scelte: dict) -> None:
    tutti = p.template(scelte["vcenter_env_name"])
    if not tutti:
        raise SystemExit(
            "Nessun template censito per questo vCenter.\n"
            "  I template si dichiarano a mano in FortiGate Delivery → Parametri: portano con "
            "se' gli IP di provisioning, le porte e le credenziali native."
        )
    # ⚠️ Un template occupato non si propone. Il portale lo rifiuterebbe con
    # `409 TEMPLATE_LOCKED` — ma **dopo** aver creato la riga del job, che resta
    # in cronologia marcata fallita. Il lock lo si legge da qui, e costa niente.
    liberi = [t for t in tutti if not t.get("locked_by_job_id")]
    occupati = [t for t in tutti if t.get("locked_by_job_id")]
    for t in occupati:
        _avviso(
            f"Template '{t['label']}' in uso dal job #{t['locked_by_job_id']} "
            f"({t.get('locked_job_vm_name') or '?'}, {t.get('locked_job_status') or '?'}) "
            "— non selezionabile finche' quella delivery non si conclude."
        )
    if not liberi:
        raise SystemExit("Tutti i template di questo vCenter sono in uso da un'altra delivery.")
    tpl = scegli(
        liberi,
        lambda t: f"{t['label']}  ({t['template_name']})",
        titolo="2 · Template",
        nota=lambda t: (
            f"FortiOS {t.get('fortios_version') or 'non dichiarato'} · "
            f"disco {t.get('disk_gb') or '?'} GB · "
            f"provisioning {t['primary_ip']} / {t['secondary_ip']}"
        ),
    )
    if not tpl.get("has_native_admin_pass_set"):
        _avviso(
            "Il template non ha la password nativa impostata: la fase 5 non potra' entrare. "
            "Si imposta in FortiGate Delivery → Parametri."
        )
    scelte["template_id"] = tpl["id"]
    scelte["template_label"] = tpl["label"]
    scelte["itop_subnet_id_template"] = tpl.get("itop_subnet_id", "")


def passo_identita(scelte: dict) -> None:
    _titolo("3 · Identita' e intestazione")
    print(
        "  Il nome della VM finisce su vCenter, in DVLS e nel censimento iTop;\n"
        "  cliente e contratto restano sul job e si leggono in cronologia."
    )
    scelte["vm_name"] = chiedi_obbligatorio("Nome della VM (2-80 caratteri)")
    scelte["cliente"] = chiedi_obbligatorio("Cliente")
    scelte["rif_contratto"] = chiedi_obbligatorio("Riferimento contratto")


def passo_licenza(p: Portale, scelte: dict) -> None:
    """La licenza: FlexVM, FortiCloud, o nessuna.

    ⚠️ **Nessuna licenza non e' un errore**: la delivery prosegue e il FortiGate
    resta in valutazione. E' una scelta legittima su un apparato di prova, e il
    portale la dichiara nel log della fase 4 invece di fermarsi — ma su un
    apparato che si consegna e' quasi sempre una dimenticanza."""
    parametri = p.parametri()
    modi = [{"key": "nessuna", "label": "Nessuna licenza (il FortiGate resta in valutazione)"}]
    if parametri.get("flexvm_enabled"):
        modi.insert(0, {"key": "flexvm", "label": "FlexVM / FortiFlex (entitlement automatico)"})
    else:
        _avviso(
            "FlexVM e' spento nei Parametri del modulo: non viene proposto. "
            "Si accende in FortiGate Delivery → Parametri."
        )
    modi.insert(
        len(modi) - 1,
        {"key": "forticloud", "label": "FortiCloud (account di licenza esistente)"},
    )
    modo = scegli(modi, lambda m: m["label"], titolo="4 · Licenza")
    scelte["licenza"] = modo["key"]

    if modo["key"] == "flexvm":
        programma = scegli(
            p.programmi_flexvm(),
            lambda x: x["serial"],
            titolo="4a · Programma FortiFlex",
            nota=lambda x: f"valido dal {x.get('start_date', '?')} al {x.get('end_date', '?')}",
        )
        configurazioni = p.configurazioni_flexvm(programma["id"])
        if not configurazioni:
            raise SystemExit(
                f"Nessuna configurazione FortiGate-VM attiva nel programma {programma['serial']}."
            )
        config = scegli(
            configurazioni,
            lambda c: f"{c['name']}  ({c.get('product_type', '?')})",
            titolo="4b · Configurazione FortiFlex",
        )
        # L'API vuole una stringa anche dove FortiFlex restituisce un intero.
        scelte["flexvm_config_id"] = str(config["id"])
        scelte["flexvm_comment"] = chiedi(
            "Nota sull'entitlement (resta su FortiFlex)",
            f"{scelte['cliente']} — {scelte['vm_name']}",
        )
    elif modo["key"] == "forticloud":
        scelte["forticloud_license_account_id"] = chiedi_obbligatorio("Account FortiCloud")
        # ⚠️ La password NON entra in `scelte`: vedi l'intestazione del file.
        # Viene chiesta in `main`, subito prima di mandare la richiesta.


def passo_dimensionamento(scelte: dict) -> None:
    _titolo("5 · Dimensionamento")
    print("  Invio lascia quello del template: una VM clonata nasce con le sue risorse.")
    scelte["cpu_count"] = chiedi_intero("vCPU", None)
    scelte["ram_gb"] = chiedi_intero("Memoria (GB)", None)


# ── rete: due percorsi, perche' sono due infrastrutture ──────────────────────


def passo_rete_vcenter(p: Portale, scelte: dict) -> None:
    modi = [
        {"key": "p2p", "label": "Segmento P2P NSX-T (censito, con la sua subnet in iTop)"},
        {"key": "manual", "label": "Portgroup vCenter scelto a mano"},
    ]
    modo = scegli(modi, lambda m: m["label"], titolo="6 · Rete")
    scelte["network_mode"] = modo["key"]

    if modo["key"] == "p2p":
        # ⚠️ L'elenco e' «cio' che esiste», non «cio' che e' nostro»: fra i
        # segmenti vuoti possono esserci i residui di un wizard interrotto —
        # usarli e' legittimo, ma vedi la nota in testa al file.
        segmenti = p.segmenti_p2p(scelte["env_id"])
        if not segmenti:
            raise SystemExit(
                "Nessun segmento P2P trovato su NSX-T.\n"
                "  L'elenco e' filtrato per prefisso e suffisso (Parametri del modulo): un "
                "segmento che non li rispetta non compare qui anche se esiste."
            )
        seg = scegli(
            segmenti,
            lambda s: s["name"],
            titolo="6a · Segmento P2P",
            nota=lambda s: f"rete {s.get('network') or '?'} · gateway {s.get('gateway') or '?'}",
        )
        rete = seg.get("network") or ""
        gateway_cidr = seg.get("gateway") or ""
        if not rete or not gateway_cidr:
            raise SystemExit(
                f"Il segmento '{seg['name']}' non dichiara rete o gateway su NSX-T: "
                "senza quelli l'indirizzo non e' derivabile."
            )
        gateway = gateway_cidr.split("/")[0]
        indirizzi = p.indirizzi_p2p(scelte["env_id"], rete, gateway_cidr)
        liberi = [
            i for i in indirizzi if (i.get("itop_status") or "") not in ("allocated", "reserved")
        ]
        if not liberi:
            raise SystemExit(f"Nessun indirizzo libero nel segmento {rete} secondo iTop.")
        ind = scegli(
            liberi,
            lambda i: i["ip"],
            titolo="6b · Indirizzo di produzione",
            nota=lambda i: (
                f"{i.get('fqdn') or 'nessun FQDN in iTop'}"
                + (f" · {i['itop_comment']}" if i.get("itop_comment") else "")
            ),
        )
        scelte["nsx_segment_id"] = seg["id"]
        scelte["nsx_segment_network"] = rete
        scelte["assigned_ip"] = ind["ip"]
        scelte["gateway"] = gateway
        scelte["netmask"] = _netmask(ipaddress.IPv4Network(rete, strict=False).prefixlen)
        return

    pgs = p.portgroup(scelte["env_id"])
    # ⚠️ Un uplink non e' una rete su cui si mette una VM: il portale lo dichiara
    # (`uplink`) invece di nasconderlo, e qui si toglie dall'elenco.
    pgs = [g for g in pgs if not g.get("uplink")]
    pg = scegli(
        pgs,
        lambda g: g["name"],
        titolo="6a · Portgroup",
        nota=lambda g: (
            f"{g.get('type', '?')}"
            + (f" · VLAN {g['vlan_id']}" if g.get("vlan_id") is not None else "")
            + (f" · {g['datacenter']}" if g.get("datacenter") else "")
        ),
    )
    subnet = scegli(
        p.subnet_itop(),
        lambda s: f"{s.get('cidr') or s.get('ip')}  {s.get('name', '')}",
        titolo="6b · Subnet iTop in cui censire l'indirizzo",
        nota=lambda s: s.get("block_name", ""),
    )
    liberi_ip = p.indirizzi_liberi_itop(subnet["id"])
    if not liberi_ip:
        raise SystemExit(f"Nessun indirizzo libero nella subnet {subnet.get('cidr')} secondo iTop.")
    ip = scegli(
        [{"ip": x} for x in liberi_ip],
        lambda x: x["ip"],
        titolo="6c · Indirizzo di produzione",
    )["ip"]
    scelte["portgroup_name"] = pg["name"]
    scelte["itop_subnet_id"] = subnet["id"]
    scelte["assigned_ip"] = ip
    scelte["netmask"] = subnet.get("mask") or chiedi_obbligatorio("Netmask")
    scelte["gateway"] = chiedi("Gateway", subnet.get("gateway") or "")


def passo_rete_vcloud(p: Portale, scelte: dict) -> None:
    """Su vCloud la rete si sceglie fra quelle **gia' importate** nel VDC.

    ⓘ Il wizard dell'interfaccia sa anche creare il VDC e importarvi un segmento
    NSX in un colpo solo (`create_vdc`, `nsx_segment_*`). Qui no, ed e' una
    scelta: creare un VDC e' un atto che consuma quota del Provider VDC e non si
    annulla con un `retry`. Si parte da un VDC che esiste."""
    org = scegli(
        p.organizzazioni(scelte["env_id"]),
        lambda o: o["name"],
        titolo="6 · Organizzazione vCloud",
    )
    vdc = scegli(
        p.vdcs(scelte["env_id"], org["id"]),
        lambda v: v["name"],
        titolo="6a · VDC",
        nota=lambda v: v.get("status", ""),
    )
    reti = p.reti_vcloud(scelte["env_id"], vdc["id"])
    if not reti:
        raise SystemExit(
            f"Il VDC '{vdc['name']}' non ha reti OrgVDC.\n"
            "  Una rete si importa dal wizard dell'interfaccia (o dal modulo P2P Segment): "
            "questo esempio non la crea."
        )
    rete = scegli(
        reti,
        lambda n: n["name"],
        titolo="6b · Rete OrgVDC",
        nota=lambda n: (
            f"gateway {n.get('gateway') or '?'}/{n.get('prefixLength') or '?'} · "
            f"{n.get('type', '')}"
        ),
    )
    indirizzi = p.indirizzi_vcloud(scelte["env_id"], rete["id"])
    if indirizzi.get("warning"):
        _avviso(indirizzi["warning"])
    liberi = [i for i in indirizzi.get("available", []) if i.get("status") == "free"]
    if not liberi:
        raise SystemExit(
            f"Nessun indirizzo libero nella rete '{rete['name']}' secondo l'IPAM di vCloud."
        )
    ind = scegli(
        liberi,
        lambda i: i["ip"],
        titolo="6c · Indirizzo di produzione",
        nota=lambda i: (
            f"pool {i.get('pool', '')}"
            + (f" · {i['fqdn']}" if i.get("fqdn") else "")
            + (f" · {i['comment']}" if i.get("comment") else "")
        ),
    )
    prefisso = rete.get("prefixLength")
    scelte["org_name"] = org["name"]
    scelte["org_id"] = org["id"]
    scelte["vdc_name"] = vdc["name"]
    scelte["vdc_id"] = vdc["id"]
    scelte["vcloud_network_id"] = rete["id"]
    scelte["assigned_ip"] = ind["ip"]
    scelte["gateway"] = rete.get("gateway") or chiedi_obbligatorio("Gateway")
    scelte["netmask"] = _netmask(int(prefisso)) if prefisso else chiedi_obbligatorio("Netmask")


def passo_destinazione_vcenter(p: Portale, scelte: dict) -> None:
    """Dove la VM finisce su vCenter, a delivery conclusa (fase 12).

    Durante il lavoro la VM vive in una cartella `Deployment` sotto quella
    dell'ambiente: questi due valori dicono dove viene **spostata** alla fine,
    insieme al nome definitivo."""
    rp = scegli(
        p.resource_pool(scelte["env_id"]),
        lambda r: r["path"],
        titolo="7 · Resource pool",
    )
    cartella = scegli(
        p.cartelle(scelte["env_id"]),
        lambda f: f["path"],
        titolo="7a · Cartella VM",
    )
    scelte["resource_pool"] = rp["path"]
    scelte["dest_folder"] = cartella["path"]


def passo_script_finale(scelte: dict) -> None:
    """Uno script Ansible in piu', eseguito alla fase 10.

    ⚠️ Non e' bloccante: se fallisce, il log lo dichiara e la delivery prosegue.
    E' il posto giusto per una configurazione specifica del cliente, non per
    qualcosa senza cui l'apparato non si puo' consegnare."""
    _titolo("8 · Script finale (facoltativo)")
    print("  Invio per saltarlo. Un percorso di file ne carica il contenuto.")
    percorso = chiedi("File dello script Ansible", "")
    if not percorso:
        scelte["custom_final_script"] = ""
        return
    with open(percorso) as fh:
        scelte["custom_final_script"] = fh.read()
    print(f"  Caricato: {len(scelte['custom_final_script'])} caratteri.")


# ─────────────────────────── Il corpo della richiesta ───────────────────────


def corpo_delivery(scelte: dict) -> tuple[str, dict]:
    """Da scelte a `(percorso, corpo)`, senza chiamare niente.

    Funzione **pura**: si prova, si confronta e si riesegue senza un portale
    davanti. Il percorso (`vcenter` o `vcloud`) esce da qui insieme al corpo
    perche' i due non si possono scambiare — vedi l'intestazione del file."""
    comune = {
        "vm_name": scelte["vm_name"],
        "template_id": scelte["template_id"],
        "assigned_ip": scelte["assigned_ip"],
        "netmask": scelte["netmask"],
        "gateway": scelte["gateway"],
        "cliente": scelte.get("cliente", ""),
        "rif_contratto": scelte.get("rif_contratto", ""),
        "custom_final_script": scelte.get("custom_final_script", ""),
    }
    for campo in ("cpu_count", "ram_gb"):
        if scelte.get(campo) is not None:
            comune[campo] = scelte[campo]

    licenza: dict = {}
    if scelte.get("licenza") == "flexvm":
        licenza["flexvm_config_id"] = scelte["flexvm_config_id"]
        licenza["flexvm_comment"] = scelte.get("flexvm_comment", "")
    elif scelte.get("licenza") == "forticloud":
        licenza["forticloud_license_account_id"] = scelte["forticloud_license_account_id"]
        # La password la aggiunge `main` subito prima di mandare: non sta mai
        # nelle scelte, quindi non sta mai in un file salvato.

    if scelte["env_type"] == "vcloud":
        corpo = {
            **comune,
            **licenza,
            "org_name": scelte["org_name"],
            "org_id": scelte.get("org_id", ""),
            "vdc_name": scelte["vdc_name"],
            "vdc_id": scelte.get("vdc_id", ""),
            "create_vdc": False,
            "vcloud_network_id": scelte["vcloud_network_id"],
        }
        return "vcloud", corpo

    corpo = {
        **comune,
        **licenza,
        "network_mode": scelte["network_mode"],
        "resource_pool": scelte["resource_pool"],
        "dest_folder": scelte["dest_folder"],
    }
    if scelte["network_mode"] == "p2p":
        corpo["nsx_segment_id"] = scelte["nsx_segment_id"]
        corpo["nsx_segment_network"] = scelte["nsx_segment_network"]
    else:
        corpo["portgroup_name"] = scelte["portgroup_name"]
        corpo["itop_subnet_id"] = scelte.get("itop_subnet_id", "")
    return "vcenter", corpo


def riepilogo(scelte: dict) -> None:
    _titolo("Riepilogo")
    righe = [
        ("Ambiente", f"{scelte['env_label']}  ({scelte['env_type']})"),
        ("Template", scelte.get("template_label", "")),
        ("VM", scelte["vm_name"]),
        ("Cliente", f"{scelte.get('cliente', '')} — {scelte.get('rif_contratto', '')}"),
        ("Licenza", scelte.get("licenza", "nessuna")),
        (
            "Dimensionamento",
            " · ".join(
                x
                for x in (
                    f"{scelte['cpu_count']} vCPU" if scelte.get("cpu_count") else "",
                    f"{scelte['ram_gb']} GB" if scelte.get("ram_gb") else "",
                )
                if x
            )
            or "quello del template",
        ),
        ("Indirizzo", f"{scelte['assigned_ip']} / {scelte['netmask']} via {scelte['gateway']}"),
    ]
    if scelte["env_type"] == "vcloud":
        righe.append(("Destinazione", f"{scelte['org_name']} / {scelte['vdc_name']}"))
        righe.append(("Rete", scelte["vcloud_network_id"]))
    else:
        righe.append(("Rete", scelte.get("nsx_segment_id") or scelte.get("portgroup_name", "")))
        righe.append(("Destinazione", f"{scelte['dest_folder']}  ·  {scelte['resource_pool']}"))
    for etichetta, valore in righe:
        print(f"  {etichetta:<16} {valore}")


# ─────────────────────────── Seguire il job ─────────────────────────────────


def _mostra_nuove_righe(log: str, gia_visto: int) -> int:
    """Stampa le righe comparse dal giro precedente, e restituisce quanto si e' letto.

    Si segue il **log**, non un elenco di fasi scritto qui dentro: quelle righe
    le scrive il portale (`→ [n/14] …`) e un secondo elenco qui divergerebbe al
    primo passo aggiunto, senza che nulla lo segnali."""
    nuovo = log[gia_visto:]
    for riga in nuovo.splitlines():
        if riga.strip():
            print(f"  {riga.rstrip()}")
    return len(log)


def _risolvi_attesa_licenza(p: Portale, job_id: int) -> None:
    dettaglio = p.decisione_licenza(job_id)
    ent = dettaglio.get("existing_entitlement", {})
    _titolo("Il job attende una decisione sulla licenza")
    print(
        f"  Su FortiFlex esiste gia' un entitlement **attivo** per questa delivery:\n"
        f"    serial  {ent.get('serial', '?')}\n"
        f"    stato   {ent.get('status', '?')}\n"
        f"    config  {ent.get('config_name', '?')}\n"
        "  Terminarlo libera il posto nel contratto; lasciarlo attivo ne consuma un secondo."
    )
    if not _interattivo():
        raise SystemExit(
            f"Nessuna console: la decisione va presa a mano.\n"
            f"  POST /api/v1/fortigate-vm/jobs/{job_id}/license-decision "
            '{"terminate_existing": true|false}'
        )
    termina = chiedi_si_no("Terminare l'entitlement esistente?", predefinito=False)
    p.decidi_licenza(job_id, termina)
    print(f"  Decisione registrata: {'termina' if termina else 'mantieni'}. Il job riprende.")


def _risolvi_attesa_indirizzo(p: Portale, job_id: int) -> None:
    dettaglio = p.indirizzi_sorgente(job_id)
    _titolo("Il job attende l'indirizzo transitorio di provisioning")
    print(
        f"  Subnet {dettaglio.get('subnet_cidr', '?')} · "
        f"indirizzo di setup {dettaglio.get('setup_ip', '?')}"
    )
    liberi = dettaglio.get("free_ips") or []
    if not liberi:
        raise SystemExit("Nessun indirizzo libero nella subnet sorgente secondo iTop.")
    if not _interattivo():
        raise SystemExit(
            f"Nessuna console: l'indirizzo va scelto a mano.\n"
            f"  POST /api/v1/fortigate-vm/jobs/{job_id}/src-ip "
            '{"src_secondary_ip": "..."}\n'
            f"  Liberi: {', '.join(liberi[:10])}{' …' if len(liberi) > 10 else ''}"
        )
    ip = scegli(
        [{"ip": x} for x in liberi],
        lambda x: x["ip"],
        titolo="Indirizzo transitorio",
    )["ip"]
    # ⚠️ Il portale lo annota in iTop **subito**, e rifiuta con 409 se nel
    # frattempo qualcun altro lo ha preso: la verifica non e' decorativa, due
    # delivery in parallelo sulla stessa subnet se lo contendono davvero.
    p.scegli_indirizzo_sorgente(job_id, ip)
    print(f"  Indirizzo {ip} annotato in iTop. Il job riprende.")


def segui_job(p: Portale, job_id: int, *, minuti: int = 60, intervallo: int = 15) -> dict:
    """Segue il job finche' non dice `success` o `failed`.

    ⚠️ Il 202 e' una promessa, non un risultato: il lavoro prosegue in un job, e
    fermarsi la' vuol dire dichiarare riuscito un apparato che sta ancora
    clonando — o che e' gia' fallito. Una delivery sono quattordici fasi e due
    riavvii del FortiGate: decine di minuti sono il caso normale."""
    scadenza = time.monotonic() + minuti * 60
    letto = 0
    ultimo_stato = ""
    while time.monotonic() < scadenza:
        job = p.job(job_id)
        stato = job.get("status", "")
        letto = _mostra_nuove_righe(job.get("log_output") or "", letto)
        if stato != ultimo_stato:
            print(
                f"  \033[2m— {stato} "
                f"({job.get('last_completed_step', 0)}/{job.get('total_steps', 14)})\033[0m"
            )
            ultimo_stato = stato
        if stato == "waiting_license_decision":
            _risolvi_attesa_licenza(p, job_id)
        elif stato == "waiting_src_ip":
            _risolvi_attesa_indirizzo(p, job_id)
        elif stato in ("success", "failed"):
            return job
        time.sleep(intervallo)
    raise SystemExit(
        f"Il job {job_id} non si e' concluso entro {minuti} minuti. Non e' detto "
        f"che sia fallito: si rilegge con GET /api/v1/fortigate-vm/jobs/{job_id}."
    )


def esito(job: dict) -> int:
    if job["status"] == "success":
        print(f"\n\033[32mDelivery conclusa: {job.get('vm_name')}\033[0m")
        righe = [
            ("Indirizzo", job.get("assigned_ip") or "—"),
            ("Serial licenza", job.get("license_serial") or "nessuna licenza applicata"),
            ("Utenza UID", job.get("uid_admin_name") or "—"),
        ]
        for etichetta, valore in righe:
            print(f"  {etichetta:<16} {valore}")
        print(
            "  Le credenziali stanno in DVLS (connessioni SSH e Web GUI create dalla fase 13):\n"
            "  il portale non le restituisce — espone i soli nomi utente."
        )
        return 0

    print(f"\n\033[31mDelivery fallita alla fase {job.get('last_completed_step', 0) + 1}\033[0m")
    if job.get("manual_review_required"):
        _avviso(
            "Il rollback automatico allo snapshot e' gia' stato tentato e non e' bastato: "
            "la VM e' spenta e va guardata a mano prima di riprovare."
        )
    elif job.get("rollback_attempted"):
        _avviso("C'e' stato un rollback automatico allo snapshot durante questa delivery.")
    ultimo = job.get("last_completed_step", 0)
    print(
        f"\n  La ripresa non riparte da capo: riprende dalla fase scelta, al massimo "
        f"{ultimo + 1} (ultima completata: {ultimo})\n"
        f"    POST /api/v1/fortigate-vm/jobs/{job['id']}/retry "
        f'{{"resume_from_step": {ultimo + 1}}}'
    )
    if job.get("licensing_mode") == "forticloud":
        _avviso(
            "Con FortiCloud la password dell'account va rimandata alla ripresa: il portale "
            "la azzera appena la licenza e' stata applicata."
        )
    return 1


# ─────────────────────────── Il programma ───────────────────────────────────


def raccogli_scelte(p: Portale) -> dict:
    scelte: dict = {}
    passo_ambiente(p, scelte)
    passo_template(p, scelte)
    passo_identita(scelte)
    passo_licenza(p, scelte)
    passo_dimensionamento(scelte)
    if scelte["env_type"] == "vcloud":
        passo_rete_vcloud(p, scelte)
    else:
        passo_rete_vcenter(p, scelte)
        passo_destinazione_vcenter(p, scelte)
    passo_script_finale(scelte)
    return scelte


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Wizard di creazione di un FortiGate VM tramite l'API del portale.",
        epilog="La API key si passa in PORTAL_API_KEY, mai come argomento.",
    )
    parser.add_argument(
        "--portale", default=os.environ.get("PORTAL_URL", "https://portal.admin.dinova.cloud")
    )
    parser.add_argument(
        "--conferma",
        action="store_true",
        help="crea davvero il FortiGate. Senza, il wizard si ferma stampando il corpo.",
    )
    parser.add_argument("--da-file", metavar="FILE", help="riusa scelte gia' prese, senza wizard")
    parser.add_argument(
        "--salva-scelte", metavar="FILE", help="scrive le scelte per poterle rieseguire"
    )
    parser.add_argument(
        "--senza-verifica-tls",
        action="store_true",
        help="solo per gli ambienti con certificato autofirmato (dev, staging)",
    )
    parser.add_argument("--attesa-minuti", type=int, default=60)
    args = parser.parse_args(argv)

    try:
        p = Portale(
            args.portale,
            os.environ.get("PORTAL_API_KEY", ""),
            verifica_tls=not args.senza_verifica_tls,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    # ⚠️ La credenziale si prova **prima** di qualunque altra cosa. Con
    # `--da-file` non si chiama nulla finche' non si conferma: un 403 emergerebbe
    # al `POST .../deliver`, cioe' nel punto peggiore — e senza `--conferma` non
    # emergerebbe affatto, lasciando credere che la chiave vada bene.
    try:
        p.ambienti()
    except ErrorePortale as exc:
        print(f"Il portale non risponde come atteso: {exc}", file=sys.stderr)
        if exc.stato in (401, 403):
            print(
                "  Serve una chiave valida con `fortigate_vm: OPERATOR` in PORTAL_API_KEY.",
                file=sys.stderr,
            )
        return 2

    try:
        if args.da_file:
            with open(args.da_file) as fh:
                scelte = json.load(fh)
        else:
            scelte = raccogli_scelte(p)
    except ErrorePortale as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 2
    except (KeyboardInterrupt, EOFError):
        print("\nInterrotto: non e' stato creato niente.")
        return 130

    riepilogo(scelte)
    percorso, corpo = corpo_delivery(scelte)

    if args.salva_scelte:
        with open(args.salva_scelte, "w") as fh:
            json.dump(scelte, fh, indent=2, ensure_ascii=False)
        print(f"\n  Scelte salvate in {args.salva_scelte}")

    if not args.conferma:
        rotta = f"/api/v1/fortigate-vm/{percorso}/{scelte['env_id']}/deliver"
        print(f"\n  Corpo che verrebbe mandato a POST {rotta}:")
        print(json.dumps(corpo, indent=2, ensure_ascii=False))
        if scelte.get("licenza") == "forticloud":
            print(
                "  (piu' `forticloud_license_account_password`, che si chiede al momento "
                "e non viene mai salvata)"
            )
        print("\n  Nessun FortiGate creato. Per crearlo davvero: --conferma")
        return 0

    if scelte.get("licenza") == "forticloud":
        if not _interattivo():
            print(
                "Con la licenza FortiCloud serve la password dell'account, e non c'e' una "
                "console per chiederla: eseguire con un terminale.",
                file=sys.stderr,
            )
            return 2
        corpo["forticloud_license_account_password"] = chiedi_segreto(
            f"Password dell'account FortiCloud {scelte['forticloud_license_account_id']}"
        )

    try:
        risposta = p.delivery(percorso, scelte["env_id"], corpo)
    except ErrorePortale as exc:
        print(f"\nDelivery rifiutata: {exc}", file=sys.stderr)
        if exc.codice == "TEMPLATE_LOCKED":
            print(
                "  Un'altra delivery sta usando quel template. ⚠️ Questo rifiuto lascia "
                "comunque un job fallito in cronologia.",
                file=sys.stderr,
            )
        return 2
    print(f"\n  Accodata: job {risposta['job_id']} ({risposta.get('status', '?')})")
    job = segui_job(p, risposta["job_id"], minuti=args.attesa_minuti)
    return esito(job)


if __name__ == "__main__":
    raise SystemExit(main())
