# Test di latch

Piani, procedure ed esiti dei test eseguiti a mano sul cluster di sviluppo (Kind
`kind-er-operator`), oltre ai test automatici (`make test`).

| Documento | Contenuto | Stato |
|---|---|---|
| [TEST-PLAN.md](TEST-PLAN.md) | Test degli step Terraform del workflow Virtuozzo (`virtuozzo-vm`) contro Virtuozzo reale: percorso normale, modifiche, errori di comunicazione, interruzioni, lock, perdita dello state, drift, concorrenza, approvazione manuale, timeout, destroy. Con esiti e difetti trovati | Eseguito il 26/10/2026, completato il 27/10/2026 (pulizia fatta) |
| [MOCK-PORTAL.md](MOCK-PORTAL.md) | Mock dell'admin portal per il workflow FortiGate (p2p): letture iTop e portale, segmento, delivery e job con le 14 fasi, ripresa; stesso motore e stesse regole di guasto del mock OpenStack, e i 26 scenari che lo usano | Implementato; P1–P26 eseguiti il 27/10/2026, con esiti e 2 difetti di latch corretti |
| [MOCK-OPENSTACK.md](MOCK-OPENSTACK.md) | Mock OpenStack (Keystone, Nova, Neutron, Glance) per far girare lo stesso codice Terraform senza Virtuozzo, con guasti iniettabili, e i 21 scenari che lo usano | Implementato; M1–M21 eseguiti il 27/10/2026, con esiti |

I manifest dei test stanno con i sample: `config/samples/terraform-workflow/` (workflow reale) e
`config/samples/terraform-workflow/mock/`, `config/samples/p2p-workflow/mock/` (mock, generati
con `go run ./hack/gen-mock-samples`). Il codice dei mock è in `cmd/mock-api`.
