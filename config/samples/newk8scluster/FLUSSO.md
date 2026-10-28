### VDC - Terraform
- [ ] creazione resourcepool 
- [ ] creazione folder
- [ ] creazione dell’organizzazione

### vSphere - Admin portal
- [ ] creazione firewall
- [ ] creazione rete per collegamento VDC/NSX
- [ ] import della rete creata con il tf su vcenter/NSX nel VDC IaaS del fw
- [ ] collegamento della rete nuova del VDC al fortigate (172.19.x.x) sulla porta 2 del fortigate

### iTop - Terraform
- [ ] recuperare la rete k8s cliente da iTOP e aggiornarlo di conseguenza (chiedere a Grillo)

### Fortigate - Admin Portal
- [ ] configurazione base del fortigate
- [ ] configurazione della porta fgt (adesso lo fa Cenedese)
- [ ] Creazione della policy di allow dal cluster k8s verso internet
- [ ] NAT della 6443 verso APISERVER
- [ ] NAT 80/443 del cliente
- [ ] allowlist ip cliente per 6443

### Google - Terraform
- [x] creazione zona google per il cliente
- [ ] creazione del service account per la nuova zona creata
- [X] chiedere a Fausto un serviceaccount con i permessi per poterlo fare