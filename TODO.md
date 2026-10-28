casi per la delete
test su retry job /api/v1/fortigate-vm/jobs/{job_id}/retry

Il provider OpenStack non ritenta né i 503 né le connessioni chiuse, e non ha un timeout per richiesta. Un solo 503 o una rete instabile fanno fallire il run; un'API bloccata tiene fermo il Job fino al suo timeout.

