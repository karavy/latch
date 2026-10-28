# Local development stack: operator + in-cluster mock API + one safe smoke-test CR.
# This intentionally supports only the verified local Kind context.
allow_k8s_contexts('kind-er-operator')

# Build the operator image with the sibling local Go modules required by go.mod.
custom_build(
    'controller:tilt',
    'docker build --build-context=api-engine=../api-engine --build-context=terraform-engine=../terraform-engine -f Dockerfile.tilt -t $EXPECTED_REF .',
    deps=[
        'Dockerfile.tilt', 'go.mod', 'go.sum', 'api', 'cmd', 'internal',
        '../api-engine', '../terraform-engine',
    ],
)

# Build the standalone mock HTTP API image from local source.
docker_build(
    'latch-mock-api:tilt',
    '.',
    dockerfile='Dockerfile.mock-api',
)

# Terraform steps run Jobs with a separate runner image (OpenTofu + terraform-engine). Tilt
# only builds images that appear in the Kubernetes YAML, and this one is named by an
# environment variable of the operator, so build it here and load it into the Kind node.
local_resource(
    'terraform-runner-image',
    cmd=('docker build --build-context=api-engine=../api-engine ' +
         '--build-context=terraform-engine=../terraform-engine ' +
         '-f Dockerfile.terraform-runner -t latch-terraform-runner:tilt . ' +
         '&& kind load docker-image latch-terraform-runner:tilt --name er-operator'),
    deps=[
        'Dockerfile.terraform-runner', 'go.mod', 'go.sum', 'cmd/terraform-runner',
        '../api-engine', '../terraform-engine',
    ],
)

# The Kind node cannot pull the MinIO image (the registry answers it 401), so take it from the
# local Docker (pulling it there if needed) and import it into the node. "kind load" would fail
# on this multi-platform image because it imports every platform, so import the image by hand.
local_resource(
    'minio-image',
    cmd=('docker image inspect quay.io/minio/minio:latest >/dev/null 2>&1 || ' +
         'docker pull quay.io/minio/minio:latest; ' +
         'docker save quay.io/minio/minio:latest | ' +
         'docker exec --privileged -i er-operator-control-plane ' +
         'ctr -n k8s.io images import --digests --snapshotter=overlayfs -'),
)

# Kustomize deploys CRDs, RBAC, operator, ClusterIP mock API, and the templates of the smoke
# tests. Tilt streams logs and reports pod/CR status. The sample Workflows and WorkflowPipelines
# are left out, so that starting the stack runs nothing: add them with `tilt up -- --workflows`.
config.define_bool('workflows')
settings = config.parse()
objects = decode_yaml_stream(kustomize('config/tilt'))
if not settings.get('workflows', False):
    objects = [o for o in objects if o['kind'] not in ['Workflow', 'WorkflowPipeline']]
k8s_yaml(encode_yaml_stream(objects))

# The test certificate of the mock API: its HTTPS port serves the mock admin portal, which the
# operator calls with an API key, and the operator only sends credentials over HTTPS to an origin
# whose CA it trusts. Created once (Secret and ConfigMap), before the operator and the mock start.
local_resource('mock-api-tls', cmd='hack/mock-api-tls.sh', resource_deps=['uncategorized'])

# Link each workload to its locally built image in the Tilt UI.
k8s_resource('latch-controller-manager', resource_deps=['mock-api-tls'])
k8s_resource('mock-api', port_forwards=['8088:8080', '8443:8443'], resource_deps=['mock-api-tls'])
# Terraform state storage (MinIO): S3 API on 9000 and the web console on 9001.
k8s_resource('minio', port_forwards=['9000:9000', '9001:9001'], resource_deps=['minio-image'])
