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

# Kustomize deploys CRDs, RBAC, operator, ClusterIP mock API, and a single
# POST /echo smoke test. Tilt streams logs and reports pod/CR status.
k8s_yaml(kustomize('config/tilt'))

# Link each workload to its locally built image in the Tilt UI.
k8s_resource('latch-controller-manager')
k8s_resource('mock-api', port_forwards=['8088:8080'])
# Terraform state storage (MinIO): S3 API on 9000 and the web console on 9001.
k8s_resource('minio', port_forwards=['9000:9000', '9001:9001'], resource_deps=['minio-image'])
