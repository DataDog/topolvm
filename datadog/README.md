# Datadog downstream module

Everything below this directory is a Datadog-specific downstream integration.
It is not part of upstream-supported TopoLVM functionality and is not intended
for upstream submission.

This directory is an independent Go module,
`github.com/DataDog/topolvm/datadog`, with its own `go.mod`, `Makefile`, and
`.golangci.yml`. It does not import the upstream TopoLVM module, and the
upstream `./...` build, vet, lint, and test targets do not include it. Only
`Dockerfile.dd` and `.gitlab-ci.yml` at the repository root reference it.

```bash
make -C datadog lint   # gofmt, golangci-lint, go vet
make -C datadog test   # unit + envtest tests (downloads envtest binaries)
make -C datadog build VERSION=<version> GOARCH=<arch>
```

Binaries are written to `datadog/build/` and copied into the image by
`Dockerfile.dd`. Layout:

```text
cmd/topolvm-capacity-template-controller/   # command entry point and flags
internal/capacitytemplate/                  # reconcilers and capacity handlers
```

The capacity-template controller is maintained on the downstream `datadog`
branch. Its capacity matrix mirrors `GetRemoteLVMCapacity` from the
k8s-nodegroups remote TopoLVM implementation, including `diskCount`,
`raidLevel`, whole-GiB cloud-disk rounding, and 8 MiB of LVM overhead per
resulting PV. Re-check that matrix whenever NodeGroup storage preparation
changes.

## Capacity-template controller configuration

The controller reads one configuration file, for example:

```yaml
capacityTemplates:
  - storageClassName: ephemeral-remote-data
    deviceClassName: remote-ssd
    spareGB: 0
    handler: remote-lvm
```

Each entry is reconciled independently for every NodeGroup and produces at most
one `CSIStorageCapacity`. An entry is published only when its StorageClass
exists, uses the `topolvm.io` provisioner with `WaitForFirstConsumer`, and has
`topolvm.io/device-class` equal to `deviceClassName`, and when its handler
reports capacity for the NodeGroup. Otherwise that entry's object is deleted
without affecting other entries. Removing an entry from the file deletes its
objects on the next reconcile.

Constraints:

- `storageClassName` must be unique across entries.
- `handler` defaults to `remote-lvm`. `remote-lvm` is the only implemented
  handler; any other value is rejected at startup. A future `local-lvm`
  handler would be added to the handler registry in
  `internal/capacitytemplate/handler.go`.
- Unknown keys in the file are rejected.

## Startup taint (avoiding duplicate scale-ups)

Template capacities only match Cluster Autoscaler (CA) template Nodes
(`cluster-autoscaler.kubernetes.io/template-node=true`). After a scale-up, the
new real Node becomes Ready before external-provisioner publishes its per-node
`CSIStorageCapacity`. In that window CA's simulation contains the real Node,
which has no matching capacity, so still-pending Pods look unschedulable and
trigger another scale-up.

To close the window, NodeGroups register Nodes with a CA startup taint:

```yaml
spec:
  kubelet:
    taints:
      startup-taint.cluster-autoscaler.kubernetes.io/topolvm-capacity-not-ready: "true:NoSchedule"
```

CA treats any Ready Node with a `startup-taint.cluster-autoscaler.kubernetes.io/`
taint as not yet started (for up to `--max-node-startup-time`, default 15m). It
removes such a Node from its simulation and injects a sanitized template copy
instead. That copy carries the template-node label and matches the template
capacity. CA also strips the taint from template Nodes, so no CA flags are
needed.

The same binary runs a startup-taint controller (`--startup-taint`, which
defaults to the key above; an empty value disables it). It removes the taint
from a Node once every StorageClass in `capacityTemplates` has a real (not
controller-managed) per-node `CSIStorageCapacity` with positive capacity for
that Node. It keeps all other taints and emits a `StartupTaintRemoved` Event.

It requires the configured StorageClasses explicitly, not any positive
capacity, for two reasons:

- TopoLVM publishes zero capacity until topolvm-node annotates the Node.
- Embedded lvmd starts without every volume group, so local capacity can be
  positive while the remote volume group is still being prepared.

The controller does not watch Nodes or read NodeGroups:

- It reacts only to `CSIStorageCapacity` events that can release a Node: a
  configured-class capacity that is created positive, or that changes from zero
  to positive. Ordinary capacity changes from volume provisioning are ignored.
- On such an event it reads that one Node by the name in
  `topology.topolvm.io/node`, using `resourceVersion=0` so the API server's
  watch cache serves the read instead of etcd.
- On start or leader change, every positive capacity is replayed once. That is
  one Node read per Node per configured StorageClass, deduplicated per Node.
- The cache fails reads of unwatched types (`ReaderFailOnMissingInformer`), so
  nothing can implicitly start a cluster-wide Node watch. All cached objects
  are stripped of `managedFields`, and NodeGroup reads are served from the
  informer.

Operational notes:

- Only taint NodeGroups that provide every configured StorageClass. The
  controller does not know which NodeGroup a Node belongs to. A tainted Node
  whose configured capacity never becomes positive stays tainted. CA then
  classifies it as unready after `--max-node-startup-time`, and normal
  unready-node handling applies.
- Roll out the controller before adding the taint to any NodeGroup. If the
  controller is not running, tainted Nodes never accept workloads.
- The taint blocks every Pod that does not tolerate it. DaemonSets that must
  start for capacity to appear, such as topolvm-node and CNI, must tolerate it.
  topolvm-node already tolerates all taints (`operator: Exists`).
- The tainted window lasts until external-provisioner's next capacity poll.
  Lower `csi-provisioner --capacity-poll-interval` (default 1m) to shorten
  Pod startup. The startup taint, not the poll interval, is what prevents
  duplicate scale-ups.
- Required RBAC: `nodes` get/patch, `csistoragecapacities` get/list/watch, and
  `events` create/patch.

## Capacity-template controller rollback

Scale or remove the controller Deployment first, then remove only its dynamic
objects with the exact ownership label:

```sh
kubectl delete csistoragecapacities.storage.k8s.io --all-namespaces \
  -l storage.datadoghq.com/managed-by=datadog-topolvm-capacity-template-controller
```

Before scaling the controller down, remove the startup taint from NodeGroup
`spec.kubelet.taints`. Then clear it from any Node that still carries it:

```sh
key=startup-taint.cluster-autoscaler.kubernetes.io/topolvm-capacity-not-ready
kubectl get nodes -o json \
  | jq -r --arg key "$key" '.items[] | select(any(.spec.taints[]?; .key == $key)) | .metadata.name' \
  | xargs -r -I{} kubectl taint node {} "$key-"
```
