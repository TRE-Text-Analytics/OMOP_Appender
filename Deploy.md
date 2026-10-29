# Deploying OMOP-Appender

This guide covers running OMOP-Appender on your own infrastructure:

- **[Docker](#option-1-docker)**: quickest, for a single machine or a trial run.
- **[Kubernetes with Helm](#option-2-kubernetes-with-helm-recommended)**: recommended for shared or production environments.
- **[Kubernetes with plain manifests](#option-3-kubernetes-with-plain-manifests)**: if your cluster doesn't use Helm.

All three use the same pre-built container image:

```
ghcr.io/tre-text-analytics/omop_appender
```

Available versions are listed on the [package page](https://github.com/TRE-Text-Analytics/OMOP_Appender/pkgs/container/omop_appender). Each release is tagged with its full version (e.g. `1.0.3`), its minor version (e.g. `1.0`, which follows the latest patch) and `latest`.

---

## Before you start

- **Two PostgreSQL OMOP CDM databases**: a source to copy from and a target to append into (CDM v5.4, or v5.3 source into v5.4 target).
- **Network access from where the app runs to both databases.** Database connection details are entered in the app's web interface, not in the deployment, but the app must be able to reach the database host and port. See [Connecting to your databases](#connecting-to-your-databases).
- **Enough memory.** See [Sizing](#sizing). Large targets need several GB.
- **Take a backup of the target first.** Each merge runs in a single transaction and is rolled back if it fails, but a successful merge writes to your target. Use the app's dry run to preview a merge before running it for real.

> [!WARNING]
> **The app has no login.** Anyone who can open it in a browser can connect it to any database it can reach and write to it. Keep it on a private network, reach it through `kubectl port-forward` or a VPN, or put it behind your organisation's authentication (for example an ingress with SSO). Never expose it directly to the internet.

---

## Option 1: Docker

Requires [Docker](https://docs.docker.com/get-docker/) with the Compose plugin.

```bash
git clone https://github.com/TRE-Text-Analytics/OMOP_Appender.git
cd OMOP_Appender
docker compose up -d
```

Open http://localhost:8000.

If your databases run on the same machine as Docker, use the hostname `host.docker.internal` in the app's connection settings instead of `localhost`. (Inside a container, `localhost` means the container itself.)

To update to the newest image: `docker compose pull && docker compose up -d`. To stop: `docker compose down`.

---

## Option 2: Kubernetes with Helm (recommended)

Requires access to a Kubernetes cluster (`kubectl get nodes` should work) and [Helm 3](https://helm.sh/docs/intro/install/).

### 1. Get the chart

The chart lives in this repository:

```bash
git clone https://github.com/TRE-Text-Analytics/OMOP_Appender.git
cd OMOP_Appender
```

### 2. Create your settings file

Don't edit the chart's own `values.yaml`. Create a small file with only the settings you want to change, for example `my-values.yaml`:

```yaml
image:
  tag: "1.0.3"            # pin a version; avoid "latest" in production
  pullPolicy: IfNotPresent

resources:
  requests:
    memory: 2Gi           # reserved for the app on the node
  limits:
    memory: 10Gi          # the most it may use; see "Sizing"
```

All available settings, with comments, are in [`helm/omop-appender/values.yaml`](helm/omop-appender/values.yaml). The ones most people need are listed under [Common settings](#common-settings).

### 3. Install

```bash
helm upgrade --install omop-appender ./helm/omop-appender \
  --namespace omop --create-namespace \
  -f my-values.yaml
```

Wait until the pod shows `1/1 Running`:

```bash
kubectl -n omop get pods -w
```

### 4. Open the app

Without an ingress, forward a local port to it:

```bash
kubectl -n omop port-forward svc/omop-appender 8000:8000
```

Then open http://localhost:8000. Leave that command running while you use the app.

To give it a permanent address instead, enable the ingress (see [Common settings](#common-settings)) and ask your cluster administrator which ingress class and hostname to use.

### Upgrading and removing

Change `image.tag` in your settings file and run the same `helm upgrade --install` command again. Kubernetes starts the new version and then removes the old one.

```bash
helm rollback omop-appender -n omop      # go back to the previous release
helm uninstall omop-appender -n omop     # remove the app completely
```

### Common settings

| Setting | Default | What it does |
|---|---|---|
| `image.tag` | see `values.yaml` | App version to run. Pin a version number in production. |
| `image.pullPolicy` | see `values.yaml` | Use `Always` with `latest`, otherwise a node may keep running an old copy; `IfNotPresent` with pinned versions. |
| `resources.requests.memory` / `limits.memory` | see `values.yaml` | Memory reserved and maximum allowed. See [Sizing](#sizing). |
| `ingress.enabled`, `ingress.className`, `ingress.hosts`, `ingress.tls` | disabled | Publish the app on a hostname through your ingress controller. |
| `ingress.annotations` | none | Ingress controller options, e.g. authentication or longer timeouts. |
| `hostAliases` | none | Make a hostname resolve to a fixed IP inside the pod. See [Connecting to your databases](#connecting-to-your-databases). |
| `imagePullSecrets` | none | Credentials, if your cluster pulls the image through a private registry or mirror. |
| `extraEnv`, `envFrom` | none | Extra environment variables, e.g. from a Kubernetes Secret. |
| `service.type` | `ClusterIP` | `NodePort` or `LoadBalancer` to expose the app without an ingress. |
| `nodeSelector`, `tolerations`, `affinity` | none | Control which nodes the app runs on. |

> [!TIP]
> Long merges stream their progress over one HTTP request that can stay open for many minutes. If you use an ingress and merges cut off part-way, raise the controller's read timeout. For ingress-nginx:
> ```yaml
> ingress:
>   annotations:
>     nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
>     nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
>     nginx.ingress.kubernetes.io/proxy-buffering: "off"
> ```

---

## Option 3: Kubernetes with plain manifests

The `k8s/` folder contains the same Deployment, Service and optional Ingress as plain YAML, for clusters that don't use Helm.

1. Edit `k8s/deployment.yaml`:
   - **image**: set the version you want, e.g. `ghcr.io/tre-text-analytics/omop_appender:1.0.3`.
   - **hostAliases**: delete this block unless you need it (see [Connecting to your databases](#connecting-to-your-databases)).
   - **resources**: adjust memory (see [Sizing](#sizing)).
2. Edit `k8s/ingress.yaml` with your hostname and ingress class, or remove `ingress.yaml` from `k8s/kustomization.yaml` if you don't need one.
3. Apply:

   ```bash
   kubectl create namespace omop
   kubectl apply -k k8s/
   kubectl -n omop port-forward svc/omop-appender 8000:8000
   ```

To remove it: `kubectl delete -k k8s/`.

---

## Connecting to your databases

You enter the source and target connection details (host, port, database, schema, username, password) in the web interface. What you put in **host** depends on where each database lives:

| Database location | Host to enter |
|---|---|
| Elsewhere on your network, or a managed cloud database | Its normal hostname or IP. The cluster must be allowed to reach it; ask your network or platform team if the connection times out. |
| In the same Kubernetes cluster | Its service name: `<service>.<namespace>.svc.cluster.local` |
| On the same machine as Docker | `host.docker.internal` |

**Keeping `host.docker.internal` on Kubernetes.** If you'd rather keep the same hostname you used with Docker, map it to the database machine's IP with `hostAliases`:

```yaml
hostAliases:
  - ip: "10.20.30.40"          # your database server's IP
    hostnames:
      - host.docker.internal
```

Use **Test connection** in the app before scanning. A *timeout* usually means the network path is blocked. *Connection refused* means the host was reached but nothing was accepting connections on that port.

---

## Sizing

**Memory is the setting that matters.** To spot duplicates, the app loads the target's existing rows for the patients being merged, one table at a time, so the memory it needs grows with the size of the **largest table in your target**, not with how much you're appending.

| Rows in the target's largest table (usually `measurement`) | Suggested memory limit |
|---|---|
| up to ~5 million | 2Gi |
| ~5–20 million | 4Gi |
| ~20–50 million | 8–10Gi |
| more | Measure with a dry run (below) |

As a reference, a target with 7 million `measurement` and 3 million `observation` rows peaks at about 1.1 GB.

- **Run a dry run first** on real-sized data and watch usage with `kubectl -n omop top pod`. Set the limit to about 1.5× the peak you see.
- **Running out of memory stops the run.** Kubernetes restarts the app and the run's transaction is rolled back, so nothing is half-written, but you'll need to start that run again.
- **One instance serves everyone.** Two people running merges at the same time need roughly twice the memory. Keep `replicaCount` at 1; extra replicas don't speed up a merge.
- **Keep the request below the limit.** The request is reserved on a node whether the app is busy or idle. A request of `1–2Gi` with a higher limit is a good balance on shared clusters.
- **CPU barely matters.** The app runs a single process that uses at most about one core, and spends much of its time waiting on the databases. A request of `500m` is enough.

---

## Troubleshooting

Start with these two commands. The first shows the pod's status and restart count; the second shows the app's output, including errors from scans and merges.

```bash
kubectl -n omop get pods
kubectl -n omop logs deploy/omop-appender --tail=100
```

Add `--previous` to the logs command to see output from before the most recent restart. For more detail on a pod that won't start, use `kubectl -n omop describe pod -l app.kubernetes.io/name=omop-appender` and read the **Events** at the bottom.

| What you see | Likely cause and fix |
|---|---|
| `ImagePullBackOff` / `ErrImagePull` | The cluster can't download the image. Check that `image.tag` exists on the package page. Clusters without internet access need the image mirrored to an internal registry; set `image.repository` and `imagePullSecrets` accordingly. |
| `InvalidImageName` | A tag was included in `image.repository`. Put only the name there and the version in `image.tag`. |
| `Pending` | No node has enough free memory or CPU for the requested resources. `describe pod` says which. Lower `resources.requests` or ask for more capacity. |
| Restarts, `Last State: OOMKilled` | The app ran out of memory. Raise `resources.limits.memory` (see [Sizing](#sizing)). |
| `CrashLoopBackOff` | The app fails on startup. Check `logs --previous` for the error. |
| Database connection times out | The cluster can't reach the database. Check the hostname, firewall rules and any Kubernetes network policies. |
| A long merge stops part-way through an ingress | Raise the ingress read timeout (see the tip under [Common settings](#common-settings)). |
| `port-forward` prints `lost connection to pod` | The pod restarted (check `get pods`) or the forward dropped during a long request. Run the `port-forward` command again. |