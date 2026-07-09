# Security notes

## What kexplain accesses

- **Read-only** Kubernetes API access via your kubeconfig: pod/node/event
  listing, NodeClaim/NodePool/EC2NodeClass objects, and Karpenter controller
  logs. It never mutates cluster state.
- **Read-only** AWS API calls (optional): `ec2:DescribeInstanceTypes`,
  `ec2:DescribeSpotPriceHistory`. No writes, no IAM, no instance access.

## What kexplain stores locally

`~/.kexplain/<cluster>/` contains harvested controller logs, Karpenter-related
events, and object snapshots. These include **node names, private IPs, pod
names, and EC2 instance IDs**. That is infrastructure metadata, not workload data or
secrets. Treat the store directory with the same sensitivity as your
kubeconfig. It is intentionally kept outside the repo; `.gitignore` also
blocks it and all `*.jsonl` harvests as a second layer.

## What must never be committed to this repo

- Harvest stores, logs from provisioning runs (`infra/*.log`), since they contain
  account IDs and instance IDs.
- Kubeconfigs, AWS credentials, `.env` files, private keys.

## Reporting

This is a diagnostic tool; if you find an issue (e.g. the store capturing
more than described above), please open an issue.
