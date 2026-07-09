#!/usr/bin/env bash
# Tear down everything create-cluster.sh made.
set -uo pipefail
export PATH="$HOME/.local/bin:$PATH"
CLUSTER_NAME="karpenter-explain-demo"
export AWS_DEFAULT_REGION="us-east-1"

helm uninstall karpenter --namespace kube-system || true
# delete nodeclaims first so instances terminate cleanly
kubectl delete nodeclaims --all --timeout=300s || true
aws cloudformation delete-stack --stack-name "Karpenter-${CLUSTER_NAME}" || true
aws ec2 describe-instances \
  --filters "Name=tag:karpenter.sh/nodepool,Values=*" \
            "Name=tag:kubernetes.io/cluster/${CLUSTER_NAME},Values=owned" \
  --query 'Reservations[].Instances[].InstanceId' --output text | \
  xargs -r aws ec2 terminate-instances --instance-ids || true
eksctl delete cluster --name "${CLUSTER_NAME}" --wait
