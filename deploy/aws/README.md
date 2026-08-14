# AWS (EC2 + Docker Compose + Terraform)

This example provisions one Ubuntu EC2 instance with an encrypted persistent
root EBS volume, Elastic IP, Session Manager access, Docker Compose, and Caddy.
Caddy obtains HTTPS certificates and requires a bearer token on every endpoint
except `/healthz`.

This intentionally uses one EC2 instance rather than ECS/Fargate: Minnow's
simple deployment keeps its blob store and query cache on a local persistent
filesystem. Fargate ephemeral storage is not a durable replacement. An ECS
variant should first use S3 for blobs, MongoDB for event durability, and accept
that the query cache is disposable.

## Prerequisites

- Terraform >= 1.6 and authenticated AWS CLI credentials.
- The [AWS Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)
  installed locally (`session-manager-plugin --version`) if you want shell
  access without opening SSH.
- A container image in a registry the instance can pull. After this feature
  reaches `main`, the container workflow publishes `ghcr.io/mikills/minnow:edge`;
  subsequent releases publish `vX.Y.Z`. Confirm the GHCR package is public
  before deploying because cloud-init performs an anonymous pull. For private
  GHCR/ECR images, add registry credentials and least-privilege pull access.
- A DNS name you control.

## Deploy

Create the two encrypted parameters first, in the same region configured for
Terraform. Their values never enter Terraform state or EC2 user data:

```bash
export AWS_REGION=us-east-1
export OPENAI_API_KEY='sk-...'
export MINNOW_TOKEN="$(openssl rand -hex 32)"
aws ssm put-parameter --region "$AWS_REGION" --type SecureString --overwrite \
  --name /minnow/openai-api-key --value "$OPENAI_API_KEY"
aws ssm put-parameter --region "$AWS_REGION" --type SecureString --overwrite \
  --name /minnow/bearer-token --value "$MINNOW_TOKEN"
unset OPENAI_API_KEY

cd deploy/aws/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit the domain, image, region, and parameter names if needed.
terraform init
terraform apply
terraform output elastic_ip
```

Create an `A` record for your configured domain pointing at `elastic_ip`.
Caddy will obtain the certificate after DNS resolves. Then verify:

```bash
curl https://minnow.example.com/healthz
curl -H "Authorization: Bearer $MINNOW_TOKEN" \
  https://minnow.example.com/mcp
```

Connect through AWS Systems Manager without opening SSH:

```bash
$(terraform output -raw ssm_start_session)
```

## Operational notes

- `prevent_destroy` and ignored AMI/user-data drift stop ordinary Terraform
  updates from replacing the stateful instance. Apply container/secret changes
  through Session Manager. Before deliberate replacement or teardown, take and
  verify an EBS snapshot, then remove the lifecycle guard. The root volume is
  deleted with the instance so Terraform does not leave an untracked billable
  volume behind.
- Without MongoDB, queued/in-flight operations and operation history remain
  in-memory and can be lost on restart; published blobs/manifests persist.
- The instance role can read only the two configured SSM parameters, and
  containers cannot reach EC2 instance metadata. If the parameters use a custom
  KMS key rather than the AWS-managed SSM key, grant the role narrowly scoped
  `kms:Decrypt` access.
- After rotating either parameter, connect through Session Manager and run
  `sudo /usr/local/sbin/minnow-refresh-secrets`, followed by
  `cd /opt/minnow && sudo docker compose up -d --force-recreate`.
- The example uses the default VPC to stay small. For organizational workloads,
  use a dedicated VPC/subnet and tighten `allowed_ipv4_cidrs`.
- Port 8080 is never exposed. Ports 80/443 reach only Caddy.
- `t3.small` has 2 GiB RAM and explicitly uses standard CPU credits to prevent
  surprise T3 Unlimited charges. Sustained indexing may be throttled; use a
  non-burstable instance for consistently heavy ingestion.
