# Deployment pricing

Last checked: **2026-08-14**. Prices are estimates in USD before tax. Cloud
pricing changes, varies by region, and is prorated from actual usage. Free-tier
credits, support plans, domain registration, custom KMS keys, log ingestion,
and taxes are excluded.

The estimates below use the defaults committed in this repository:

- **Fly.io:** Ashburn (`iad`), one always-running `shared-cpu-2x` Machine with
  2 GB RAM and one 10 GB Fly Volume.
- **AWS:** `us-east-1`, one always-running Linux `t3.small`, one 30 GB gp3 root
  EBS volume, and one public IPv4 address.

## Baseline comparison

| Platform | Estimated monthly baseline | Estimated annual baseline |
|---|---:|---:|
| Fly.io | **$12.89** | **$154.68** |
| AWS | **$21.23** | **$254.76** |
| AWS with an optional Route 53 hosted zone | **$21.73** | **$260.76** |

These totals exclude embedding API usage, backups above the included allowance,
data transfer, and optional services.

## Fly.io

| Resource | Default | Estimate |
|---|---:|---:|
| Fly Machine | `shared-cpu-2x`, 2 GB, always running in `iad` | $11.39/month |
| Persistent volume | 10 GB at $0.15/GB-month | $1.50/month |
| Shared IPv4 and Anycast IPv6 | Included | $0.00 |
| Managed TLS certificate | Within the first 10 single-hostname certificates | $0.00 |
| Snapshot storage | First 10 GB of stored snapshot data each month | $0.00 initially |
| **Baseline** | | **$12.89/month** |

Fly Volumes are slices of local NVMe drives. They provide low-latency local
storage, but a volume is tied to one physical server, attaches to one Machine at
a time, and is not automatically replicated. The example enables scheduled
snapshots with 14-day retention.

Additional Fly.io usage includes:

| Usage | Published rate |
|---|---:|
| Internet egress from North America or Europe | $0.02/GB |
| Snapshot data beyond the first 10 GB/month | $0.08/GB-month |
| Additional volume capacity | $0.15/GB-month |
| Dedicated IPv4, if requested | $2.00/month |

Compute prices have regional markups. For example, the same Machine and volume
are approximately $14.43/month in London (`lhr`) instead of $12.89 in Ashburn.

## AWS

| Resource | Default | Estimate |
|---|---:|---:|
| EC2 | Linux `t3.small`, 730 hours in `us-east-1` | $15.18/month |
| EBS | 30 GB gp3 at $0.08/GB-month | $2.40/month |
| Public IPv4 / Elastic IP | $0.005/hour for 730 hours | $3.65/month |
| SSM standard parameters | Two SecureString parameters | $0.00 |
| Session Manager | Standard interactive access | $0.00 |
| Caddy and Let's Encrypt | Self-managed in the instance | $0.00 |
| **Baseline** | | **$21.23/month** |

The Terraform example explicitly uses standard T3 CPU credits. Sustained CPU
above the instance baseline may therefore be throttled instead of generating T3
Unlimited surplus-credit charges.

Optional AWS usage includes:

| Usage | Typical `us-east-1` rate |
|---|---:|
| Route 53 hosted zone | $0.50/month |
| Route 53 standard DNS queries | $0.40/million queries |
| EBS snapshot storage | Approximately $0.05/used GB-month |
| Internet egress | First 100 GB/month free across AWS, then approximately $0.09/GB |
| Domain registration | Varies by TLD |

Using a customer-managed KMS key, exporting Session Manager logs, or adding
CloudWatch metrics and alarms introduces additional charges. New-account AWS
credits may temporarily reduce the bill but are not included in this estimate.

## Embedding API usage

Both deployments configure OpenAI `text-embedding-3-small` by default. OpenAI
charges for input tokens rather than the number of HTTP requests; Minnow batches
multiple chunks into embedding requests.

| Embedded input | Estimated OpenAI cost |
|---:|---:|
| 1 million tokens | $0.02 |
| 10 million tokens | $0.20 |
| 100 million tokens | $2.00 |
| 1 billion tokens | $20.00 |

Initial indexing embeds all selected text. Incremental refreshes embed changed
files only, although the first index of each new branch is currently a full
index. Vector and hybrid searches also embed their query text, but query costs
are usually negligible. For example, 100,000 searches averaging 30 input tokens
cost about $0.06.

Embedding dimensions affect local storage and memory, not OpenAI's input-token
price. `text-embedding-3-small` has a native maximum of 1,536 dimensions. The
committed `dimensions: 0` uses that model default; configuring 768 dimensions
approximately halves raw vector storage but requires rebuilding existing
knowledge bases after the change.

## Sources

### Fly.io

- [Fly.io resource pricing](https://fly.io/docs/about/pricing/)
- [Fly Volumes overview](https://fly.io/docs/volumes/overview/)
- [Fly Volume snapshots](https://fly.io/docs/volumes/snapshots/)

### AWS

- [Amazon EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/)
- [Amazon EBS pricing](https://aws.amazon.com/ebs/pricing/)
- [Amazon VPC and public IPv4 pricing](https://aws.amazon.com/vpc/pricing/)
- [AWS data-transfer pricing](https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer)
- [Amazon Route 53 pricing](https://aws.amazon.com/route53/pricing/)
- [AWS Systems Manager pricing](https://aws.amazon.com/systems-manager/pricing/)

### Embeddings

- [OpenAI API pricing](https://platform.openai.com/docs/pricing)
- [OpenAI `text-embedding-3-small`](https://platform.openai.com/docs/models/text-embedding-3-small)
