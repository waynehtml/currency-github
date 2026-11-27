# DataForge Template Project Guide

**DataForge** is the Databricks template project used to start standardized OEDS/DataBridge data engineering solutions.

The template provides a ready project shape for Databricks Asset Bundles, workflow orchestration, ingestion, transformation, Unity Catalog delivery, release operations, validation assets, and AI-assisted development. A new project should begin by copying this bundle, initializing project-specific configuration, then replacing the sample workflow pieces with real dataset logic.

This guide is an overview. It explains why the template exists, what components it contains, and how those components fit together. For step-by-step setup instructions, use the related guides linked at the end.

## DataBridge naming

DataBridge is the current name for the ingestion project and service formerly
called DIS/OEDIS. The sibling source project is stored in
`application/databricks/bundles/databridge`, and DataForge keeps its
synchronized contract snapshot under `docs/reference/databridge`.

Compatibility identifiers remain unchanged, including the `oedis` Python
package, `oedis.handler.*` handler and schema contracts, `DisClient` APIs,
DIS-prefixed variables, and OEDIS-named deployed infrastructure.

## Purpose

DataForge exists to make new Databricks data projects consistent from the first commit.

It gives developers a common foundation for:

- Databricks Asset Bundle deployment
- Environment-specific project configuration
- Dataflow, curation, integration, service, and operation workflow jobs
- DataBridge-backed ingestion
- Landing, raw, standardized, and curated processing stages
- Shared notebook runtime context
- Reusable Python workflow utilities
- Packaged configuration for schemas, permissions, email, files, file loading, and timeseries
- Unity Catalog table, view, and permission delivery
- Release and maintenance notebooks
- Examples
- the agent prompts and skills that follow the project structure

The goal is not only to copy files faster. The goal is to reduce project-to-project variation so projects are easier to review, operate, enhance, and hand over.

## Design Principles

DataForge follows a few practical rules:

- Keep deployment configuration in bundle files and resource files.
- Keep workflow orchestration in `jobs`.
- Keep shared runtime setup in `common.py`.
- Keep reusable project code in `src/oeds/domain`.
- Keep operational configuration packaged under `conf/oeds/config`.
- Keep examples, tests, releases, and tools separate from production workflow tasks.
- Prefer extending existing registries, managers, and utility patterns before adding a new project-specific pattern.

These rules help both human developers and AI coding tools produce changes that are easy to locate and review.

## Project Lifecycle

A typical project starts from this sequence:

1. Copy `application/databricks/bundles/dataforge` to a new sibling folder.
2. Rename the copied folder to the intended project or dataset name.
3. Create `databricks.yml` from `default.databricks.yml`, adjacent comment mappings, and confirmed project-specific values.
4. Align [common.py](../../common.py) fallback defaults with the generated bundle values.
5. Replace sample config and task logic with real dataset behavior.
6. Add or update schema, permission, email, fileloader, and timeseries config as needed.
7. Validate the Databricks Asset Bundle and project-specific tests before deployment.

The copied folder name normally becomes the bundle name. The initialized
`databricks.yml` becomes the project-specific deployment descriptor.
`/init-project` runs once: after all initialization validation succeeds, it
removes only the copied `default.databricks.yml`. Legacy domain YAML files, if
present, remain untouched and are not initialization inputs. The canonical
DataForge template keeps its default YAML. Later
configuration changes edit project YAML directly and keep `common.py` fallbacks
aligned; initialization stops if `databricks.yml` already exists.

## Component Map

| Component | Location | Role |
| --- | --- | --- |
| Template provenance | [.dataforge/manifest.yml](../../.dataforge/manifest.yml) | Standalone metadata recording the DataForge release from which a project was created and the most recently applied release. |
| Template changelog | [.dataforge/CHANGELOG.md](../../.dataforge/CHANGELOG.md) | DataForge release history kept separate from the generated project's root changelog. |
| Bundle defaults | `default.databricks.yml` | Commented source template used to generate project-specific `databricks.yml`. |
| Shared job configuration | [resources/jobs/common.yml](../../resources/jobs/common.yml) | Complex variables for shared cluster settings and notebook base parameters. |
| Dashboards | [resources/dashboards](../../resources/dashboards) and [dashboards](../../dashboards) | Native AI/BI dashboard resources and JSON assets; independent of jobs, alerts, and apps. |
| SQL alerts | [resources/alerts](../../resources/alerts) | Native SQL Alerts v2 with shared permissions, warehouse, and subscriptions; independent of jobs. |
| Job resources | [resources/jobs](../../resources/jobs) | Job-specific definitions, task graphs, parameters, and application of shared permissions. |
| App resources | [resources/apps/main.yml](../../resources/apps/main.yml) | Main Databricks App resource template, excluded from the default job bundle and deployed separately. |
| Runtime bootstrap | [common.py](../../common.py) | Builds the string-valued shared job context, resolves secrets, configures logging, and initializes shared services. |
| Dataflow workflow | [jobs/dataflow](../../jobs/dataflow) | Notebook tasks for main, ingest, land-to-raw, raw-to-standardized, standardized-to-curated, completion, and failure handling. |
| Curation workflow | [jobs/curation](../../jobs/curation) | Independent curated-layer tasks with optional dependency and integration stages, completion handling, and failure handling. |
| Service workflow | [jobs/service](../../jobs/service) | Service job entry point for release, utility, named task, or app-triggered operations. |
| Operation workflow | [jobs/operation](../../jobs/operation) | Fixed single-task job for a project-owned recurring or independently triggered operation. |
| Project configuration | [conf/oeds/config](../../conf/oeds/config) | Project-owned definitions and static resources imported through `oeds.config`. |
| Workflow package | [src/oeds/domain](../../src/oeds/domain) | Reusable Python modules used by notebooks and examples. |
| Vendored helpers | [src/databricks_tools_core](../../src/databricks_tools_core) | Databricks helper modules bundled with the template. |
| Examples | [examples](../../examples) | Runnable examples for Databricks helpers, datafile lookup, email, file loading, permissions, releases, schemas, and timeseries. |
| Release assets | [releases](../../releases) | Release notebook location for project initialization and deployment-time operations. |
| App scaffold | [apps/main](../../apps/main) | Minimal Streamlit app with reusable clients and UI helpers; supports optional workspace and backend settings and deploys separately. |
| Cluster setup | [cluster](../../cluster) | Cluster init script, environment file, and Python requirements. |
| Deployment hooks | [resources/script.yml](../../resources/script.yml) | Shared hooks and DataBench publishing; app scripts in `resources/apps/main.yml`. Ordered pre/post selections are configured in [default.databricks.yml](../../default.databricks.yml). |
| Operator tools | [tools](../../tools) | Maintenance utilities such as file loading, wheelhouse cleanup, and cluster restart helpers. |
| Documentation | [docs](..) | Overview, setup guides, the agent docs, and standards or practices. |

## Runtime Flow

The template provides `DATAFLOW`, `CURATION`, `SERVICE`, and `OPERATION` job
definitions. Each resource file declares its purpose and required status in
leading comment metadata. SERVICE is required and included automatically. The
developer may select any subset of the other three optional jobs, including
none. Project initialization automatically enables any future job marked required. Future
resource files under `resources/jobs` automatically become additional choices.

`DATAFLOW` is the dataset pipeline. It is organized as a gated flow:

1. `task_00_main.py` builds context and prepares the run.
2. `task_10_ingest.py` calls DataBridge.
3. `task_20_land_to_raw.py` moves landed files into raw storage.
4. `task_30_raw_to_std.py` is the raw-to-standardized extension point.
5. `task_40_std_to_curated.py` is the standardized-to-curated extension point.
6. `task_50_integrate.py` is the downstream-integration extension point.
7. `task_90_on_complete.py` handles successful completion or controlled early exit.
8. `task_91_on_failure.py` handles failure notification.

`CURATION` is a gated curated-layer job that runs independently from
`DATAFLOW`. It validates shared inputs, optionally checks dependencies, runs
the project curation task, and optionally runs project integration. Successful,
disabled, controlled-completion, and failed paths reach the shared curation
lifecycle handlers.

When Job Setup is enabled, its final step selects compute for enabled jobs from
the default YAML choices. Independent Alert Setup, Dashboard Setup, and App Setup follow before validation and
source-template cleanup; disabled job files stay unchanged.

`SERVICE` is required when Job Setup is enabled. It can run release notebooks, utility notebooks, or named task notebooks by passing a task name or notebook path through job parameters.

`OPERATION` is a fixed single-task job for project-owned work that needs its own
job identity. It does not dispatch arbitrary notebook paths. Projects that
enable it must replace its successful no-op placeholder with the intended
operation logic.

Every enabled job receives common base parameters from [resources/jobs/common.yml](../../resources/jobs/common.yml) through its definition in [resources/jobs](../../resources/jobs). Those parameters are read by [common.py](../../common.py), which gives notebooks one shared runtime context.

## Configuration Model

DataForge keeps project configuration outside task notebooks whenever possible.

Packaged configuration lives under [conf/oeds/config](../../conf/oeds/config):

- `ingest`: DataBridge ingestion request templates.
- `datafile`: file definitions, schemas, and reader options.
- `fileloader`: file-to-table load mappings.
- `schema`: Unity Catalog table and view definitions.
- `permission`: catalog, schema, table, and view grant rules.
- `email`: email registry entries and HTML templates.
- `timeseries`: provider, dataset, series, and publishing metadata.

The bundle treats `conf` as a Python source root and imports these areas
through `oeds.config.<area>`.

Reusable managers and utilities live under [src/oeds/domain/utils](../../src/oeds/domain/utils). The intent is for project notebooks to call these utilities instead of embedding large blocks of configuration or orchestration logic directly in notebooks.

## Deployment Configuration

The template is initialized from:

- `default.databricks.yml`
- adjacent domain mapping comments in the default and enabled resource YAML
- explicit or confirmed inferred project values

The initialized output is `databricks.yml` in the project root. That generated
file defines bundle identity, includes the shared and enabled resource
files, and configures variables, targets, environment-specific overrides,
workspace paths, service principals, notification emails, Databricks workspace
references, DataBridge job IDs, and Datamart endpoints. The main configuration
owns the shared domain run-as mapping. Job common.yml retains the compatible
`job_runas_account` alias and owns job notification and permission mappings.
Initialization populates target variables from each selected mapping;
every included resource uses its resolved values.

General setup always runs, followed by independent Job, Alert, Dashboard, and App setup
choices in that order. Declining any section skips its inputs and resources;
declining all four is supported. Job includes remain explicit. When jobs are enabled, it includes
`resources/jobs/common.yml` and every job whose metadata
marks it required and only the optional jobs selected during initialization; it
never uses the `resources/jobs/*.yml` wildcard.

Required project identity, resource naming, and raw-path inputs are initialized
once in the complex `project` variable. The `{{team}}`, `{{domain}}`,
`{{tag}}`, `{{catalog}}`, `{{dataset}}`, and `{{raw_path}}` placeholders live
only in `default.databricks.yml` and are resolved from explicit or confirmed
inferred values. Each initialization placeholder has adjacent `name`,
`description`, `required`, and `value` comment metadata. The `value` metadata
lists its allowed resolution modes and only the applicable options, inference
rule, or optional input example. An example is not a default. `project.domain`
selects matching entries in adjacent comment mappings case-insensitively.

Workspace roots, catalog names, volume paths, and antivirus paths retain their
shared derivations. The shared settings `app_secrete_scope`, `secrete_scope`,
`service_spn_id_key`, `service_spn_secrete_key`, `runas_service_account`, and `email_mappings` instead use
literal `mapping` comments beside their declarations in
`default.databricks.yml`. General setup resolves them automatically, even without
jobs, alerts, dashboards, or apps. Scope mappings contain one scalar per domain, resolved into the
variable default inherited by all targets. SPN keys and email mappings retain
domain/target entries. Changing project.tag does not change these values. Email mappings
remain JSON strings. Target metadata and unrelated target overrides remain in the main configuration.
Job failure recipients and job permissions use adjacent mappings in
`resources/jobs/common.yml` only when jobs are enabled. SQL alert permissions
use their separate mapping in `resources/alerts/common.yml` only when alerts
are enabled.

Each job resource file begins with `purpose` and `required` selection metadata
and owns required name and description placeholders with the same initialization
comment schema. Their `infer` rules produce uppercase
`<DOMAIN>-<DATASET>-<JOB-TYPE>` names and `<dataset> <job-type> job`
descriptions. Their `input` mode lets a project provide either complete value
without enforcing the inferred name structure; initialization uppercases the
complete job name while preserving the description. Initialization asks only
for enabled job identities, so disabled resource files remain unchanged.

After `databricks.yml` is created, [common.py](../../common.py) must be aligned with the same project values so direct notebook runs and tests do not fall back to stale template defaults.

## AI-Assisted Development

See the [AI-Assisted Development Guide](G002-ai-assisted-development-guide.md)
for Copilot and Genie Code workflows and shared skill setup.

The template includes shared agent instructions and task skills:

- [Project instructions](../../AGENTS.md)
- [.agents/skills](../../.agents/skills)
- [DataKit API reference](../reference/datakit/README.md)

Use these files when asking an agent to initialize a project, maintain configuration, or extend the template. Read the applicable AGENTS.md files and linked skills before making changes.

## What Developers Usually Customize

For a real project, the common customization points are:

1. Project identity and deployment variables in generated `databricks.yml`.
2. Matching fallback defaults in `common.py`.
3. Ingestion requests under `conf/oeds/config/ingest`.
4. File and file load definitions under `conf/oeds/config/datafile` and `conf/oeds/config/fileloader`.
5. Schema definitions under `conf/oeds/config/schema`.
6. Permission rules under `conf/oeds/config/permission`.
7. Email registry and templates under `conf/oeds/config/email`.
8. Timeseries registration config under `conf/oeds/config/timeseries`, when needed.
9. Task-specific notebook logic in `jobs/dataflow`, `jobs/curation`, and
   `jobs/operation`.
10. Release, example, and test assets that prove the project behavior.

The `DATAFLOW` transformation placeholders are `task_30_raw_to_std.py` and
`task_40_std_to_curated.py`. Its integration placeholder is
`task_50_integrate.py`. `CURATION` retains its separate `task_20_curate.py` and
`task_30_integrate.py` entry points. The two workflows do not share task
implementations.

## Related Guides

- [App Development Guide](G401-app-develop-guide.md): run and extend the standalone Streamlit starter, configure backend clients, and deploy the app separately.

- [Init Project Guide](G101-init-project-guide.md): copy the template and initialize `databricks.yml` manually or with the agent.
- [Dataflow Job Guide](G201-dataflow-job-guide.md): understand and customize the dataflow Databricks workflow.
- [Curation Job Guide](G206-curation-job-guide.md): configure independently
  runnable curated-layer processing and its optional integration stage.
- [Operation Job Guide](G204-operation-job-guide.md): configure the fixed
  project operation job and replace its placeholder logic.
- [Datafile Module Developer Guide](G225-datafile-developer-guide.md): define source file locations, reader options, and source schemas.
- [Fileloader Developer Guide](G226-fileloader-developer-guide.md): normalize, transform, validate, and write registered files to registered tables.
- [Land To Raw Flow Guide](G230-land-to-raw-flow-guide.md): validate and copy landing batches into raw while preserving landing files.
- [Raw To Standardized Flow Guide](G240-raw-to-standardized-flow-guide.md): strictly read, transform, validate, and load raw batches into standardized tables.
- [DataFrame Validator Guide](G245-dataframe-validator-guide.md): define and interpret standardized-record validation rules.
- [Email Module Developer Guide](G303-email-developer-guide.md): configure registries, templates, recipients, attachments, and email providers.
- [Service Job Guide](G202-service-job-guide.md): use the service job for release notebooks and named service tasks.
- [Common Notebook Guide](G203-common-notebook-guide.md): understand shared notebook context, parameter priority, and runtime utilities.
- [Deployment Guide](G112-deployment-guide.md): understand Databricks Asset Bundle deployment, Azure DevOps parameters, and pre/post deployment hooks.
- [Release Guide](G111-release-guide.md): enable the service job, create release notebooks, deploy the bundle, and run releases.
- [Local Cluster Initialization](G101-init-project-guide.md#local-cluster-initialization): install local classic dependencies directly from Nexus.
- [Schema Guide](G211-schema-guide.md): define Unity Catalog tables and views in the packaged schema registry.
- [Permission Module Developer Guide](G212-schema-permission-guide.md): configure Unity Catalog grants and apply them through the Python API or release notebooks.
- [Calendar Guide](G301-calendar-guide.md): initialize the calendar from reference holidays and calculate workflow dates.
- [AI-Assisted Development Guide](G002-ai-assisted-development-guide.md): use Copilot and Genie Code with shared instructions and skills.
- [Project instructions](../../AGENTS.md): shared agent guidance and task skills.
- [Landing Zone Best Practices](../standards-and-practices/landing_zone_best_practices.md): follow landing-zone conventions.

## Summary

DataForge is the starting foundation for OEDS/DataBridge Databricks data products. It gives each project the same deployment shape, runtime context, workflow layout, configuration model, support tooling, and AI guidance.

Use this guide to understand the template at a high level. Use the focused guides when you are ready to initialize, configure, or extend a copied project.

## SQL Alert Configuration

SQL alerts are native Databricks SQL Alerts v2, deployed with the bundle rather
than a notebook or custom REST deployment hook. Use Databricks CLI 0.279.0 or
later; the repository pipeline currently pins 1.17.0. See the official
[alert resource reference](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources#alert).

During initialization, enable Alert Setup independently of jobs. It adds:

```yaml
include:
  - resources/script.yml
  - resources/alerts/*.yml
```

Retain other enabled job/app includes. The wildcard includes alert `common.yml`
once and every alert definition; do not add common.yml separately. Additional
`.yml` files placed directly in this folder deploy automatically. Keep drafts
outside the included folder. Disabling a schedule uses `PAUSED`; removing a
resource from the bundle is a resource lifecycle change, not a pause operation.

`resources/alerts/common.yml` owns three variables:

| Variable | Default and purpose |
| --- | --- |
| `alert_permissions` | Domain/target mappings retaining job audiences except app SPNs, with alert ACL levels. |
| `alert_warehouse_id` | `${var.sql_warehouse_serverless_small_id}`, using the existing target IDs. |
| `alert_subscriptions` | Independent domain/target SQL alert recipients; supports explicit overrides. |

Each other YAML file declares one unique key under `resources.alerts`. Keep the
query inline in `query_text`, reference shared variables explicitly, and define
the evaluation and Quartz schedule in that file. Use `${var.catalog}` in real
queries for the environment-specific catalog and `${var.project.domain}` and
`${var.project.dataset}` for display names. Resource keys must remain stable
across deployments. Do not use cross-file YAML anchors. Override the shared
warehouse through target variables, or set a different `warehouse_id` on one
alert when necessary.

Every alert explicitly sets:

```yaml
run_as:
  service_principal_name: ${var.runas_service_account}
```

General setup resolves this application ID from the domain/environment mapping
in the main template. Jobs default to the same identity through
`job_runas_account`; explicit job overrides remain supported and do not alter
alert execution identity. Alert ACLs exclude domain app SPNs because apps do not need alert access.
All mapped service accounts receive `CAN_MANAGE` in every domain and target
so they can administer alerts. Group permissions retain the existing translation
from job ACLs: `CAN_MANAGE_RUN` to `CAN_RUN`, `CAN_VIEW` to `CAN_READ`, and
`CAN_MANAGE` unchanged. Alert permissions control access to the alert; they do
not change its run-as identity or grant warehouse or table access.

The deployer needs permission to use the domain service principal (the Service
Principal User role). That principal needs warehouse `CAN_USE` and appropriate
query-data privileges, including `USE CATALOG`, `USE SCHEMA`, and `SELECT` on
referenced tables/views. Confirm those prerequisites with the platform owner;
Alert Setup does not provision warehouses, principals, grants, or destinations.

### Example and notifications

`main.yml` defines `EXAMPLE_VIOLATION_COUNT`, querying
`SELECT 0 AS violation_count` and testing whether the value exceeds zero. Its
hourly schedule uses `${var.timezone}` and remains explicitly `PAUSED` for dev,
stg, and prd. Its subscriptions use the independent alert recipient mapping for the
selected domain and target. Replace the sample SQL and
condition with project-specific logic before enabling a real alert.

Alert common.yml owns its recipient mapping so alerts work without Job Setup.
Maintain alert recipients independently of job failure emails; changes to either
recipient list do not require changing the other. Explicit project subscription
overrides are preserved. Override subscriptions as a native
YAML list, for example in a target's
`variables` mapping:

```yaml
alert_subscriptions:
  - user_email: analyst@example.com
  - destination_id: <existing-notification-destination-id>
```

These are illustrative values, not deployment defaults. Use supported workspace
users or preconfigured notification destinations. Configuring an address does
not verify its eligibility as a Databricks user. For distribution lists or other
non-user addresses, configure an email notification destination with those
recipients and override the subscription with its destination_id. Do not enable
a real alert until the workspace accepts its recipients. See
[notification destinations](https://docs.databricks.com/aws/en/admin/workspace-settings/notification-destinations). Review
subscriptions, condition, empty-result behavior, notification retrigger/recovery
settings, schedule, and timezone before explicitly changing an alert to
`UNPAUSED`. Alert notifications are independent of DataBridge email delivery.

Validate every generated target with `databricks bundle validate -t dev` (and
`stg`/`prd`). Treat unknown alert properties as failures. Check duplicate keys,
resolved variables, the domain identity, permissions, and the paused example.
Workspace validation requires an installed CLI and authenticated access; a
local YAML check alone does not confirm workspace permissions or query execution.

### Adopting alerts in an existing project

Existing projects are not changed automatically. Apply the feature as an
explicit upgrade, preserving project-specific configuration:

1. Add `runas_service_account` to the project's main bundle variables and set its
   dev/stg/prd target values from the selected domain mapping in the template.
2. Keep existing `job_runas_account` overrides. For jobs that should follow the
   domain identity, use `${var.runas_service_account}` as the alias default and
   remove only redundant target overrides after checking their equality.
3. Copy the alert resources and resolve the alert permission mapping for the
   project's domain and each target. Resolve the independent alert
   subscription mapping, preserving explicit project overrides. Keep the example
   paused and verify recipient eligibility before enabling a real alert.
4. Add the alert wildcard once and validate all three targets. Do not rerun
   init-project against an existing `databricks.yml` or modify inherited guide
   files; record project-specific adoption details under `docs/project/`.

No runtime Python API changes or `common.py` fallbacks are needed for these
bundle-only variables. No existing generated project or live resource is
migrated by updating the DataForge template.

## Dashboard Configuration

AI/BI dashboards are native bundle resources, independent of jobs, SQL alerts,
and apps. Initialization runs General, Job, Alert, Dashboard, then App setup.
Accept "Set up AI/BI dashboards?" to add the following include once, preserving
other enabled resources:

```yaml
include:
  - resources/script.yml
  - resources/dashboards/*.yml
```

The wildcard includes common.yml and every dashboard resource YAML directly in
that folder; do not include common.yml separately. Declining Dashboard Setup
omits the include, leaves dashboard files unchanged, and skips dashboard inputs
and validation. A dashboard-only project needs no job compute setup.

### Shared settings and viewer access

[Dashboard common.yml](../../resources/dashboards/common.yml) owns:

| Variable | Default and purpose |
| --- | --- |
| `dashboard_permissions` | Independent domain/target permission mapping with explicit project overrides supported. |
| `dashboard_warehouse_id` | `${var.sql_warehouse_serverless_small_id}`, using existing dev/stg/prd IDs unless overridden. |

Default permission audiences initially match alerts, excluding app principals.
All mapped service principals and manager groups receive `CAN_MANAGE` in every
domain and target; other groups receive `CAN_READ`, the API name for view access. These are separate mappings: later job or alert ACL changes
must not change dashboard permissions. Initialization selects the confirmed
project domain case-insensitively and resolves each target to a native list.
Explicit permission overrides take precedence; initialization consolidates
resolved target permissions into dashboard common.yml to avoid conflicting
values across included files. Preserve warehouse overrides at shared, target,
or individual resource level.

The example explicitly sets `embed_credentials: false`: viewers use their own
credentials. The deployment identity manages the dashboard; it is not the job
or alert run-as identity, and dashboard resources do not use their `run_as`
configuration. Changing to embedded credentials requires an explicit project
access decision because viewers would query using the deployment identity.
Dashboard ACLs alone grant neither warehouse `CAN_USE` nor `USE CATALOG`,
`USE SCHEMA`, or `SELECT` on real query sources. Confirm viewer prerequisites
with the platform owner. Initialization does not provision warehouses or grants.
See the official [dashboard resource reference](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#dashboards)
and [resource permissions](https://docs.databricks.com/aws/en/dev-tools/bundles/permissions).

### Example and authoring

[main.yml](../../resources/dashboards/main.yml) declares `DASHBOARD_MAIN`, with
display name `${var.project.domain}-${var.project.dataset}-DASHBOARD` and
`file_path: ../../dashboards/main.lvdash.json`. File paths are relative to the
resource YAML, not the bundle root. The [JSON asset](../../dashboards/main.lvdash.json)
has one Overview page and a table showing `SELECT 0 AS violation_count`.
It needs no business tables and defines no refresh schedule or subscriptions.

Replace the sample with an exported AI/BI `.lvdash.json` for actual reporting,
keeping its dataset, query, field, and widget references consistent. Do not
assume `${var...}` substitutions inside JSON SQL are expanded by the bundle;
configure environment-specific dataset sources using supported dashboard
resource settings and validate them for each target before deployment.
Keep resource keys stable across deployments.

To add another dashboard, place its JSON under `dashboards/` and add a YAML
under `resources/dashboards/` with one new unique `resources.dashboards` key.
Reference the shared warehouse and permissions, set viewer credentials
explicitly, and point file_path to the corresponding JSON. common.yml must
contain only shared variables/targets. New resource YAML in the included folder
is automatically included; keep drafts outside that folder. Removing a deployed
resource from the bundle is a lifecycle change, not a way to hide a draft.

### Validation and adopting existing projects

Reject duplicate YAML/JSON keys, duplicate resource names across files, missing
JSON assets, broken dataset/query-field/widget references, invalid ACLs, unknown
resource properties, and unresolved initialization placeholders in active values.
Check every target's effective warehouse and permissions, including explicit
overrides. Parse preserved comment metadata separately from active YAML values.
Validate the resolved resource schema with the installed Databricks CLI, then run
`databricks bundle validate -t dev`, `-t stg`, and `-t prd` with authenticated
workspace access. Treat unknown-property warnings as failures. If CLI/access is
unavailable, report incomplete validation and retain the copied source template;
local checks alone prove neither workspace deployment nor dashboard rendering.
Initialization does not deploy, publish, or execute the dashboard.

Existing projects require an explicit upgrade:

1. Copy dashboard resource files and the JSON example, preserving any existing
   dashboard definitions and stable resource keys.
2. Resolve the independent domain/target permission mappings, preserving explicit
   project permissions and warehouse overrides. Verify viewer data access.
3. Add the dashboard wildcard once and validate all targets. Do not rerun
   init-project against an existing databricks.yml. Record project-specific
   adoption details under docs/project/ without modifying inherited guides.

Dashboard variables need no Python runtime APIs or common.py fallbacks. Updating
DataForge does not migrate generated projects or change release versions.
