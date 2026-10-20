# Azure Unity Catalog storage setup for dab

How to build, from scratch, the Azure storage chain behind the `dab_dev` and `dab_prod` catalogs
in the Azure workspace (`adb-7405618954475016`, profile `azure`).

The Azure metastore (`metastore_azure_eastus2`) has no default storage, so every catalog needs its own
storage location. That location has to be backed by an Azure storage account that Unity Catalog is allowed to use.

This setup is separate from the one behind `e2e_dev` / `e2e_prod`
(`ste2elearningkl` / `ac-e2e-learning` / `e2e_learning_cred` / `e2e_learning_uc`); see
`databricks_end_to_end_project/docs/part9_cicd_plan.md` step A5.

## The chain

```
Azure (rg-dab-learning)                       Databricks Unity Catalog
──────────────────────────                    ───────────────────────────────
stdablearningkl  (Step 2, ADLS Gen2)
  └─ dab container (Step 3)       ◄─ role ─┐
ac-dab-learning  (Step 4, identity) ───────┘ (Step 5)
        │
        └──────────────────────────────► dab_cred   (Step 6, storage credential)
                                           └─ dab_uc (Step 7, external location → abfss://dab@…/)
                                                ├─ dab_dev  (Step 8) → /dab_dev
                                                └─ dab_prod (Step 8) → /dab_prod
```

| Step | Object | Where | Why |
|---|---|---|---|
| 1 | Resource group `rg-dab-learning` | Azure | One place for everything, so it can be deleted in one go |
| 2 | Storage account `stdablearningkl` | Azure | Where table files live (must be ADLS Gen2) |
| 3 | Container `dab` | Azure | Top-level folder; each catalog gets a sub-folder |
| 4 | Access connector `ac-dab-learning` | Azure | Managed identity Databricks uses to reach storage |
| 5 | Role assignment | Azure | Lets that identity read/write the storage account |
| 6 | Storage credential `dab_cred` | Unity Catalog | Registers the access connector in UC |
| 7 | External location `dab_uc` | Unity Catalog | "UC may use `dab_cred` for anything under this URL" |
| 8 | Catalogs `dab_dev`, `dab_prod` | Unity Catalog | Managed tables stored under `/dab_dev`, `/dab_prod` |

All commands below are PowerShell.

## Step 0: Sign in to the right Azure account

The workspace is in subscription `bdb9ef8a-2f26-4733-b22f-a4b6c3125c31`. Make sure `az` is signed in with
the account that owns it (not the fortbendisd.gov account).

```powershell
az login
az account set --subscription bdb9ef8a-2f26-4733-b22f-a4b6c3125c31
az account show -o table
```

Set the names used below. Storage account names must be globally unique, 3–24 characters,
lowercase letters and digits only. Change `$sa` if the name is taken.

```powershell
$rg   = "rg-dab-learning"
$loc  = "eastus2"                 # same region as the workspace
$sa   = "stdablearningkl"         # storage account
$cont = "dab"                     # container
$ac   = "ac-dab-learning"         # access connector
```

## Step 1: Resource group

```powershell
az group create -n $rg -l $loc
```

## Step 2: Storage account (ADLS Gen2)

`--hns true` enables hierarchical namespace, which makes it ADLS Gen2. Unity Catalog requires it.

```powershell
az storage account create -n $sa -g $rg -l $loc --sku Standard_LRS --kind StorageV2 --hns true
```

Portal: Storage accounts → Create. On the **Advanced** tab, tick **Enable hierarchical namespace**.

## Step 3: Container

```powershell
az storage container create -n $cont --account-name $sa
```

## Step 4: Access connector

Creates an Azure resource with a system-assigned managed identity (no keys or secrets to manage).

```powershell
az databricks access-connector create -n $ac -g $rg -l $loc --identity-type SystemAssigned
# The first time, az offers to install the 'databricks' extension; accept.

$acId        = az databricks access-connector show -n $ac -g $rg --query id -o tsv
$acPrincipal = az databricks access-connector show -n $ac -g $rg --query identity.principalId -o tsv
```

Portal: search **Access Connector for Azure Databricks** → Create.

## Step 5: Give the connector access to the storage

**Storage Blob Data Contributor** lets the identity read, write and delete files in the storage account.

```powershell
$saId = az storage account show -n $sa -g $rg --query id -o tsv
az role assignment create --assignee-object-id $acPrincipal --assignee-principal-type ServicePrincipal `
  --role "Storage Blob Data Contributor" --scope $saId
```

Portal: storage account → **Access control (IAM)** → Add role assignment → Storage Blob Data Contributor →
Managed identity → the access connector.

Role assignments can take 1–5 minutes to take effect. If Step 6 or 7 fails with a permission error, wait and retry.

## Step 6: Storage credential (Unity Catalog)

```powershell
@{ azure_managed_identity = @{ access_connector_id = $acId } } | ConvertTo-Json | Out-File -Encoding ascii cred.json
databricks storage-credentials create dab_cred --json "@cred.json" --comment "Access to $sa via $ac" --profile azure
Remove-Item cred.json
```

The JSON goes through a file because Windows PowerShell 5.1 mangles quotes in inline JSON passed to native programs.

UI: Catalog → **External data** → Credentials → Create credential → Azure Managed Identity →
paste the access connector's resource ID (`$acId`).

## Step 7: External location (Unity Catalog)

```powershell
databricks external-locations create dab_uc "abfss://$cont@$sa.dfs.core.windows.net/" dab_cred `
  --comment "Managed storage for catalogs dab_dev / dab_prod" --profile azure
```

Creating it runs a test read and write. If it succeeds, Steps 2–6 are wired up correctly.

UI: Catalog → External data → External locations → Create.

## Step 8: Catalogs

```powershell
databricks catalogs create dab_dev  --storage-root "abfss://$cont@$sa.dfs.core.windows.net/dab_dev" `
  --comment "Project catalog for dab dev targets (made by hand; the bundle does not own catalogs)" --profile azure
databricks catalogs create dab_prod --storage-root "abfss://$cont@$sa.dfs.core.windows.net/dab_prod" `
  --comment "Project catalog for dab prod targets (made by hand; the bundle does not own catalogs)" --profile azure
```

UI: Catalog → **+** → Create a catalog → Standard → Storage location `dab_uc`, sub-path `dab_dev` (then `dab_prod`).

The folders don't need to exist in Azure first; Unity Catalog creates them when the first table is written.

## Verify

```powershell
databricks catalogs get dab_dev --profile azure -o json        # storage_root should be abfss://dab@<sa>.../dab_dev
databricks external-locations get dab_uc --profile azure -o json
databricks bundle validate --strict --target azure-dev
```

## AWS (DEFAULT profile)

None of the above applies. The Free Edition workspace uses Databricks-managed default storage:

```powershell
databricks catalogs create dab_dev  --profile DEFAULT
databricks catalogs create dab_prod --profile DEFAULT
```

## Clean up (to practise again)

Delete in reverse order:

```powershell
databricks catalogs delete dab_dev  --force --profile azure
databricks catalogs delete dab_prod --force --profile azure
databricks external-locations delete dab_uc --profile azure
databricks storage-credentials delete dab_cred --profile azure
az group delete -n $rg --yes          # removes the storage account and access connector
```

## Notes

- **Workspace-managed storage is different.** The `e2e_learning` catalog uses `dbstoragekzkfftyf6myt4`, a storage
  account Azure Databricks created in the managed resource group `databricks-rg-e2e-learning-…` when the workspace
  was created. That resource group is locked (deny assignment), so don't use it for your own catalogs.
- **Bundles don't create catalogs.** `databricks.yml` only refers to `dab_dev` / `dab_prod` through the `catalog`
  variable; create them by hand as above before `bundle deploy`. Schemas, by contrast, can be bundle resources
  (`resources.schemas`), and development mode prefixes them with `dev_<user>_`.
