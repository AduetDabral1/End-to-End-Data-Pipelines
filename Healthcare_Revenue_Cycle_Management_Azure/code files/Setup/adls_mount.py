# Databricks notebook source
storageAccountName = "aduetadlsdev"

storageAccountAccessKey = dbutils.secrets.get(
    "aduet-healthcare-kv-scope",
    "aduet-healthcare-adls-access-key-dev"
)

mountPoints = ["gold", "silver", "bronze", "landing", "configs"]

for mountPoint in mountPoints:
    try:
        if not any(m.mountPoint == f"/mnt/{mountPoint}" for m in dbutils.fs.mounts()):

            dbutils.fs.mount(
                source=f"wasbs://{mountPoint}@{storageAccountName}.blob.core.windows.net/",
                mount_point=f"/mnt/{mountPoint}",
                extra_configs={
                    f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net":
                        storageAccountAccessKey
                }
            )

            print(f"✅ {mountPoint} mounted successfully.")

        else:
            print(f"ℹ️ {mountPoint} is already mounted.")

    except Exception as e:
        print(f"❌ {mountPoint} mount failed.")
        print(e)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/configs"))

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("aduet-healthcare-key-vault-scope")