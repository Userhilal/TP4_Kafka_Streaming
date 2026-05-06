import json
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

bucket = "real-time"
print(f"Objets dans le bucket {bucket} :")
for obj in client.list_objects(bucket, recursive=True):
    print(f"- {obj.object_name} | {obj.size} bytes | {obj.last_modified}")

first = next(client.list_objects(bucket, recursive=True), None)
if first:
    response = client.get_object(bucket, first.object_name)
    content = response.read().decode("utf-8")
    records = json.loads(content)
    print("\nExemple du premier fichier JSON :")
    print(json.dumps(records[:2], indent=2, ensure_ascii=False))
