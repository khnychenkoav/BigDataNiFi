import json
import os
import time
import urllib.error
import urllib.request


API = os.getenv("NIFI_API", "http://nifi:8080/nifi-api")
CLIENT_ID = "bigdata-nifi-lab"


def request(method, path, payload=None):
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))


def wait_api():
    last_error = None
    for _ in range(80):
        try:
            request("GET", "/system-diagnostics")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(3)
    raise last_error


def find_bundle(kind, type_name):
    items = request("GET", f"/flow/{kind}-types")["processorTypes" if kind == "processor" else "controllerServiceTypes"]
    matches = [item for item in items if item["type"] == type_name]
    if not matches:
        raise RuntimeError(f"Type not found: {type_name}")
    item = matches[0]
    return {
        "group": item["bundle"]["group"],
        "artifact": item["bundle"]["artifact"],
        "version": item["bundle"]["version"],
    }


def create_controller_service(name, type_name, properties):
    entity = request(
        "POST",
        "/process-groups/root/controller-services",
        {
            "revision": {"clientId": CLIENT_ID, "version": 0},
            "component": {
                "type": type_name,
                "bundle": find_bundle("controller-service", type_name),
                "name": name,
                "properties": properties,
            },
        },
    )
    return entity


def update_controller_service(entity, properties):
    component = entity["component"]
    entity = request(
        "PUT",
        f"/controller-services/{component['id']}",
        {
            "revision": entity["revision"],
            "component": {
                "id": component["id"],
                "name": component["name"],
                "properties": properties,
            },
        },
    )
    return entity


def enable_controller_service(entity):
    service_id = entity["component"]["id"]
    return request(
        "PUT",
        f"/controller-services/{service_id}/run-status",
        {"revision": entity["revision"], "state": "ENABLED"},
    )


def create_processor(name, type_name, position):
    return request(
        "POST",
        "/process-groups/root/processors",
        {
            "revision": {"clientId": CLIENT_ID, "version": 0},
            "component": {
                "type": type_name,
                "bundle": find_bundle("processor", type_name),
                "name": name,
                "position": position,
            },
        },
    )


def update_processor(entity, properties, auto_terminated=None):
    component = entity["component"]
    config = {"properties": properties}
    if auto_terminated is not None:
        config["autoTerminatedRelationships"] = auto_terminated
    return request(
        "PUT",
        f"/processors/{component['id']}",
        {
            "revision": entity["revision"],
            "component": {
                "id": component["id"],
                "name": component["name"],
                "config": config,
            },
        },
    )


def connect(source, target, relationships):
    return request(
        "POST",
        "/process-groups/root/connections",
        {
            "revision": {"clientId": CLIENT_ID, "version": 0},
            "component": {
                "name": f"{source['component']['name']} to {target['component']['name']}",
                "source": {
                    "id": source["component"]["id"],
                    "groupId": "root",
                    "type": "PROCESSOR",
                },
                "destination": {
                    "id": target["component"]["id"],
                    "groupId": "root",
                    "type": "PROCESSOR",
                },
                "selectedRelationships": relationships,
            },
        },
    )


def start_processor(entity):
    return request(
        "PUT",
        f"/processors/{entity['component']['id']}/run-status",
        {"revision": entity["revision"], "state": "RUNNING"},
    )


def root_processors():
    flow = request("GET", "/flow/process-groups/root")
    return [item["component"]["name"] for item in flow["processGroupFlow"]["flow"]["processors"]]


def main():
    wait_api()
    if "Consume sales JSON from Kafka" in root_processors():
        print("nifi flow already exists")
        return

    dbcp_props = {
        "Database Connection URL": "jdbc:postgresql://postgres:5432/nifi_lab",
        "Database Driver Class Name": "org.postgresql.Driver",
        "Database Driver Location(s)": "/opt/nifi/nifi-current/lib/postgresql-42.7.4.jar",
        "Database User": "lab",
        "Password": "lab",
    }
    reader_props = {
        "schema-access-strategy": "infer-schema",
        "Date Format": "M/d/yyyy",
        "Timestamp Format": "M/d/yyyy HH:mm:ss",
    }

    dbcp = create_controller_service("PostgreSQL connection pool", "org.apache.nifi.dbcp.DBCPConnectionPool", dbcp_props)
    reader = create_controller_service("JSON sales reader", "org.apache.nifi.json.JsonTreeReader", reader_props)
    dbcp = update_controller_service(dbcp, dbcp_props)
    reader = update_controller_service(reader, reader_props)
    dbcp = enable_controller_service(dbcp)
    reader = enable_controller_service(reader)

    consume = create_processor(
        "Consume sales JSON from Kafka",
        "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
        {"x": 0.0, "y": 0.0},
    )
    putdb = create_processor(
        "Write raw sales to PostgreSQL",
        "org.apache.nifi.processors.standard.PutDatabaseRecord",
        {"x": 450.0, "y": 0.0},
    )

    consume_props = {
        "bootstrap.servers": "kafka:9092",
        "topic": "pet_sales_raw",
        "group.id": "nifi-sales-loader",
        "auto.offset.reset": "earliest",
        "Commit Offsets": "true",
        "honor-transactions": "false",
    }
    putdb_props = {
        "put-db-record-record-reader": reader["component"]["id"],
        "put-db-record-dcbp-service": dbcp["component"]["id"],
        "put-db-record-schema-name": "stage",
        "put-db-record-table-name": "sales_raw",
        "put-db-record-statement-type": "INSERT",
        "put-db-record-unmatched-field-behavior": "Ignore Unmatched Fields",
        "put-db-record-unmatched-column-behavior": "Ignore Unmatched Columns",
        "put-db-record-quoted-identifiers": "false",
        "put-db-record-quoted-table-identifiers": "false",
    }

    consume = update_processor(consume, consume_props, ["parse.failure"])
    putdb = update_processor(putdb, putdb_props, ["success", "failure", "retry"])
    connect(consume, putdb, ["success"])
    putdb = request("GET", f"/processors/{putdb['component']['id']}")
    consume = request("GET", f"/processors/{consume['component']['id']}")
    start_processor(putdb)
    start_processor(consume)
    print("nifi flow configured and started")


if __name__ == "__main__":
    main()
