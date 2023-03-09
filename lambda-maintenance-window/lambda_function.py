import json
import boto3
import jmespath
import time
from datetime import date, datetime
from pkg_resources import parse_version


def lambda_handler(event, context):
    print("maintenance lambda event: " + json.dumps(event, default=json_serial))
    region = event["region"]
    # default auto query all
    clusterIds = event.get("clusterIds", "all")

    rds_client = boto3.client('rds', region_name=region)

    payload = {}
    payload["Region"] = region
    # report before operation
    originReport = read_clusters_maintenance_info(rds_client, clusterIds)
    print(originReport)
    payload["BeforeOperation"] = originReport

    # perform maintenance window change
    adjustmentReport = adjust_clusters_maintenance_window(
        rds_client, originReport, datetime.utcnow())
    print(adjustmentReport)
    payload["Ajustments"] = adjustmentReport

    # report after operation
    finalReport = read_clusters_maintenance_info(rds_client, clusterIds)
    print(finalReport)
    payload["AfterOperation"] = finalReport

    payload["CurrentTime"] = datetime.utcnow().isoformat() + "Z"
    return payload


def read_clusters_maintenance_info(rds_client, clusterIds):
    # for rds mysql non cluster instances, we should use describe-db-instances
    report = []
    clusterInfoNonCluster = jmespath.compile(
        "DBInstances[?DBClusterIdentifier==null].{DBInstanceIdentifier:DBInstanceIdentifier,PreferredMaintenanceWindow:PreferredMaintenanceWindow,Engine:Engine,EngineVersion:EngineVersion,PreferredBackupWindow:PreferredBackupWindow}").search(
        rds_client.describe_db_instances())
    # for rds mysql cluster, we should use describe-db-clusters
    clusterInfoCluster = jmespath.compile(
        "DBClusters[].{EngineVersion:EngineVersion,PreferredMaintenanceWindow:PreferredMaintenanceWindow,Engine:Engine, DBClusterIdentifier:DBClusterIdentifier,PreferredBackupWindow:PreferredBackupWindow}").search(
        rds_client.describe_db_clusters())
    clusterInfo = []
    if clusterIds != "all":
        for n in clusterInfoNonCluster:
            if n['DBInstanceIdentifier'] in clusterIds:
                clusterInfo.append(n)
        for n in clusterInfoCluster:
            if n['DBClusterIdentifier'] in clusterIds:
                clusterInfo.append(n)
    else:
        clusterInfo = [*clusterInfoNonCluster, *clusterInfoCluster]

    for info in clusterInfo:
        if info.get('DBInstanceIdentifier'):
            report.append({
                "ClusterId": info['DBInstanceIdentifier'],
                "Type": "instance",
                "Engine": info['Engine'],
                "EngineVersion": info['EngineVersion'],
                "MaintenanceWindow": info["PreferredMaintenanceWindow"],
                "BackupWindow": info["PreferredBackupWindow"]
            })
        elif info.get('DBClusterIdentifier'):
            version = ""
            if info['Engine'] == 'mysql':
                version = info['EngineVersion']
            else:
                _, version = info["EngineVersion"].split(
                    ".mysql_aurora.")
            report.append({
                "ClusterId": info['DBClusterIdentifier'],
                "Type": "cluster",
                "Engine": info['Engine'],
                "EngineVersion": version,
                "MaintenanceWindow": info["PreferredMaintenanceWindow"],
                "BackupWindow": info["PreferredBackupWindow"]
            })
    print(clusterInfo, 'read_clusters_maintenance_info============')
    return report


def adjust_clusters_maintenance_window(rds_client, records, current_datetime):
    adjustment = []
    day_of_week = current_datetime.weekday()
    print(f"day of week: {day_of_week}")
    for record in records:
        cluster_id = record["ClusterId"]
        version = record["EngineVersion"]
        engine = record["Engine"]
        record_type = record["Type"]
        maintenance_window = record["MaintenanceWindow"]
        backup_window = record["BackupWindow"]
        new_maintenance_window = replace_3day_later(
            maintenance_window, day_of_week)

        # mysql needs 5.37+
        if engine == "mysql" and parse_version(version) < parse_version(
                '5.7.37') and new_maintenance_window != maintenance_window:
            update_maintenance_window(
                rds_client, cluster_id, new_maintenance_window, backup_window, record_type)
            # avoid API throttling
            time.sleep(1)
            adjustment.append({
                "ClusterId": cluster_id,
                "Engine": engine,
                "EngineVersion": version,
                "NewMaintenanceWindow": new_maintenance_window
            })
        elif engine == "aurora-mysql" and not version.startswith("2.07") and parse_version(version) < parse_version(
                '2.11.1') and new_maintenance_window != maintenance_window:
            update_maintenance_window(
                rds_client, cluster_id, new_maintenance_window, backup_window, record_type)
            # avoid API throttling
            time.sleep(1)
            adjustment.append({
                "ClusterId": cluster_id,
                "Engine": engine,
                "EngineVersion": version,
                "NewMaintenanceWindow": new_maintenance_window
            })
    return adjustment


def update_maintenance_window(rds_client, cluster_id, new_maintenance_window, backup_window, record_type):
    try:
        if record_type == 'cluster':
            rds_client.modify_db_cluster(
                DBClusterIdentifier=cluster_id,
                ApplyImmediately=True,
                PreferredMaintenanceWindow=new_maintenance_window)
        elif record_type == 'instance':
            rds_client.modify_db_instance(
                DBInstanceIdentifier=cluster_id,
                ApplyImmediately=True,
                PreferredMaintenanceWindow=new_maintenance_window)
    except:
        print("maintenance window might overlap with backup window")
        new_backup_window = adjust_backup_window(backup_window)
        if record_type == 'cluster':
            rds_client.modify_db_cluster(
                DBClusterIdentifier=cluster_id,
                ApplyImmediately=True,
                PreferredMaintenanceWindow=new_maintenance_window,
                PreferredBackupWindow=new_backup_window
            )
        elif record_type == 'instance':
            rds_client.modify_db_instance(
                DBInstanceIdentifier=cluster_id,
                ApplyImmediately=True,
                PreferredMaintenanceWindow=new_maintenance_window,
                PreferredBackupWindow=new_backup_window)


def adjust_backup_window(backup_window):
    begin_time, end_time = backup_window.split("-")
    begin_hour, begin_minute = begin_time.split(":")
    end_hour, end_minute = end_time.split(":")
    if begin_hour < 12:
        return "{:02}:{}-{:02}:{}".format(int(begin_hour) + 1, begin_minute, int(end_hour) + 1, end_minute)
    else:
        return "{:02}:{}-{:02}:{}".format(int(begin_hour) - 1, begin_minute, int(end_hour) - 1, end_minute)


def replace_3day_later(current_maintenance_window, day_of_week):
    if day_of_week == 0:
        if current_maintenance_window[:3] in ["mon", "tue", "wed"]:
            return current_maintenance_window.replace("thu", "sun").replace("wed", "sat").replace("tue", "fri").replace(
                "mon", "thu")
    if day_of_week == 1:
        if current_maintenance_window[:3] in ["tue", "wed", "thu"]:
            return current_maintenance_window.replace("fri", "mon").replace("thu", "sun").replace("wed", "sat").replace(
                "tue", "fri")
    if day_of_week == 2:
        if current_maintenance_window[:3] in ["wed", "thu", "fri"]:
            return current_maintenance_window.replace("sat", "tue").replace("fri", "mon").replace("thu", "sun").replace(
                "wed", "sat")
    if day_of_week == 3:
        if current_maintenance_window[:3] in ["thu", "fri", "sat"]:
            return current_maintenance_window.replace("sun", "wed").replace("sat", "tue").replace("fri", "mon").replace(
                "thu", "sun")
    if day_of_week == 4:
        if current_maintenance_window[:3] in ["fri", "sat", "sun"]:
            return current_maintenance_window.replace("mon", "thu").replace("sun", "wed").replace("sat", "tue").replace(
                "fri", "mon")
    if day_of_week == 5:
        if current_maintenance_window[:3] in ["sat", "sun", "mon"]:
            return current_maintenance_window.replace("tue", "fri").replace("mon", "thu").replace("sun", "wed").replace(
                "sat", "tue")
    if day_of_week == 6:
        if current_maintenance_window[:3] in ["sun", "mon", "tue"]:
            return current_maintenance_window.replace("wed", "sat").replace("tue", "fri").replace("mon", "thu").replace(
                "sun", "wed")
    return current_maintenance_window


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


# if __name__ == '__main__':
#     lambda_handler({"region": "us-east-1"}, {})
