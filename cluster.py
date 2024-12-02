import boto3
import time

# Información directamente proporcionada del clúster en ejecución
CLUSTER_ID = 'j-CN685OWRABYO'  # ID del clúster
LOG_URI = 's3n://aws-logs-192272991520-us-east-1/elasticmapreduce/'  # Ruta de logs de S3
SCRIPT_PATH = 's3://aws-emr-studio-192272991520-us-east-1/1730387024458/e-7K9W2KAO3GV4Q4NQVQUFQLAP7/parcialspark.py'  # Ruta al script Python en S3
INSTANCE_TYPE = 'm5.xlarge'  # Tipo de instancia para master y slave
INSTANCE_COUNT = 3  # Número de instancias
SERVICE_ROLE = 'arn:aws:iam::192272991520:role/EMR_DefaultRole'  # Rol de servicio de EMR
INSTANCE_PROFILE = 'EMR_EC2_DefaultRole'  # Perfil de instancia EC2 (solo el nombre del rol)

# Cliente de Boto3
emr_client = boto3.client('emr', region_name='us-east-1')

# Ruta al script de bootstrap en S3 (instalar Papermill si fuera necesario)
BOOTSTRAP_SCRIPT_PATH = 's3://parcialtri/driver/bootstrap.sh'

def create_cluster():
    cluster_config = {
        'MasterInstanceType': INSTANCE_TYPE,
        'SlaveInstanceType': INSTANCE_TYPE,
        'InstanceCount': INSTANCE_COUNT,
        'LogUri': LOG_URI,
        'ReleaseLabel': 'emr-6.9.0',  # Versión de EMR
        'Applications': [
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'JupyterHub'},
            {'Name': 'JupyterEnterpriseGateway'}
        ],
        'TerminationProtected': False
    }

    # Crear un nuevo clúster
    response = emr_client.run_job_flow(
        Name='NewEMRCluster',
        LogUri=cluster_config['LogUri'],
        ReleaseLabel=cluster_config['ReleaseLabel'],
        Applications=cluster_config['Applications'],
        Instances={
            'MasterInstanceType': cluster_config['MasterInstanceType'],
            'SlaveInstanceType': cluster_config['SlaveInstanceType'],
            'InstanceCount': cluster_config['InstanceCount'],
            'KeepJobFlowAliveWhenNoSteps': False,  # Terminar el clúster después de ejecutar el paso
            'TerminationProtected': cluster_config['TerminationProtected'],
            'Ec2KeyName': 'vockey',  # Nombre de la clave EC2
            'Ec2SubnetId': 'subnet-0233fc207a4946607',  # Subnet ID
            'EmrManagedMasterSecurityGroup': 'sg-0cbc2610998f2afe0',
            'EmrManagedSlaveSecurityGroup': 'sg-062e297b0f91258eb',
        },
        BootstrapActions=[
            {
                'Name': 'Install Spark and Dependencies',
                'ScriptBootstrapAction': {
                    'Path': BOOTSTRAP_SCRIPT_PATH,
                    'Args': []  # Puedes incluir argumentos si tu script los necesita
                }
            }
        ],
        ServiceRole=SERVICE_ROLE,  # Rol de servicio de EMR
        Steps=[{
            'Name': 'Run PySpark Job',
            'ActionOnFailure': 'TERMINATE_CLUSTER',  # Terminar el clúster si el paso falla
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--master', 'yarn',
                    '--deploy-mode', 'cluster',
                    '--num-executors', '3',
                    '--executor-memory', '4G',
                    '--executor-cores', '2',
                    '--driver-memory', '4G',
                    '--conf', 'spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain',
                    '--conf', 'spark.sql.shuffle.partitions=50',
                    '--conf', 'spark.sql.parquet.writeLegacyFormat=true',
                    SCRIPT_PATH  # Ruta al archivo Python en S3
                ]
            }
        }],
        VisibleToAllUsers=True,
        JobFlowRole=INSTANCE_PROFILE  # Rol de instancia para el JobFlow
    )
    return response['JobFlowId']


# Función para monitorear el estado del clúster
def monitor_cluster(cluster_id):
    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        print(f"Estado del clúster: {state}")
        if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
            break
        time.sleep(60)  # Revisa el estado cada minuto

# Función Lambda principal
def lambda_handler(event, context):
    # Crear el nuevo clúster
    cluster_id = create_cluster()
    print(f"Nuevo clúster creado: {cluster_id}")

    # Monitorear el clúster y esperar a que termine
    monitor_cluster(cluster_id)
    print(f"Cluster {cluster_id} terminado.")

    return {
        'statusCode': 200,
        'body': f'EMR job completado y el clúster {cluster_id} ha terminado.'
    }
