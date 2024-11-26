import boto3
import logging

# Inicializar el cliente de Glue
glue_client = boto3.client('glue')

# Nombre del Crawler que debe ser ejecutado
CRAWLER_NAME = 'tercerpunto'  # Cambia este valor por el nombre de tu crawler

# Configurar el logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Iniciar el Crawler
        response = glue_client.start_crawler(
            Name=CRAWLER_NAME
        )
        
        # Log de la respuesta
        logger.info(f"Crawler {CRAWLER_NAME} iniciado con éxito: {response}")
        
        return {
            'statusCode': 200,
            'body': f"Crawler {CRAWLER_NAME} iniciado con éxito."
        }
    except Exception as e:
        logger.error(f"Error al iniciar el crawler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error al iniciar el crawler: {str(e)}"
        }
