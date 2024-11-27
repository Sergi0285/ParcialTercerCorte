import requests
import boto3
import datetime

# URL de las páginas
URLs = [
    {"url": "https://www.eltiempo.com/", "nombre": "tiempo"},
    {"url": "https://www.portafolio.co/", "nombre": "portafolio"}  # Aquí puedes usar el URL de El Espectador
]

# Cliente de S3
s3 = boto3.client('s3')

# Nombre de tu bucket S3
BUCKET_NAME = 'parcialtri'

def f(event, context):
    # Obtener la fecha actual
    today = datetime.date.today()
    date_str = today.strftime("%Y-%m-%d")

    for site in URLs:
        url = site["url"]
        nombre = site["nombre"]
        
        try:
            # Descargar el contenido de la página
            response = requests.get(url)
            response.raise_for_status()

            # Crear el nombre del archivo S3, ahora con el nombre del sitio (tiempo o espectador)
            file_name = f"headlines/raw/{nombre}-{date_str}.html"

            # Subir el archivo a S3
            s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=response.text)

            print(f"Archivo descargado y subido exitosamente: {file_name}")

        except requests.RequestException as e:
            print(f"Error al descargar la página {url}: {e}")

    return {
        'statusCode': 200,
        'body': 'Proceso completado'
    }