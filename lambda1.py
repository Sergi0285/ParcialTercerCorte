import requests
import boto3
import datetime
import os

# URL de los periódicos
URLS = {
    "el_tiempo": "https://www.eltiempo.com/",
    "el_espectador": "https://www.publimetro.co/"
}


# Configuración del bucket de S3 
S3_BUCKET = "parcialtri"  # Cambia esto por tu bucket de S3
S3_PATH = "headlines/raw/"  # La carpeta dentro del bucket

# Cliente de S3
s3 = boto3.client('s3')

def download_page(url: str) -> str:
    """Descargar el contenido HTML de la página principal de un periódico."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza un error si la petición fue incorrecta
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar la página: {e}")
        return None

def upload_to_s3(content: str, filename: str):
    """Sube el contenido HTML a un bucket de S3 con un nombre de archivo adecuado."""
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=content)
        print(f"Archivo {filename} subido exitosamente a S3.")
    except Exception as e:
        print(f"Error al subir a S3: {e}")

def lambda_handler(event, context):
    """Función Lambda principal que descarga las páginas y las guarda en S3."""
    
    # Obtener la fecha de hoy en formato yyyy-mm-dd
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    
    # Recorrer cada periódico y descargar su página principal
    for name, url in URLS.items():
        print(f"Descargando la página de {name}...")
        content = download_page(url)
        if content:
            # Generar el nombre de archivo para S3
            filename = f"{S3_PATH}contenido-{today}-{name}.html"
            upload_to_s3(content, filename)

    return {"statusCode": 200, "body": "Páginas descargadas y subidas a S3 correctamente"}
