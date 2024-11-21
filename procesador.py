import boto3
import csv
from bs4 import BeautifulSoup
from io import StringIO
from urllib.parse import unquote_plus

# Configuración del bucket de S3
S3_BUCKET = "parcialtri"  # Cambia esto por tu bucket de S3
FINAL_PATH = "headlines/final/"  # Carpeta destino para los CSV procesados

# Cliente de S3
s3 = boto3.client('s3')


def process_file(bucket_name, key):
    """
    Procesa un archivo HTML descargado desde S3 y guarda un CSV en la estructura especificada.
    
    Args:
        bucket_name (str): Nombre del bucket S3.
        key (str): Clave del archivo HTML en S3.
    """
    print(f"Procesando archivo: {key}")

    # Descargar el archivo HTML desde S3
    html_content = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read().decode('utf-8')

    # Parsear el HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extraer datos
    articles = []
    for article in soup.find_all('article'):  # Cambiar según la estructura del HTML
        title = article.find('h2') or article.find('h3')  # Ejemplo: etiquetas h2/h3
        link = article.find('a', href=True)
        category = article.find('span', class_='category')  # Cambiar según la clase del sitio

        if title and link:
            articles.append({
                'category': category.get_text(strip=True) if category else 'Sin categoría',
                'title': title.get_text(strip=True),
                'link': link['href']
            })

    # Extraer información de la clave y fecha del evento
    filename_parts = key.split('/')[-1].split('-')
    periodico = filename_parts[2].replace('.html', '')  # Ejemplo: el_tiempo
    date = filename_parts[1]  # Ejemplo: 2024-11-21
    year, month, day = date.split('-')

    # Generar la clave del CSV en la estructura requerida
    csv_key = f"{FINAL_PATH}periodico={periodico}/year={year}/month={month}/day={day}/headlines.csv"

    # Crear CSV en memoria
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=['category', 'title', 'link'])
    csv_writer.writeheader()
    csv_writer.writerows(articles)

    # Subir el CSV a S3
    s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
    print(f"Archivo CSV generado y guardado en S3: {csv_key}")


def lambda_recive(event, context):
    """
    Handler principal que procesa los eventos de S3.
    """
    print(f"Evento recibido: {event}")
    
    # Procesar cada registro del evento
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])  # Decodificar la clave del archivo
        if key.startswith('headlines/raw/') and key.endswith('.html'):
            process_file(bucket_name, key)
        else:
            print(f"Archivo ignorado: {key}")

    return {"statusCode": 200, "body": "Archivos procesados correctamente"}