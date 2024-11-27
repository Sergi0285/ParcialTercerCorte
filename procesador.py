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

    try:
        # Descargar el archivo HTML desde S3
        html_content = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read().decode('utf-8')

        # Parsear el HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Identificar la fuente del archivo
        if "portafolio" in key.lower():
            # Procesar archivo de Portafolio
            articles = []
            for article in soup.find_all('article'):  # Cambiar según la estructura del HTML
                title = article.find('h3')  # Las noticias están en etiquetas <h3>
                link = article.find('a', href=True)
                category_span = article.find('span', class_='category')  # La categoría está en un <span class="category">
                category = category_span.get_text(strip=True) if category_span else 'Sin categoría'

                if title and link:
                    articles.append({
                        'category': category,
                        'title': title.get_text(strip=True),
                        'link': link['href']
                    })

        elif "eltiempo" in key.lower():
            # Procesar archivo de El Tiempo
            articles = []
            for article in soup.find_all('div', class_='c-article'):  # Div con clase c-article contiene las noticias
                title = article.find('h2')  # Título dentro de etiqueta <h2>
                link = article.find('a', href=True)
                category_div = article.find('div', class_='category')  # Categoría en <div class="category">
                category = category_div.get_text(strip=True) if category_div else 'Sin categoría'

                if title and link:
                    articles.append({
                        'category': category,
                        'title': title.get_text(strip=True),
                        'link': link['href']
                    })

        # Extraer información del nombre del archivo
        try:
            print(f"Dividiendo clave: {key}")
            filename_parts = key.split('/')[-1].split('-')  # Separar por '-'
            print(f"Partes del archivo: {filename_parts}")

            if len(filename_parts) < 5:
                raise ValueError("Nombre del archivo no tiene suficientes partes para procesar (se esperaban al menos 5).")

            # Extraer fecha y periódico
            date = filename_parts[1] + '-' + filename_parts[2] + '-' + filename_parts[3]
            periodico = filename_parts[4].replace('.html', '')

            # Validar formato de la fecha
            year, month, day = date.split('-')
            print(f"Fecha procesada: {year}-{month}-{day}, Periódico: {periodico}")

        except ValueError as e:
            print(f"Error al procesar la fecha o el nombre del archivo '{key}': {e}")
            return

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

    except Exception as e:
        print(f"Error al procesar el archivo {key}: {e}")

def lambda_recive(event, context):
    """
    Handler principal que procesa los eventos de S3.
    """
    print(f"Evento recibido: {event}")
    
    for record in event['Records']:
        try:
            bucket_name = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])  # Decodificar la clave del archivo
            if key.startswith('headlines/raw/') and key.endswith('.html'):
                process_file(bucket_name, key)
            else:
                print(f"Archivo ignorado: {key}")
        except Exception as e:
            print(f"Error al procesar el evento {record}: {e}")

    return {"statusCode": 200, "body": "Archivos procesados correctamente"}