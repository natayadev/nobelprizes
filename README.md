# Nobel Prizes

🏆 Desafío técnico: ejemplo para Hackademy de un EDA con PySpark que consume la API de los Premios Nobel, en su versión "notebook" y en su versión con Airflow + Docker.

## Requisitos

- Python 3.8+
- Docker Compose
- Docker

---

## Instalación con Airflow + Docker:

1. Clona el repositorio:

  ```bash
  git clone https://github.com/natayadev/nobelprizes.git
  cd nobelprizes
  ```

2. Crea un entorno virtual:

  ```bash
  python3 -m venv venv
  source venv/bin/activate  # En Linux/macOS
  venv\Scripts\activate     # En Windows
  ```

3. Instala las dependencias:
 
  ```bash
  pip install -r requirements.txt
  ```

## Instrucciones de uso con Airflow + Docker:

1. Levantar los contenedores:
 
  ```bash
  docker-compose up -d
  ```

2. Acceder a la Web UI de Airflow en http://localhost:8080.

3. Detener los contenedores:

  ```bash
  docker-compose down
  ```

## Instrucciones de uso con Jupyter Notebook:
1. En caso de querer ejecutar solo la notebook: 

  ```bash
  docker-compose up -d jupyter
  ```

2. Acceder a la interfaz de Jupyter en http://localhost:8888.
