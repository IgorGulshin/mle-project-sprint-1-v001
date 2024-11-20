import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, UniqueConstraint
import os

# Импорт функций из плагина 'messages'
from messages import send_telegram_success_message, send_telegram_failure_message


# Определение DAG с использованием декоратора
@dag(
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_flats_data_dataset():

    @task()
    def create_table() -> None:
        hook = PostgresHook('destination_db')  # Подключаемся к базе данных
        db_conn = hook.get_sqlalchemy_engine()  # Получаем SQLAlchemy engine

        # Удаление существующей таблицы (на всякий случай)
        with db_conn.connect() as connection:
            connection.execute("DROP TABLE IF EXISTS flats_data;")

        # Инициализация объекта MetaData для управления схемой
        metadata = MetaData()

        # Определение структуры таблицы flats_data
        flats_data = Table(
            'flats_data', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('building_id', Integer, nullable=False),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', String),
            Column('studio', String),
            Column('total_area', Float),
            Column('price', Float),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', String),
            UniqueConstraint('id', name='unique_id')
        )

        # Создание таблицы в базе данных
        metadata.create_all(db_conn)

    @task()
    def extract():
        hook = PostgresHook('student_db')  # Подключаемся к исходной базе данных
        conn = hook.get_conn()
        sql = """
        SELECT
            f.*,
            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator
        FROM flats AS f
        JOIN buildings AS b ON f.building_id = b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        # Преобразования данных (если необходимо)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')  # Подключаемся к целевой базе данных
        engine = hook.get_sqlalchemy_engine()
        data.to_sql('flats_data', engine, if_exists='append', index=False)

    @task()
    def save_to_repository(data: pd.DataFrame):
        import os

        # Получаем путь к текущему файлу DAG
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Путь к файлу, который будет сохранен на уровень выше
        file_path = os.path.join(current_dir, 'flats_data.csv')

        # Сохраните DataFrame в файл CSV
        data.to_csv(file_path, index=False)
        print(f"Данные сохранены в {file_path}")

    # Определение порядка выполнения задач
    create_table_task = create_table()
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_task = load(transformed_data)
    save_task = save_to_repository(transformed_data)

    # Установите зависимости
    create_table_task >> extracted_data >> transformed_data >> [load_task, save_task]

# Инициализация DAG
prepare_flats_data_dataset()