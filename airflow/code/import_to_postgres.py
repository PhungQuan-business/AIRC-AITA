import psycopg2
import csv

DB_PARAMS = {
    'host': '192.168.88.146',
    'database': 'formatted-zone',
    'user': 'airc',
    'password': 'admin',
    'port': '5432'  # default PostgreSQL port
}

# Cấu hình PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Đường dẫn file CSV
csv_file_path = '/opt/airflow/data/cleaned_canada.csv'

# Hàm kiểm tra bảng có tồn tại không
def table_exists(table_name):
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

# Hàm nhập dữ liệu vào PostgreSQL
def import_csv_to_postgres(table_name):
    try:
        # Kiểm tra bảng có tồn tại không, nếu không thì tạo mới
        if table_exists(table_name):
            cur.execute(f"DELETE FROM {table_name};")
            print(f"🗑️ Dữ liệu cũ trong bảng '{table_name}' đã được xóa.")
        else:
            print(f"⚠️ Bảng '{table_name}' chưa tồn tại. Sẽ tạo mới.")

        # Đọc và làm sạch dữ liệu CSV
        with open(csv_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            data = list(reader)

        if len(data) == 0:
            print('⚠️ File CSV rỗng.')
            return

        # Lấy danh sách cột từ CSV
        columns = data[0].keys()

        # Tạo bảng nếu chưa có
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join([f'"{col}" TEXT' for col in columns])}
            );
        """
        cur.execute(create_table_query)
        print(f"🛠️ Bảng '{table_name}' đã sẵn sàng.")

        # Chèn dữ liệu vào bảng
        for row in data:
            values = [row[col].strip() if row[col] else None for col in columns]
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in columns])})
                VALUES ({', '.join(['%s'] * len(values))});
            """
            cur.execute(insert_query, values)

        conn.commit()
        print(f"✅ Nhập thành công {len(data)} dòng vào bảng '{table_name}'.")

    except Exception as error:
        print('❌ Lỗi:', error)

    finally:
        cur.close()
        conn.close()
        print('🔌 Đã ngắt kết nối với PostgreSQL.')

# Chạy chương trình
table_name = 'cleaned_data'
import_csv_to_postgres(table_name)
