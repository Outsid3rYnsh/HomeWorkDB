import psycopg2
import random
import string
import time
from datetime import datetime

class DatabasePerformanceTester:
    def __init__(self, db_params):
        self.conn = psycopg2.connect(**db_params)
        self.cursor = self.conn.cursor()
        self.log_file = 'database_performance_results.txt'

    def generate_random_string(self, length=10):
        """Генерація випадкового рядка."""
        return ''.join(random.choices(string.ascii_lowercase, k=length))

    def create_test_table(self):
        """Створення тестової таблиці."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS performance_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                description TEXT,
                created_at TIMESTAMP
            )
        """)
        self.conn.commit()

    def log_result(self, message):
        """Запис результату у файл."""
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(message + '\n')
        print(message)  # Також виводимо в консоль

    def insert_data(self, num_records):
        """Вставка тестових даних."""
        start_time = time.time()
        
        for _ in range(num_records):
            name = self.generate_random_string()
            description = self.generate_random_string(50)
            created_at = datetime.now()
            
            self.cursor.execute("""
                INSERT INTO performance_test (name, description, created_at) 
                VALUES (%s, %s, %s)
            """, (name, description, created_at))
        
        self.conn.commit()
        return time.time() - start_time

    def select_data(self, num_records):
        """Вимірювання часу вибірки даних."""
        start_time = time.time()
        
        self.cursor.execute(f"""
            SELECT * FROM performance_test LIMIT {num_records}
        """)
        results = self.cursor.fetchall()
        
        return time.time() - start_time

    def update_data(self, num_records):
        """Оновлення тестових даних."""
        start_time = time.time()
        
        self.cursor.execute(f"""
            UPDATE performance_test 
            SET description = description || '_updated' 
            WHERE id IN (
                SELECT id FROM performance_test 
                LIMIT {num_records}
            )
        """)
        
        self.conn.commit()
        return time.time() - start_time

    def delete_data(self, num_records):
        """Видалення тестових даних."""
        start_time = time.time()
        
        self.cursor.execute(f"""
            DELETE FROM performance_test 
            WHERE id IN (
                SELECT id FROM performance_test 
                LIMIT {num_records}
            )
        """)
        
        self.conn.commit()
        return time.time() - start_time

    def run_performance_tests(self, test_sizes):
        """Запуск тестів продуктивності."""
        results = {}
        
        # Очищення файлу перед початком тестів
        open(self.log_file, 'w', encoding='utf-8').close()
        
        for size in test_sizes:
            self.log_result(f"\nТестування для {size} записів:")
            
            # Очищення таблиці перед тестом
            self.cursor.execute("TRUNCATE TABLE performance_test RESTART IDENTITY")
            
            # Вставка даних
            insert_time = self.insert_data(size)
            self.log_result(f"Час вставки {size} записів: {insert_time:.4f} сек")
            
            # Вибірка
            select_time = self.select_data(size)
            self.log_result(f"Час вибірки {size} записів: {select_time:.4f} сек")
            
            # Оновлення
            update_time = self.update_data(size)
            self.log_result(f"Час оновлення {size} записів: {update_time:.4f} сек")
            
            # Видалення
            delete_time = self.delete_data(size)
            self.log_result(f"Час видалення {size} записів: {delete_time:.4f} сек")
            
            results[size] = {
                'Insert': insert_time,
                'Select': select_time,
                'Update': update_time,
                'Delete': delete_time
            }
        
        # Виведення підсумкової таблиці
        self.log_result("\nПорівняльна таблиця часу виконання операцій:")
        self.log_result("К-сть записів | Insert | Select | Update | Delete")
        self.log_result("-" * 55)
        for size, times in results.items():
            self.log_result(f"{size:<12} | {times['Insert']:<5.4f} | {times['Select']:<5.4f} | {times['Update']:<5.4f} | {times['Delete']:<5.4f}")
        
        return results

    def close(self):
        """Закриття з'єднання з базою даних."""
        self.cursor.close()
        self.conn.close()

def main():
    # Параметри підключення до бази даних
    db_params = {
         'dbname': ' ',
        'user': ' ',
        'password': ' ',
        'host': ' ',
        'port': ' '
    }

    tester = DatabasePerformanceTester(db_params)
    tester.create_test_table()

    test_sizes = [1000, 10000, 100000, 1000000]
    tester.run_performance_tests(test_sizes)

    tester.close()
    print(f"\nРезультати збережено у файлі database_performance_results.txt")

if __name__ == "__main__":
    main()