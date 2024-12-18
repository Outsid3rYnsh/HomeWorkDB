import psycopg2
import random
import string
import time
from datetime import datetime

class DatabasePerformanceTester:
    def __init__(self, db_params):
        self.conn = psycopg2.connect(**db_params)
        self.cursor = self.conn.cursor()
        self.log_file = 'database_performance_results_with_indexes.txt'

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

    def create_indexes(self):
        """Створення індексів."""
        try:
            # Створення індексу на поле name
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_performance_test_name 
                ON performance_test (name)
            """)
            
            # Створення індексу на поле created_at
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_performance_test_created_at 
                ON performance_test (created_at)
            """)
            
            self.conn.commit()
            self.log_result("Індекси успішно створено")
        except Exception as e:
            self.log_result(f"Помилка при створенні індексів: {e}")

    def drop_indexes(self):
        """Видалення індексів."""
        try:
            self.cursor.execute("""
                DROP INDEX IF EXISTS idx_performance_test_name
            """)
            self.cursor.execute("""
                DROP INDEX IF EXISTS idx_performance_test_created_at
            """)
            
            self.conn.commit()
            self.log_result("Індекси успішно видалено")
        except Exception as e:
            self.log_result(f"Помилка при видаленні індексів: {e}")

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
        
        # Вибірка з використанням звичайної вибірки з обмеженням
        self.cursor.execute("""
            SELECT * FROM performance_test 
            ORDER BY id
            LIMIT %s
        """, (num_records,))
        results = self.cursor.fetchall()
        
        return time.time() - start_time

    def update_data(self, num_records):
        """Оновлення тестових даних."""
        start_time = time.time()
        
        self.cursor.execute("""
            UPDATE performance_test 
            SET description = description || '_updated' 
            WHERE id IN (
                SELECT id FROM performance_test 
                ORDER BY id
                LIMIT %s
            )
        """, (num_records,))
        
        self.conn.commit()
        return time.time() - start_time

    def delete_data(self, num_records):
        """Видалення тестових даних."""
        start_time = time.time()
        
        self.cursor.execute("""
            DELETE FROM performance_test 
            WHERE id IN (
                SELECT id FROM performance_test 
                ORDER BY id
                LIMIT %s
            )
        """, (num_records,))
        
        self.conn.commit()
        return time.time() - start_time

    def run_performance_tests(self, test_sizes):
        """Запуск тестів продуктивності."""
        results_without_index = {}
        results_with_index = {}
        
        # Очищення файлу перед початком тестів
        open(self.log_file, 'w', encoding='utf-8').close()
        
        self.log_result("=== Тестування без індексів ===")
        results_without_index = self._run_test_cycle(test_sizes)
        
        # Створення індексів
        self.create_indexes()
        
        self.log_result("\n=== Тестування з індексами ===")
        results_with_index = self._run_test_cycle(test_sizes)
        
        # Видалення індексів
        self.drop_indexes()
        
        # Виведення порівняльної таблиці
        self._print_comparison_table(results_without_index, results_with_index)
        
        return results_without_index, results_with_index

    def _run_test_cycle(self, test_sizes):
        """Виконання циклу тестування."""
        results = {}
        
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
        
        return results

    def _print_comparison_table(self, results_without_index, results_with_index):
        """Виведення порівняльної таблиці."""
        self.log_result("\nПорівняння продуктивності:")
        self.log_result("К-сть | Операція | Без індексів | З індексами | Різниця")
        self.log_result("-" * 55)
        
        for size in results_without_index.keys():
            for op in ['Insert', 'Select', 'Update', 'Delete']:
                without_index = results_without_index[size][op]
                with_index = results_with_index[size][op]
                diff_percent = ((without_index - with_index) / without_index) * 100
                
                self.log_result(f"{size:<5} | {op:<8} | {without_index:11.4f} | {with_index:11.4f} | {diff_percent:+.2f}%")

    def close(self):
        """Закриття з'єднання з базою даних."""
        self.cursor.close()
        self.conn.close()

def main():
    # Параметри підключення до бази даних
    db_params = {
        'dbname': '',
        'user': '',
        'password': '',
        'host': '',
        'port': ''
    }

    tester = DatabasePerformanceTester(db_params)
    tester.create_test_table()

    test_sizes = [1000, 10000, 100000, 1000000]
    tester.run_performance_tests(test_sizes)

    tester.close()
    print(f"\nРезультати збережено у файлі database_performance_results_with_indexes.txt")

if __name__ == "__main__":
    main()