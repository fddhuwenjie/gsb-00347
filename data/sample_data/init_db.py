import sqlite3
import os

data_dir = os.path.dirname(os.path.abspath(__file__))

conn = sqlite3.connect(os.path.join(data_dir, 'sales.db'))
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    region TEXT,
    city TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product TEXT NOT NULL,
    amount REAL NOT NULL,
    quantity INTEGER NOT NULL,
    order_date TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
)
''')

customers_data = [
    (1, '张三', 'zhangsan@example.com', '华北', '北京'),
    (2, '李四', 'lisi@example.com', '华东', '上海'),
    (3, '王五', 'wangwu@example.com', '华南', '广州'),
    (4, '赵六', 'zhaoliu@example.com', '华南', '深圳'),
    (5, '钱七', 'qianqi@example.com', '华东', '杭州'),
    (6, '孙八', 'sunba@example.com', '华东', '南京'),
    (7, '周九', 'zhoujiu@example.com', '西南', '成都'),
    (8, '吴十', 'wushi@example.com', '华中', '武汉'),
]

orders_data = [
    (1, 1, '笔记本电脑', 5999.00, 1, '2024-01-15'),
    (2, 1, '鼠标', 99.00, 2, '2024-01-16'),
    (3, 2, '键盘', 199.00, 1, '2024-01-17'),
    (4, 3, '显示器', 1299.00, 1, '2024-01-18'),
    (5, 4, '笔记本电脑', 6999.00, 1, '2024-01-19'),
    (6, 2, '耳机', 399.00, 1, '2024-01-20'),
    (7, 5, '手机', 4999.00, 1, '2024-01-21'),
    (8, 6, '平板', 2999.00, 1, '2024-01-22'),
    (9, 7, '手表', 1299.00, 1, '2024-01-23'),
    (10, 8, '充电器', 199.00, 2, '2024-01-24'),
    (11, 3, '鼠标', 149.00, 3, '2024-01-25'),
    (12, 1, '键盘', 299.00, 1, '2024-01-26'),
]

cursor.executemany('INSERT INTO customers VALUES (?, ?, ?, ?, ?)', customers_data)
cursor.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)', orders_data)

conn.commit()
conn.close()
print('sales.db created successfully!')
