import pymssql

# 数据库连接配置
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'

try:
    # 连接到 Azure SQL 数据库
    conn = pymssql.connect(server=server, user=user, password=password, database=database)
    cursor = conn.cursor()
    print("成功连接到数据库！")

    # 测试 SQL 查询
    cursor.execute('SELECT 1')
    result = cursor.fetchone()
    print(f"测试查询结果: {result}")

except pymssql.OperationalError as e:
    print(f"数据库连接失败: {e}")

finally:
    # 关闭数据库连接
    if 'conn' in locals() and conn:
        conn.close()
        print("数据库连接已关闭。")
