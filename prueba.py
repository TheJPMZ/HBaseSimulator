import re


def pruebas(table_name, *args):
    print(table_name)
    print(*args)
    
function_table = {
    "put": pruebas
}

a = ('NAME', '=>', 'personal', 'VERSIONS', '=>', '5')

b = list(filter(lambda x: x!="=>", a))
print(b)