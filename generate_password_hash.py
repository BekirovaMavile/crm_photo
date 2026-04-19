from getpass import getpass

from werkzeug.security import generate_password_hash

password = getpass("Введите пароль для WEB_ADMIN: ")
print(generate_password_hash(password))