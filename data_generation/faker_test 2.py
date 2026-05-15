# pyrefly: ignore [missing-import]
from faker import Faker

fake = Faker()

print(fake.name())
print(fake.email())
print(fake.phone_number())